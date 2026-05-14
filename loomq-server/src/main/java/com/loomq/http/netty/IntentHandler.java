package com.loomq.http.netty;

import com.loomq.LoomqEngine;
import com.loomq.api.CreateIntentRequest;
import com.loomq.api.ErrorResponse;
import com.loomq.api.IntentActionResponse;
import com.loomq.api.IntentResponse;
import com.loomq.api.IntentValidator;
import com.loomq.api.PatchIntentRequest;
import com.loomq.api.ValidationResult;
import com.loomq.common.exception.BackPressureException;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.http.json.JsonCodec;
import com.loomq.raft.RaftStatusProvider;
import com.loomq.raft.RaftWriteCoordinator;
import com.loomq.raft.RaftWriteUnavailableException;
import com.loomq.store.IdempotencyResult;
import com.loomq.tracing.IntentTrace;
import com.loomq.tracing.IntentTraceStore;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intent API handler for the Netty stack.
 *
 * This is the only request/response adapter used by the standalone server.
 * JSON payloads are decoded through the shared JsonCodec so Instant/record
 * fields work the same way across create, patch, and callback payloads.
 */
public class IntentHandler {

    private static final Logger logger = LoggerFactory.getLogger(IntentHandler.class);
    private static final PrecisionTier DEFAULT_PRECISION_TIER = PrecisionTierCatalog.defaultCatalog().defaultTier();

    private final LoomqEngine engine;
    private final RaftStatusProvider raftStatus;
    private final RaftWriteCoordinator raftWriteCoordinator;
    private final JsonCodec jsonCodec;

    public IntentHandler(LoomqEngine engine) {
        this(engine, null, null);
    }

    public IntentHandler(LoomqEngine engine, RaftStatusProvider raftStatus) {
        this(engine, raftStatus, null);
    }

    public IntentHandler(LoomqEngine engine, RaftStatusProvider raftStatus,
                         RaftWriteCoordinator raftWriteCoordinator) {
        this.engine = engine;
        this.raftStatus = raftStatus;
        this.raftWriteCoordinator = raftWriteCoordinator;
        this.jsonCodec = JsonCodec.instance();
    }

    /**
     * Register API routes.
     */
    public void register(RadixRouter router) {
        router.add(HttpMethod.POST, "/v1/intents", this::createIntent);
        router.add(HttpMethod.GET, "/v1/intents/{intentId}", this::getIntent);
        router.add(HttpMethod.GET, "/v1/intents/{intentId}/trace", this::getTrace);
        router.add(HttpMethod.PATCH, "/v1/intents/{intentId}", this::patchIntent);
        router.add(HttpMethod.POST, "/v1/intents/{intentId}/cancel", this::cancelIntent);
        router.add(HttpMethod.POST, "/v1/intents/{intentId}/fire-now", this::fireNow);
    }

    /**
     * POST /v1/intents
     */
    public Object createIntent(HttpMethod method, String uri, byte[] body,
                               Map<String, String> headers,
                               Map<String, String> pathParams) throws Exception {
        if (isRaftWriteBlocked()) {
            return leaderHintWriteResponse("create", null);
        }

        CreateIntentRequest request = jsonCodec.read(body, CreateIntentRequest.class);

        ValidationResult validation = IntentValidator.validateCreate(
            request.executeAt(), request.deadline(), request.shardKey());
        if (validation.isInvalid()) {
            return errorResponse(422, validation.errorCode(), validation.errorMessage());
        }

        ValidationResult futureValidation = IntentValidator.validateExecuteAtFuture(request.executeAt());
        if (futureValidation.isInvalid()) {
            return errorResponse(422, futureValidation.errorCode(), futureValidation.errorMessage());
        }

        String callbackUrl = callbackUrl(request);
        if (callbackUrl != null && !callbackUrl.isBlank()) {
            ValidationResult callbackValidation = IntentValidator.validateCallbackUrl(callbackUrl);
            if (callbackValidation.isInvalid()) {
                return errorResponse(422, callbackValidation.errorCode(), callbackValidation.errorMessage());
            }
        }

        String intentId = request.intentId();
        if (intentId == null || intentId.isBlank()) {
            intentId = UUID.randomUUID().toString();
        }

        if (request.idempotencyKey() != null && !request.idempotencyKey().isBlank()) {
            IdempotencyResult idemResult = engine.checkIdempotency(request.idempotencyKey());
            if (idemResult.isDuplicateActive()) {
                logger.info("Duplicate active intent found for key={}, returning existing intent={}",
                    request.idempotencyKey(), idemResult.getIntent().getIntentId());
                return IntentResponse.from(idemResult.getIntent());
            }
            if (idemResult.isDuplicateTerminal()) {
                logger.warn("Duplicate terminal intent for key={}, reject with 409", request.idempotencyKey());
                return conflictResponse(idemResult.getIntent().getIntentId());
            }
        }

        try {
            Intent intent = buildCreateIntentDraft(request, intentId);

            if (raftWriteCoordinator != null && raftStatus != null && raftStatus.isRaftEnabled()) {
                Intent raftSnapshot = intent.copy();
                raftSnapshot.transitionTo(IntentStatus.SCHEDULED);
                Intent committed = raftWriteCoordinator.commitSnapshot(raftSnapshot, "create");
                logger.info("Intent created via Raft: id={}, executeAt={}, ackLevel={}",
                    committed.getIntentId(), committed.getExecuteAt(), committed.getAckMode());
                return new IntentHandler.CreatedResponse(201, IntentResponse.from(committed));
            }

            engine.createIntent(intent, intent.getAckMode()).join();
            logger.info("Intent created: id={}, executeAt={}, ackLevel={}",
                intent.getIntentId(), intent.getExecuteAt(), intent.getAckMode());
            return new IntentHandler.CreatedResponse(201, IntentResponse.from(intent));
        } catch (RaftWriteUnavailableException e) {
            logger.warn("Raft write unavailable for create: {}", e.getMessage());
            return raftWriteUnavailableResponse("create", null, e.reason());
        } catch (CompletionException e) {
            if (e.getCause() instanceof BackPressureException bpe) {
                return new HttpErrorResponse(429, ErrorResponse.of(
                    "42900",
                    "Backpressure: " + bpe.getMessage(),
                    Map.of("retryAfterMs", (Object) bpe.getRetryAfterMs())
                ));
            }
            throw e;
        }
    }

    /**
     * GET /v1/intents/{intentId}
     */
    public Object getIntent(HttpMethod method, String uri, byte[] body,
                            Map<String, String> headers,
                            Map<String, String> pathParams) {
        String intentId = pathParams.get("intentId");

        if (isRaftFollowerReadBlocked()) {
            return leaderHintResponse(intentId);
        }

        Optional<Intent> intent = engine.getIntent(intentId);
        if (intent.isEmpty()) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        return IntentResponse.from(intent.get());
    }

    /**
     * PATCH /v1/intents/{intentId}
     */
    public Object patchIntent(HttpMethod method, String uri, byte[] body,
                             Map<String, String> headers,
                             Map<String, String> pathParams) throws Exception {
        String intentId = pathParams.get("intentId");
        if (isRaftWriteBlocked()) {
            return leaderHintWriteResponse("patch", intentId);
        }
        PatchIntentRequest request = jsonCodec.read(body, PatchIntentRequest.class);

        Optional<Intent> current = engine.getIntent(intentId);
        if (current.isEmpty()) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        if (!current.get().getStatus().isModifiable()) {
            return errorResponse(422, "42205", "Intent cannot be modified in state: " + current.get().getStatus());
        }

        if (request.executeAt() != null) {
            ValidationResult futureValidation = IntentValidator.validateExecuteAtFuture(request.executeAt());
            if (futureValidation.isInvalid()) {
                return errorResponse(422, futureValidation.errorCode(), futureValidation.errorMessage());
            }
        }

        Intent updatedSnapshot = current.get().copy();
        if (request.deadline() != null) {
            updatedSnapshot.setDeadline(request.deadline());
        }
        if (request.expiredAction() != null) {
            updatedSnapshot.setExpiredAction(request.expiredAction());
        }
        if (request.redelivery() != null) {
            updatedSnapshot.setRedelivery(request.redelivery());
        }
        if (request.tags() != null) {
            updatedSnapshot.setTags(request.tags());
        }
        if (request.executeAt() != null) {
            updatedSnapshot.setExecuteAt(request.executeAt());
        }

        try {
            if (raftWriteCoordinator != null && raftStatus != null && raftStatus.isRaftEnabled()) {
                Intent committed = raftWriteCoordinator.commitSnapshot(updatedSnapshot, "patch");
                return IntentResponse.from(committed);
            }

            Optional<Intent> updated = engine.updateIntent(intentId, intent -> {
                if (request.deadline() != null) {
                    intent.setDeadline(request.deadline());
                }
                if (request.expiredAction() != null) {
                    intent.setExpiredAction(request.expiredAction());
                }
                if (request.redelivery() != null) {
                    intent.setRedelivery(request.redelivery());
                }
                if (request.tags() != null) {
                    intent.setTags(request.tags());
                }
            }, request.executeAt());

            if (updated.isEmpty()) {
                return errorResponse(404, "40401", "Intent not found: " + intentId);
            }

            return IntentResponse.from(updated.get());
        } catch (RaftWriteUnavailableException e) {
            logger.warn("Raft write unavailable for patch {}: {}", intentId, e.getMessage());
            return raftWriteUnavailableResponse("patch", intentId, e.reason());
        }
    }

    /**
     * POST /v1/intents/{intentId}/cancel
     */
    public Object cancelIntent(HttpMethod method, String uri, byte[] body,
                               Map<String, String> headers,
                               Map<String, String> pathParams) {
        String intentId = pathParams.get("intentId");
        if (isRaftWriteBlocked()) {
            return leaderHintWriteResponse("cancel", intentId);
        }

        Optional<Intent> current = engine.getIntent(intentId);
        if (current.isEmpty()) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        Intent intent = current.get();
        if (!intent.getStatus().isCancellable()) {
            return errorResponse(422, "42204", "Intent cannot be cancelled in state: " + intent.getStatus());
        }

        try {
            Intent cancelledSnapshot = intent.copy();
            cancelledSnapshot.transitionTo(IntentStatus.CANCELED);

            if (raftWriteCoordinator != null && raftStatus != null && raftStatus.isRaftEnabled()) {
                Intent committed = raftWriteCoordinator.commitSnapshot(cancelledSnapshot, "cancel");
                return IntentResponse.from(committed);
            }

            if (!engine.cancelIntent(intentId)) {
                return errorResponse(500, "50002", "Failed to cancel intent");
            }

            return IntentResponse.from(intent);
        } catch (RaftWriteUnavailableException e) {
            logger.warn("Raft write unavailable for cancel {}: {}", intentId, e.getMessage());
            return raftWriteUnavailableResponse("cancel", intentId, e.reason());
        }
    }

    /**
     * POST /v1/intents/{intentId}/fire-now
     */
    public Object fireNow(HttpMethod method, String uri, byte[] body,
                          Map<String, String> headers,
                          Map<String, String> pathParams) {
        String intentId = pathParams.get("intentId");
        if (isRaftWriteBlocked()) {
            return leaderHintWriteResponse("fire-now", intentId);
        }

        Optional<Intent> current = engine.getIntent(intentId);
        if (current.isEmpty()) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        try {
            Intent fireNowSnapshot = current.get().copy();
            fireNowSnapshot.setExecuteAt(java.time.Instant.now());

            if (raftWriteCoordinator != null && raftStatus != null && raftStatus.isRaftEnabled()) {
                raftWriteCoordinator.commitSnapshot(fireNowSnapshot, "fire-now");
                return new IntentActionResponse(intentId, IntentStatus.DISPATCHING.name());
            }

            if (!engine.fireNow(intentId)) {
                return errorResponse(500, "50003", "Failed to trigger intent immediately");
            }

            return new IntentActionResponse(intentId, IntentStatus.DISPATCHING.name());
        } catch (RaftWriteUnavailableException e) {
            logger.warn("Raft write unavailable for fire-now {}: {}", intentId, e.getMessage());
            return raftWriteUnavailableResponse("fire-now", intentId, e.reason());
        }
    }

    /**
     * GET /v1/intents/{intentId}/trace
     */
    public Object getTrace(HttpMethod method, String uri, byte[] body,
                           Map<String, String> headers,
                           Map<String, String> pathParams) {
        String intentId = pathParams.get("intentId");
        IntentTrace trace = IntentTraceStore.getInstance().get(intentId);

        if (trace == null) {
            return errorResponse(404, "40402", "No trace found for intent: " + intentId);
        }

        return trace.toJson();
    }

    private String callbackUrl(CreateIntentRequest request) {
        if (request.callback() == null) {
            return null;
        }
        return request.callback().getUrl();
    }

    private Object errorResponse(int status, String code, String message) {
        return new HttpErrorResponse(status, ErrorResponse.of(code, message));
    }

    private Intent buildCreateIntentDraft(CreateIntentRequest request, String intentId) {
        Intent intent = new Intent(intentId);
        intent.setExecuteAt(request.executeAt());
        intent.setDeadline(request.deadline());
        intent.setExpiredAction(request.expiredAction());
        intent.setPrecisionTier(request.precisionTier() != null ? request.precisionTier() : DEFAULT_PRECISION_TIER);
        intent.setWalMode(request.walMode());
        intent.setShardKey(request.shardKey());
        intent.setAckMode(request.ackLevel() != null ? request.ackLevel() : AckMode.DURABLE);
        intent.setCallback(request.callback());
        intent.setRedelivery(request.redelivery());
        intent.setIdempotencyKey(request.idempotencyKey());
        intent.setTags(request.tags());
        return intent;
    }

    private boolean isRaftFollowerReadBlocked() {
        return raftStatus != null
            && raftStatus.isRaftEnabled()
            && !raftStatus.isLeader();
    }

    private boolean isRaftWriteBlocked() {
        return raftStatus != null
            && raftStatus.isRaftEnabled()
            && !raftStatus.isLeader();
    }

    private Object leaderHintResponse(String intentId) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("retryable", true);
        details.put("nodeRole", raftStatus.role().name());
        details.put("intentId", intentId);
        String leaderId = raftStatus.currentLeaderId();
        if (leaderId != null && !leaderId.isBlank()) {
            details.put("leaderId", leaderId);
        }

        return new HttpErrorResponse(
            HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
            ErrorResponse.of(
                "50301",
                "Read must be served by the Raft leader",
                details
            )
        );
    }

    private Object leaderHintWriteResponse(String operation, String intentId) {
        return raftWriteUnavailableResponse(operation, intentId, "Write must be served by the Raft leader");
    }

    private Object raftWriteUnavailableResponse(String operation, String intentId, String reason) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("retryable", true);
        details.put("nodeRole", raftStatus != null ? raftStatus.role().name() : "UNKNOWN");
        details.put("operation", operation);
        if (intentId != null && !intentId.isBlank()) {
            details.put("intentId", intentId);
        }
        String leaderId = raftStatus != null ? raftStatus.currentLeaderId() : null;
        if (leaderId != null && !leaderId.isBlank()) {
            details.put("leaderId", leaderId);
        }

        return new HttpErrorResponse(
            HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
            ErrorResponse.of(
                "50302",
                reason,
                details
            )
        );
    }

    private Object conflictResponse(String intentId) {
        return new HttpErrorResponse(409, ErrorResponse.of(
            "40901",
            "Idempotency key conflict",
            Map.of("intentId", intentId)
        ));
    }

    /**
     * Created response wrapper carrying an explicit HTTP status code.
     */
    public record CreatedResponse(int status, Object body) {}
}
