package com.loomq.http.netty;

import com.loomq.LoomqEngine;
import com.loomq.api.CreateIntentRequest;
import com.loomq.api.DeathReport;
import com.loomq.api.ErrorRecoveryAdvisor;
import com.loomq.api.ErrorResponse;
import com.loomq.api.IntentActionResponse;
import com.loomq.api.IntentListResponse;
import com.loomq.api.IntentResponse;
import com.loomq.api.PatchIntentRequest;
import com.loomq.api.RecoveryHint;
import com.loomq.api.ReviveIntentRequest;
import com.loomq.channel.http.json.JsonCodec;
import com.loomq.common.IntentValidator;
import com.loomq.common.ValidationResult;
import com.loomq.common.exception.BackPressureException;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.raft.RaftWriteBackPressureException;
import com.loomq.raft.RaftWriteConflictException;
import com.loomq.raft.RaftWriteUnavailableException;
import com.loomq.spi.RaftStatusProvider;
import com.loomq.spi.WriteCoordinator;
import com.loomq.store.IdempotencyResult;
import com.loomq.tracing.IntentTrace;
import com.loomq.tracing.IntentTraceStore;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
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
    private static final String REQUEST_ID_HEADER = "X-LoomQ-Request-Id";
    private static final String EXPECTED_REVISION_HEADER = "X-LoomQ-Expected-Revision";

    private final LoomqEngine engine;
    private final WriteCoordinator writeCoordinator;
    private final RaftStatusProvider raftStatus;
    private final boolean raftEnabled;
    private final JsonCodec jsonCodec;

    public IntentHandler(LoomqEngine engine) {
        this(engine, null, null);
    }

    /** Read-only handler with Raft status (no write coordinator). */
    public IntentHandler(LoomqEngine engine, RaftStatusProvider raftStatus) {
        this(engine, null, raftStatus);
    }

    public IntentHandler(LoomqEngine engine, WriteCoordinator writeCoordinator,
                         RaftStatusProvider raftStatus) {
        this.engine = engine;
        this.writeCoordinator = writeCoordinator;
        this.raftStatus = raftStatus;
        this.raftEnabled = raftStatus != null && raftStatus.isRaftEnabled();
        this.jsonCodec = JsonCodec.instance();
    }

    /**
     * Register API routes.
     */
    public void register(RadixRouter router) {
        router.add(HttpMethod.POST, "/v1/intents", this::createIntent);
        router.add(HttpMethod.GET, "/v1/intents", this::listIntents);
        router.add(HttpMethod.GET, "/v1/intents/{intentId}", this::getIntent);
        router.add(HttpMethod.GET, "/v1/intents/{intentId}/trace", this::getTrace);
        router.add(HttpMethod.PATCH, "/v1/intents/{intentId}", this::patchIntent);
        router.add(HttpMethod.POST, "/v1/intents/{intentId}/cancel", this::cancelIntent);
        router.add(HttpMethod.POST, "/v1/intents/{intentId}/fire-now", this::fireNow);
        router.add(HttpMethod.POST, "/v1/intents/{intentId}/revive", this::reviveIntent);
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

        String requestedIntentId = request.intentId();
        String requestKeySeed = requestKey("create", requestedIntentId != null ? requestedIntentId : "",
            body, headers, null, request.idempotencyKey());
        boolean stableCreateIdentity = hasStableCreateIdentity(headers, request.idempotencyKey());
        String intentId = requestedIntentId;
        if (intentId == null || intentId.isBlank()) {
            intentId = stableCreateIdentity ? deriveIntentId(requestKeySeed) : UUID.randomUUID().toString();
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

            if (writeCoordinator != null && raftEnabled) {
                Intent snapshot = intent.copy();
                snapshot.transitionTo(IntentStatus.SCHEDULED);
                String requestKey = requestKey("create", intentId, body, headers, null, request.idempotencyKey());
                Intent committed = writeCoordinator.commitSnapshot(snapshot, "create", requestKey);
                logger.info("Intent created via coordinator: id={}, executeAt={}, ackLevel={}",
                    committed.getIntentId(), committed.getExecuteAt(), committed.getAckMode());
                return new IntentHandler.CreatedResponse(201, IntentResponse.from(committed));
            }

            // Fallback: direct engine call (no coordinator)
            engine.createIntent(intent, intent.getAckMode()).join();
            logger.info("Intent created: id={}, executeAt={}, ackLevel={}",
                intent.getIntentId(), intent.getExecuteAt(), intent.getAckMode());
            return new IntentHandler.CreatedResponse(201, IntentResponse.from(intent));
        } catch (RaftWriteUnavailableException e) {
            logger.warn("Raft write unavailable for create: {}", e.getMessage());
            return raftWriteUnavailableResponse("create", null, e.reason());
        } catch (RaftWriteBackPressureException e) {
            logger.warn("Raft write backpressure for create: {}", e.getMessage());
            return raftBackPressureResponse("create", null, e.retryAfterMs(), e.getMessage());
        } catch (CompletionException e) {
            if (e.getCause() instanceof BackPressureException bpe) {
                Map<String, Object> ctx = Map.of("retryAfterMs", (Object) bpe.getRetryAfterMs());
                RecoveryHint recovery = ErrorRecoveryAdvisor.advise("42900", ctx);
                return new HttpErrorResponse(429, ErrorResponse.of(
                    "42900",
                    "Backpressure: " + bpe.getMessage(),
                    Map.of("retryAfterMs", (Object) bpe.getRetryAfterMs()),
                    recovery
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
        Long expectedRevision;
        try {
            expectedRevision = expectedRevision(headers);
        } catch (IllegalArgumentException e) {
            return errorResponse(400, "40001", e.getMessage());
        }
        if (raftStatus != null && raftStatus.isRaftEnabled() && expectedRevision == null) {
            return errorResponse(428, "42801", "X-LoomQ-Expected-Revision header is required for Raft writes");
        }

        Optional<Intent> current = engine.getIntent(intentId);
        if (current.isEmpty()) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        if (!current.get().getStatus().isModifiable()) {
            return errorResponse(422, "42205", "Intent cannot be modified in state: " + current.get().getStatus(),
                Map.of("currentState", current.get().getStatus().name(),
                       "precisionTier", current.get().getPrecisionTier()));
        }

        if (request.executeAt() != null) {
            ValidationResult futureValidation = IntentValidator.validateExecuteAtFuture(request.executeAt());
            if (futureValidation.isInvalid()) {
                return errorResponse(422, futureValidation.errorCode(), futureValidation.errorMessage());
            }
        }

        try {
            if (writeCoordinator != null && raftEnabled) {
                String requestKey = requestKey("patch", intentId, body, headers, expectedRevision, null);
                Intent committed = writeCoordinator.commitMutation(
                    intentId,
                    "patch",
                    requestKey,
                    expectedRevision != null ? expectedRevision : 0,
                    snapshot -> {
                        if (request.deadline() != null) {
                            snapshot.setDeadline(request.deadline());
                        }
                        if (request.expiredAction() != null) {
                            snapshot.setExpiredAction(request.expiredAction());
                        }
                        if (request.redelivery() != null) {
                            snapshot.setRedelivery(request.redelivery());
                        }
                        if (request.tags() != null) {
                            snapshot.setTags(request.tags());
                        }
                        if (request.executeAt() != null) {
                            snapshot.setExecuteAt(request.executeAt());
                        }
                        return snapshot;
                    });
                return IntentResponse.from(committed);
            }

            // Fallback: direct engine call (no coordinator)
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
        } catch (RaftWriteBackPressureException e) {
            logger.warn("Raft write backpressure for patch {}: {}", intentId, e.getMessage());
            return raftBackPressureResponse("patch", intentId, e.retryAfterMs(), e.getMessage());
        } catch (RaftWriteConflictException e) {
            logger.warn("Raft revision conflict for patch {}: {}", intentId, e.getMessage());
            return raftRevisionConflictResponse("patch", intentId, e.expectedRevision(), e.actualRevision(), e.getMessage());
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
        Long expectedRevision;
        try {
            expectedRevision = expectedRevision(headers);
        } catch (IllegalArgumentException e) {
            return errorResponse(400, "40001", e.getMessage());
        }
        if (raftStatus != null && raftStatus.isRaftEnabled() && expectedRevision == null) {
            return errorResponse(428, "42801", "X-LoomQ-Expected-Revision header is required for Raft writes");
        }

        Optional<Intent> current = engine.getIntent(intentId);
        if (current.isEmpty()) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        Intent intent = current.get();
        if (!intent.getStatus().isCancellable()) {
            return errorResponse(422, "42204", "Intent cannot be cancelled in state: " + intent.getStatus(),
                Map.of("currentState", intent.getStatus().name(),
                       "precisionTier", intent.getPrecisionTier()));
        }

        try {
            if (writeCoordinator != null && raftEnabled) {
                String requestKey = requestKey("cancel", intentId, body, headers, expectedRevision, null);
                Intent committed = writeCoordinator.commitMutation(
                    intentId,
                    "cancel",
                    requestKey,
                    expectedRevision != null ? expectedRevision : 0,
                    snapshot -> {
                        snapshot.transitionTo(IntentStatus.CANCELED);
                        return snapshot;
                    });
                return IntentResponse.from(committed);
            }

            // Fallback: direct engine call (no coordinator)
            if (!engine.cancelIntent(intentId)) {
                return errorResponse(500, "50002", "Failed to cancel intent");
            }

            return IntentResponse.from(intent);
        } catch (RaftWriteUnavailableException e) {
            logger.warn("Raft write unavailable for cancel {}: {}", intentId, e.getMessage());
            return raftWriteUnavailableResponse("cancel", intentId, e.reason());
        } catch (RaftWriteBackPressureException e) {
            logger.warn("Raft write backpressure for cancel {}: {}", intentId, e.getMessage());
            return raftBackPressureResponse("cancel", intentId, e.retryAfterMs(), e.getMessage());
        } catch (RaftWriteConflictException e) {
            logger.warn("Raft revision conflict for cancel {}: {}", intentId, e.getMessage());
            return raftRevisionConflictResponse("cancel", intentId, e.expectedRevision(), e.actualRevision(), e.getMessage());
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
        Long expectedRevision;
        try {
            expectedRevision = expectedRevision(headers);
        } catch (IllegalArgumentException e) {
            return errorResponse(400, "40001", e.getMessage());
        }
        if (raftStatus != null && raftStatus.isRaftEnabled() && expectedRevision == null) {
            return errorResponse(428, "42801", "X-LoomQ-Expected-Revision header is required for Raft writes");
        }

        Optional<Intent> current = engine.getIntent(intentId);
        if (current.isEmpty()) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        try {
            if (writeCoordinator != null && raftEnabled) {
                String requestKey = requestKey("fire-now", intentId, body, headers, expectedRevision, null);
                writeCoordinator.commitMutation(
                    intentId,
                    "fire-now",
                    requestKey,
                    expectedRevision != null ? expectedRevision : 0,
                    snapshot -> {
                        snapshot.setExecuteAt(java.time.Instant.now());
                        return snapshot;
                    });
                return new IntentActionResponse(intentId, IntentStatus.DISPATCHING.name());
            }

            // Fallback: direct engine call (no coordinator)
            if (!engine.fireNow(intentId)) {
                return errorResponse(500, "50003", "Failed to trigger intent immediately");
            }

            return new IntentActionResponse(intentId, IntentStatus.DISPATCHING.name());
        } catch (RaftWriteUnavailableException e) {
            logger.warn("Raft write unavailable for fire-now {}: {}", intentId, e.getMessage());
            return raftWriteUnavailableResponse("fire-now", intentId, e.reason());
        } catch (RaftWriteBackPressureException e) {
            logger.warn("Raft write backpressure for fire-now {}: {}", intentId, e.getMessage());
            return raftBackPressureResponse("fire-now", intentId, e.retryAfterMs(), e.getMessage());
        } catch (RaftWriteConflictException e) {
            logger.warn("Raft revision conflict for fire-now {}: {}", intentId, e.getMessage());
            return raftRevisionConflictResponse("fire-now", intentId, e.expectedRevision(), e.actualRevision(), e.getMessage());
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

    /**
     * GET /v1/intents?status=DEAD_LETTERED&limit=50&offset=0
     */
    public Object listIntents(HttpMethod method, String uri, byte[] body,
                              Map<String, String> headers,
                              Map<String, String> pathParams) {
        if (isRaftFollowerReadBlocked()) {
            return leaderHintResponse(null);
        }

        Map<String, String> queryParams = parseQueryString(uri);
        String statusParam = queryParams.get("status");
        int limit = parseIntParam(queryParams, "limit", 50, 1, 500);
        int offset = parseIntParam(queryParams, "offset", 0, 0, Integer.MAX_VALUE);

        if (statusParam == null || statusParam.isBlank()) {
            return errorResponse(400, "40002", "Query parameter 'status' is required");
        }

        IntentStatus status;
        try {
            status = IntentStatus.valueOf(statusParam.toUpperCase());
        } catch (IllegalArgumentException e) {
            return errorResponse(400, "40003", "Invalid status value: " + statusParam);
        }

        List<Intent> intents = engine.getIntentStore().findByStatus(status, offset, limit);
        long total = engine.getIntentStore().countByStatus(status);

        List<IntentListResponse.IntentSummary> summaries = intents.stream()
            .map(intent -> {
                DeathReport deathReport = null;
                if (intent.getStatus() == IntentStatus.DEAD_LETTERED) {
                    deathReport = buildDeathReport(intent);
                }
                return IntentListResponse.IntentSummary.from(intent, deathReport);
            })
            .toList();

        return IntentListResponse.of(summaries, total, offset, limit);
    }

    /**
     * POST /v1/intents/{intentId}/revive
     */
    public Object reviveIntent(HttpMethod method, String uri, byte[] body,
                               Map<String, String> headers,
                               Map<String, String> pathParams) throws Exception {
        String intentId = pathParams.get("intentId");
        if (isRaftWriteBlocked()) {
            return leaderHintWriteResponse("revive", intentId);
        }

        Optional<Intent> current = engine.getIntent(intentId);
        if (current.isEmpty()) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        Intent intent = current.get();
        if (intent.getStatus() != IntentStatus.DEAD_LETTERED) {
            return errorResponse(422, "42207", "Intent can only be revived from DEAD_LETTERED state, current state: " + intent.getStatus(),
                Map.of("currentState", intent.getStatus().name()));
        }

        ReviveIntentRequest request = null;
        if (body != null && body.length > 0) {
            request = jsonCodec.read(body, ReviveIntentRequest.class);
        }

        if (request != null) {
            if (request.executeAt() != null) {
                intent.setExecuteAt(request.executeAt());
            }
            if (request.deadline() != null) {
                intent.setDeadline(request.deadline());
            }
            if (request.callback() != null) {
                intent.setCallback(request.callback());
            }
            if (request.redelivery() != null) {
                intent.setRedelivery(request.redelivery());
            }
        }

        intent.setAttempts(0);

        Instant requestedExecuteAt = intent.getExecuteAt();
        if (requestedExecuteAt == null || requestedExecuteAt.isBefore(Instant.now())) {
            if (requestedExecuteAt != null && requestedExecuteAt.isBefore(Instant.now())) {
                logger.warn("Revive for {} has executeAt in the past ({}), resetting to now + precisionWindow",
                    intentId, requestedExecuteAt);
            }
            intent.setExecuteAt(Instant.now().plusMillis(intent.getPrecisionTier().getPrecisionWindowMs()));
        }

        try {
            if (writeCoordinator != null && raftEnabled) {
                Intent snapshot = intent.copy();
                snapshot.transitionTo(IntentStatus.SCHEDULED);
                String requestKey = requestKey("revive", intentId, body, headers, null, null);
                Intent committed = writeCoordinator.commitSnapshot(snapshot, "revive", requestKey);
                IntentTraceStore.getInstance().updateStatus(intentId, IntentStatus.SCHEDULED);
                logger.info("Intent revived via coordinator: id={}, newExecuteAt={}, tier={}",
                    committed.getIntentId(), committed.getExecuteAt(), committed.getPrecisionTier());
                return IntentResponse.from(committed);
            }

            // Fallback: direct engine call (no coordinator)
            engine.createIntent(intent, intent.getAckMode()).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof BackPressureException bpe) {
                return new HttpErrorResponse(429, ErrorResponse.of(
                    "42900",
                    "Backpressure during revive: " + bpe.getMessage(),
                    Map.of("retryAfterMs", (Object) bpe.getRetryAfterMs()),
                    ErrorRecoveryAdvisor.advise("42900", Map.of("retryAfterMs", bpe.getRetryAfterMs()))
                ));
            }
            throw e;
        }

        // engine.createIntent → scheduler.schedule → recordCreated resets trace to CREATED.
        // Update to SCHEDULED so the trace reflects the revived intent's actual state.
        IntentTraceStore.getInstance().updateStatus(intentId, IntentStatus.SCHEDULED);

        logger.info("Intent revived: id={}, newExecuteAt={}, tier={}",
            intent.getIntentId(), intent.getExecuteAt(), intent.getPrecisionTier());
        return IntentResponse.from(intent);
    }

    private DeathReport buildDeathReport(Intent intent) {
        IntentTrace trace = IntentTraceStore.getInstance().get(intent.getIntentId());
        int maxAttempts = intent.getRedelivery() != null ? intent.getRedelivery().getMaxAttempts() : 5;
        long lifetimeMs = 0;
        if (trace != null && trace.createdAtMs() > 0) {
            lifetimeMs = System.currentTimeMillis() - trace.createdAtMs();
        }
        return new DeathReport(
            intent.getAttempts(),
            maxAttempts,
            intent.getUpdatedAt() != null ? intent.getUpdatedAt().toString() : null,
            trace != null ? trace.failureReason() : null,
            trace != null ? trace.failureHttpStatus() : null,
            intent.getCallback() != null ? intent.getCallback().getUrl() : null,
            intent.getPrecisionTier() != null ? intent.getPrecisionTier().name() : null,
            lifetimeMs
        );
    }

    private Map<String, String> parseQueryString(String uri) {
        Map<String, String> params = new LinkedHashMap<>();
        int queryStart = uri.indexOf('?');
        if (queryStart < 0 || queryStart >= uri.length() - 1) {
            return params;
        }
        String query = uri.substring(queryStart + 1);
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                String key = pair.substring(0, eq);
                String value = pair.substring(eq + 1);
                params.put(key, value);
            }
        }
        return params;
    }

    private int parseIntParam(Map<String, String> params, String key, int defaultValue, int min, int max) {
        String value = params.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value);
            return Math.max(min, Math.min(max, parsed));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private String callbackUrl(CreateIntentRequest request) {
        if (request.callback() == null) {
            return null;
        }
        return request.callback().getUrl();
    }

    private Object errorResponse(int status, String code, String message) {
        RecoveryHint recovery = ErrorRecoveryAdvisor.advise(code, null);
        return new HttpErrorResponse(status, ErrorResponse.of(code, message, null, recovery));
    }

    private Object errorResponse(int status, String code, String message, Map<String, Object> context) {
        RecoveryHint recovery = ErrorRecoveryAdvisor.advise(code, context);
        return new HttpErrorResponse(status, ErrorResponse.of(code, message, null, recovery));
    }

    private Intent buildCreateIntentDraft(CreateIntentRequest request, String intentId) {
        Intent intent = new Intent(intentId);
        intent.setExecuteAt(request.executeAt());
        intent.setDeadline(request.deadline());
        intent.setExpiredAction(request.expiredAction());
        if (request.slo() != null) {
            var rec = com.loomq.application.scheduler.TierAdvisor.recommend(
                request.slo().maxTardinessMs(), request.slo().reliability());
            intent.setPrecisionTier(rec.tier());
            intent.setAckMode(request.slo().reliability().toAckMode());
        } else {
            intent.setPrecisionTier(request.precisionTier() != null ? request.precisionTier() : DEFAULT_PRECISION_TIER);
            intent.setAckMode(request.ackLevel() != null ? request.ackLevel() : AckMode.DURABLE);
        }
        intent.setWalMode(request.walMode());
        intent.setShardKey(request.shardKey());
        intent.setCallback(request.callback());
        intent.setRedelivery(request.redelivery());
        intent.setIdempotencyKey(request.idempotencyKey());
        intent.setTags(request.tags());
        return intent;
    }

    private boolean isRaftFollowerReadBlocked() {
        return raftStatus != null
            && raftStatus.isRaftEnabled()
            && !raftStatus.canServeLinearizableRead();
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

        RecoveryHint recovery = ErrorRecoveryAdvisor.advise("50301", details);
        return new HttpErrorResponse(
            HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
            ErrorResponse.of(
                "50301",
                "Read must be served by the Raft leader",
                details,
                recovery
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

        RecoveryHint recovery = ErrorRecoveryAdvisor.advise("50302", details);
        return new HttpErrorResponse(
            HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
            ErrorResponse.of(
                "50302",
                reason,
                details,
                recovery
            )
        );
    }

    private Object raftBackPressureResponse(String operation, String intentId, long retryAfterMs, String reason) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("retryable", true);
        details.put("nodeRole", raftStatus != null ? raftStatus.role().name() : "UNKNOWN");
        details.put("operation", operation);
        if (intentId != null && !intentId.isBlank()) {
            details.put("intentId", intentId);
        }
        details.put("retryAfterMs", retryAfterMs);
        String leaderId = raftStatus != null ? raftStatus.currentLeaderId() : null;
        if (leaderId != null && !leaderId.isBlank()) {
            details.put("leaderId", leaderId);
        }

        RecoveryHint recovery = ErrorRecoveryAdvisor.advise("50303", details);
        return new HttpErrorResponse(
            HttpResponseStatus.SERVICE_UNAVAILABLE.code(),
            ErrorResponse.of(
                "50303",
                reason,
                details,
                recovery
            )
        );
    }

    private Object raftRevisionConflictResponse(String operation, String intentId, long expectedRevision, long actualRevision, String reason) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("retryable", false);
        details.put("nodeRole", raftStatus != null ? raftStatus.role().name() : "UNKNOWN");
        details.put("operation", operation);
        details.put("intentId", intentId);
        details.put("expectedRevision", expectedRevision);
        details.put("actualRevision", actualRevision);
        String leaderId = raftStatus != null ? raftStatus.currentLeaderId() : null;
        if (leaderId != null && !leaderId.isBlank()) {
            details.put("leaderId", leaderId);
        }

        RecoveryHint recovery = ErrorRecoveryAdvisor.advise("40902", details);
        return new HttpErrorResponse(
            409,
            ErrorResponse.of(
                "40902",
                reason,
                details,
                recovery
            )
        );
    }

    private Long expectedRevision(Map<String, String> headers) {
        String value = header(headers, EXPECTED_REVISION_HEADER);
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid X-LoomQ-Expected-Revision header: " + value, e);
        }
    }

    private String requestKey(String operation, String intentId, byte[] body, Map<String, String> headers,
                              Long expectedRevision, String idempotencyKey) {
        String requestId = header(headers, REQUEST_ID_HEADER);
        if (requestId != null && !requestId.isBlank()) {
            return requestId.trim();
        }

        StringBuilder key = new StringBuilder(operation).append(':').append(intentId);
        if (expectedRevision != null) {
            key.append(':').append(expectedRevision);
        }
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            key.append(':').append(idempotencyKey.trim());
        }
        key.append(':').append(sha256Hex(body));
        return key.toString();
    }

    private boolean hasStableCreateIdentity(Map<String, String> headers, String idempotencyKey) {
        return (idempotencyKey != null && !idempotencyKey.isBlank())
            || (header(headers, REQUEST_ID_HEADER) != null && !header(headers, REQUEST_ID_HEADER).isBlank());
    }

    private String deriveIntentId(String requestKey) {
        String hash = sha256Hex(requestKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return "intent_" + hash.substring(0, 16);
    }

    private String header(Map<String, String> headers, String name) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(name)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private String sha256Hex(byte[] body) {
        byte[] payload = body != null ? body : new byte[0];
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(payload));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest unavailable", e);
        }
    }

    private Object conflictResponse(String intentId) {
        Map<String, Object> ctx = Map.of("intentId", intentId);
        RecoveryHint recovery = ErrorRecoveryAdvisor.advise("40901", ctx);
        return new HttpErrorResponse(409, ErrorResponse.of(
            "40901",
            "Idempotency key conflict",
            Map.of("intentId", intentId),
            recovery
        ));
    }

    /**
     * Created response wrapper carrying an explicit HTTP status code.
     */
    public record CreatedResponse(int status, Object body) {}
}
