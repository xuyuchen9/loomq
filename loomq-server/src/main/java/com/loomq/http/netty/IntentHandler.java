package com.loomq.http.netty;

import com.loomq.LoomqEngine;
import com.loomq.api.CreateIntentRequest;
import com.loomq.api.IntentActionResponse;
import com.loomq.api.ErrorResponse;
import com.loomq.api.IntentResponse;
import com.loomq.api.IntentValidator;
import com.loomq.api.PatchIntentRequest;
import com.loomq.api.ValidationResult;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.http.json.JsonCodec;
import com.loomq.replication.AckLevel;
import com.loomq.store.IdempotencyResult;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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
    private final JsonCodec jsonCodec;

    public IntentHandler(LoomqEngine engine) {
        this.engine = engine;
        this.jsonCodec = JsonCodec.instance();
    }

    /**
     * Register API routes.
     */
    public void register(RadixRouter router) {
        router.add(HttpMethod.POST, "/v1/intents", this::createIntent);
        router.add(HttpMethod.GET, "/v1/intents/{intentId}", this::getIntent);
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

        Intent intent = new Intent(intentId);
        intent.setExecuteAt(request.executeAt());
        intent.setDeadline(request.deadline());
        intent.setExpiredAction(request.expiredAction());
        intent.setPrecisionTier(request.precisionTier() != null ? request.precisionTier() : DEFAULT_PRECISION_TIER);
        intent.setShardKey(request.shardKey());
        intent.setAckLevel(request.ackLevel() != null ? request.ackLevel() : AckLevel.DURABLE);
        intent.setCallback(request.callback());
        intent.setRedelivery(request.redelivery());
        intent.setIdempotencyKey(request.idempotencyKey());
        intent.setTags(request.tags());

        AckMode ackMode = toAckMode(intent.getAckLevel());
        engine.createIntent(intent, ackMode).join();

        logger.info("Intent created: id={}, executeAt={}, ackLevel={}",
            intent.getIntentId(), intent.getExecuteAt(), intent.getAckLevel());

        return new IntentHandler.CreatedResponse(201, IntentResponse.from(intent));
    }

    /**
     * GET /v1/intents/{intentId}
     */
    public Object getIntent(HttpMethod method, String uri, byte[] body,
                            Map<String, String> headers,
                            Map<String, String> pathParams) {
        String intentId = pathParams.get("intentId");

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
    }

    /**
     * POST /v1/intents/{intentId}/cancel
     */
    public Object cancelIntent(HttpMethod method, String uri, byte[] body,
                               Map<String, String> headers,
                               Map<String, String> pathParams) {
        String intentId = pathParams.get("intentId");

        Optional<Intent> current = engine.getIntent(intentId);
        if (current.isEmpty()) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        Intent intent = current.get();
        if (!intent.getStatus().isCancellable()) {
            return errorResponse(422, "42204", "Intent cannot be cancelled in state: " + intent.getStatus());
        }

        if (!engine.cancelIntent(intentId)) {
            return errorResponse(500, "50002", "Failed to cancel intent");
        }

        return IntentResponse.from(intent);
    }

    /**
     * POST /v1/intents/{intentId}/fire-now
     */
    public Object fireNow(HttpMethod method, String uri, byte[] body,
                          Map<String, String> headers,
                          Map<String, String> pathParams) {
        String intentId = pathParams.get("intentId");

        Optional<Intent> current = engine.getIntent(intentId);
        if (current.isEmpty()) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        if (!engine.fireNow(intentId)) {
            return errorResponse(500, "50003", "Failed to trigger intent immediately");
        }

        return new IntentActionResponse(intentId, IntentStatus.DISPATCHING.name());
    }

    private AckMode toAckMode(AckLevel ackLevel) {
        if (ackLevel == null) {
            return AckMode.DURABLE;
        }
        return AckMode.valueOf(ackLevel.name());
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
