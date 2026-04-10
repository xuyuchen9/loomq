package com.loomq.http.netty;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.loomq.dispatcher.BatchDispatcher;
import com.loomq.entity.v5.Intent;
import com.loomq.entity.v5.IntentStatus;
import com.loomq.entity.v5.PrecisionTier;
import com.loomq.replication.AckLevel;
import com.loomq.scheduler.v5.PrecisionScheduler;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import com.loomq.wal.v2.IntentWalV2;
import io.netty.handler.codec.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Intent API 处理器 V2 - 简化实现
 *
 * 核心简化：
 * 1. 使用 IntentWalV2（二进制序列化，~100ns）
 * 2. 使用 BatchDispatcher（批量同步投递）
 * 3. 流式 JSON 解析保留（高效）
 *
 * @author loomq
 * @since v0.6.1
 */
public class IntentHandler {

    private static final Logger logger = LoggerFactory.getLogger(IntentHandler.class);

    private final IntentStore intentStore;
    private final IntentWalV2 intentWal;
    private final BatchDispatcher dispatcher;
    private final PrecisionScheduler scheduler;
    private final JsonFactory jsonFactory;

    public IntentHandler(IntentStore intentStore,
                         IntentWalV2 intentWal,
                         BatchDispatcher dispatcher,
                         PrecisionScheduler scheduler) {
        this.intentStore = intentStore;
        this.intentWal = intentWal;
        this.dispatcher = dispatcher;
        this.scheduler = scheduler;
        this.jsonFactory = new JsonFactory();
    }

    /**
     * 注册路由
     */
    public void register(RadixRouter router) {
        router.add(HttpMethod.POST, "/v1/intents", this::createIntent);
        router.add(HttpMethod.GET, "/v1/intents/{intentId}", this::getIntent);
        router.add(HttpMethod.PATCH, "/v1/intents/{intentId}", this::patchIntent);
        router.add(HttpMethod.POST, "/v1/intents/{intentId}/cancel", this::cancelIntent);
        router.add(HttpMethod.POST, "/v1/intents/{intentId}/fire-now", this::fireNow);
    }

    /**
     * POST /v1/intents - 创建 Intent
     *
     * 简化持久化逻辑：
     * - ASYNC: 异步写入 WAL，不等待刷盘
     * - DURABLE: 同步写入 WAL，等待刷盘完成
     */
    public Object createIntent(HttpMethod method, String uri, byte[] body,
                               Map<String, String> headers,
                               Map<String, String> pathParams) throws Exception {

        // 流式解析 JSON
        String intentId = null;
        String executeAtStr = null;
        String deadlineStr = null;
        String shardKey = null;
        String precisionTier = null;
        String ackLevelStr = null;
        String idempotencyKey = null;
        String callbackUrl = null;
        String callbackMethod = null;

        try (JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(body))) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return errorResponse("40001", "Invalid JSON");
            }

            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = parser.getCurrentName();
                if (fieldName == null) continue;

                JsonToken token = parser.nextToken();
                switch (fieldName) {
                    case "intentId" -> intentId = parser.getValueAsString();
                    case "executeAt" -> executeAtStr = parser.getValueAsString();
                    case "deadline" -> deadlineStr = parser.getValueAsString();
                    case "shardKey" -> shardKey = parser.getValueAsString();
                    case "precisionTier" -> precisionTier = parser.getValueAsString();
                    case "ackLevel" -> ackLevelStr = parser.getValueAsString();
                    case "idempotencyKey" -> idempotencyKey = parser.getValueAsString();
                    case "callback" -> {
                        // 简单解析 callback URL
                        while (parser.nextToken() != JsonToken.END_OBJECT) {
                            String cbField = parser.getCurrentName();
                            parser.nextToken();
                            if ("url".equals(cbField)) {
                                callbackUrl = parser.getValueAsString();
                            } else if ("method".equals(cbField)) {
                                callbackMethod = parser.getValueAsString();
                            }
                        }
                    }
                    default -> {
                        if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                            parser.skipChildren();
                        }
                    }
                }
            }
        }

        // 验证必填字段
        if (executeAtStr == null) {
            return errorResponse("42201", "executeAt is required");
        }
        if (deadlineStr == null) {
            return errorResponse("42202", "deadline is required");
        }
        if (shardKey == null || shardKey.isBlank()) {
            return errorResponse("42203", "shardKey is required");
        }
        if (callbackUrl == null) {
            return errorResponse("42204", "callback.url is required");
        }

        // 解析时间
        Instant executeAt = Instant.parse(executeAtStr);
        Instant deadline = Instant.parse(deadlineStr);

        if (deadline.isBefore(executeAt)) {
            return errorResponse("42205", "deadline must be after executeAt");
        }

        // 生成或使用提供的 ID
        if (intentId == null || intentId.isBlank()) {
            intentId = UUID.randomUUID().toString();
        }

        AckLevel ackLevel = parseAckLevel(ackLevelStr);

        // 幂等性检查
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            IdempotencyResult result = intentStore.checkIdempotency(idempotencyKey);
            if (result.isDuplicateActive()) {
                return intentResponse(result.getIntent());
            }
            if (result.isDuplicateTerminal()) {
                return conflictResponse(result.getIntent().getIntentId());
            }
        }

        // 创建 Intent
        Intent intent = new Intent(intentId);
        intent.setExecuteAt(executeAt);
        intent.setDeadline(deadline);
        intent.setShardKey(shardKey);
        intent.setPrecisionTier(parsePrecisionTier(precisionTier));
        intent.setAckLevel(ackLevel);
        intent.setIdempotencyKey(idempotencyKey);

        // 设置回调
        com.loomq.entity.v5.Callback callback = new com.loomq.entity.v5.Callback(callbackUrl);
        if (callbackMethod != null) {
            callback.setMethod(callbackMethod);
        }
        intent.setCallback(callback);

        intent.transitionTo(IntentStatus.SCHEDULED);

        // ========== 持久化逻辑 ==========
        if (intentWal != null) {
            switch (ackLevel) {
                case ASYNC -> {
                    // 异步：写入存储 + 异步 WAL
                    intentStore.save(intent);
                    intentWal.appendCreateAsync(intent)
                        .exceptionally(e -> {
                            logger.warn("ASYNC WAL failed for {}: {}",
                                intent.getIntentId(), e.getMessage());
                            return null;
                        });
                }
                case DURABLE -> {
                    // 持久化：写入存储 + 同步 WAL
                    intentStore.save(intent);
                    intentWal.appendCreateDurable(intent).join();
                }
                case REPLICATED -> {
                    // 复制确认：写入存储 + 同步 WAL + Replica ACK
                    intentStore.save(intent);
                    intentWal.appendCreateReplicated(intent).join();
                }
            }
        } else {
            intentStore.save(intent);
        }

        // 调度任务
        if (scheduler != null) {
            scheduler.schedule(intent);
        }

        return new IntentHandler.CreatedResponse(201, intentResponse(intent));
    }

    /**
     * GET /v1/intents/{intentId}
     */
    public Object getIntent(HttpMethod method, String uri, byte[] body,
                           Map<String, String> headers,
                           Map<String, String> pathParams) {
        String intentId = pathParams.get("intentId");

        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        return intentResponse(intent);
    }

    /**
     * PATCH /v1/intents/{intentId}
     */
    public Object patchIntent(HttpMethod method, String uri, byte[] body,
                             Map<String, String> headers,
                             Map<String, String> pathParams) throws Exception {
        String intentId = pathParams.get("intentId");

        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        if (!intent.getStatus().isModifiable()) {
            return errorResponse(422, "42206", "Intent cannot be modified");
        }

        String executeAtStr = null;
        String deadlineStr = null;

        try (JsonParser parser = jsonFactory.createParser(new ByteArrayInputStream(body))) {
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String fieldName = parser.getCurrentName();
                if (fieldName == null) continue;

                parser.nextToken();
                switch (fieldName) {
                    case "executeAt" -> executeAtStr = parser.getValueAsString();
                    case "deadline" -> deadlineStr = parser.getValueAsString();
                    default -> parser.skipChildren();
                }
            }
        }

        if (executeAtStr != null) {
            intent.setExecuteAt(Instant.parse(executeAtStr));
        }
        if (deadlineStr != null) {
            intent.setDeadline(Instant.parse(deadlineStr));
        }

        intentStore.update(intent);

        // 更新 WAL
        if (intentWal != null) {
            intentWal.appendStatusUpdate(intentId, intent.getStatus(), intent.getStatus());
        }

        return intentResponse(intent);
    }

    /**
     * POST /v1/intents/{intentId}/cancel
     */
    public Object cancelIntent(HttpMethod method, String uri, byte[] body,
                              Map<String, String> headers,
                              Map<String, String> pathParams) {
        String intentId = pathParams.get("intentId");

        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        if (!intent.getStatus().isCancellable()) {
            return errorResponse(422, "42207", "Intent cannot be cancelled");
        }

        IntentStatus oldStatus = intent.getStatus();
        intent.transitionTo(IntentStatus.CANCELED);
        intentStore.update(intent);

        if (intentWal != null) {
            intentWal.appendStatusUpdate(intentId, oldStatus, IntentStatus.CANCELED);
        }

        return intentResponse(intent);
    }

    /**
     * POST /v1/intents/{intentId}/fire-now
     */
    public Object fireNow(HttpMethod method, String uri, byte[] body,
                         Map<String, String> headers,
                         Map<String, String> pathParams) {
        String intentId = pathParams.get("intentId");

        Intent intent = intentStore.findById(intentId);
        if (intent == null) {
            return errorResponse(404, "40401", "Intent not found: " + intentId);
        }

        intent.setExecuteAt(Instant.now());
        intent.transitionTo(IntentStatus.DUE);
        intentStore.update(intent);

        // 立即投递
        if (dispatcher != null) {
            try {
                dispatcher.submit(intent);
            } catch (BatchDispatcher.BackPressureException e) {
                return errorResponse(503, "50301", "Dispatcher overloaded");
            }
        }

        return Map.of("intentId", intentId, "status", IntentStatus.DISPATCHING.name());
    }

    // ==================== 辅助方法 ====================

    private PrecisionTier parsePrecisionTier(String value) {
        if (value == null) return PrecisionTier.STANDARD;
        try {
            return PrecisionTier.valueOf(value);
        } catch (IllegalArgumentException e) {
            return PrecisionTier.STANDARD;
        }
    }

    private AckLevel parseAckLevel(String value) {
        if (value == null) return AckLevel.DURABLE;
        try {
            return AckLevel.valueOf(value);
        } catch (IllegalArgumentException e) {
            return AckLevel.DURABLE;
        }
    }

    private IntentResponseData intentResponse(Intent intent) {
        return new IntentResponseData(intent);
    }

    private Map<String, Object> errorResponse(String code, String message) {
        return Map.of("error", Map.of("code", code, "message", message));
    }

    private Map<String, Object> errorResponse(int status, String code, String message) {
        return Map.of("status", status, "error", Map.of("code", code, "message", message));
    }

    private Map<String, Object> conflictResponse(String intentId) {
        return Map.of(
            "status", 409,
            "error", Map.of(
                "code", "40901",
                "message", "Idempotency key conflict",
                "intentId", intentId
            )
        );
    }

    /**
     * 创建成功响应，携带 HTTP 状态码
     */
    public record CreatedResponse(int status, Object body) {}
}
