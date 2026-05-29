package com.loomq.channel.http.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 批量 Webhook 请求处理器参考实现。
 *
 * <p>展示如何接收和解析 {@link BatchedHttpDeliveryHandler} 发送的批量请求，
 * 以及如何构建逐个 intent 的响应。
 *
 * <p>使用方式：
 * <pre>{@code
 * BatchWebhookHandler handler = new BatchWebhookHandler(intent -> {
 *     // 自定义业务逻辑
 *     System.out.println("Processing intent: " + intent.intentId());
 *     return BatchWebhookHandler.IntentResult.success(intent.intentId());
 * });
 *
 * // 在 HTTP 服务器中调用
 * String response = handler.handleRequest(requestBody);
 * }</pre>
 *
 * @author loomq
 * @since v0.9.2
 */
public class BatchWebhookHandler {

    private static final Logger logger = LoggerFactory.getLogger(BatchWebhookHandler.class);

    private final Consumer<IntentEvent> processor;

    /**
     * 创建批量 webhook 处理器。
     *
     * @param processor 单个 intent 的处理逻辑
     */
    public BatchWebhookHandler(Consumer<IntentEvent> processor) {
        this.processor = processor;
    }

    /**
     * 处理批量 webhook 请求。
     *
     * @param requestBody JSON 请求体
     * @return JSON 响应体（包含逐个 intent 的处理结果）
     */
    public String handleRequest(String requestBody) {
        if (requestBody == null || requestBody.isBlank()) {
            return ErrorResponse.EMPTY_BODY.toJson();
        }

        try {
            List<IntentEvent> intents = parseBatchRequest(requestBody);
            if (intents.isEmpty()) {
                return ErrorResponse.NO_INTENTS.toJson();
            }

            logger.info("Received batch webhook with {} intents", intents.size());

            List<IntentResult> results = new ArrayList<>();
            for (IntentEvent intent : intents) {
                try {
                    processor.accept(intent);
                    results.add(IntentResult.success(intent.intentId()));
                } catch (Exception e) {
                    logger.warn("Failed to process intent {}: {}", intent.intentId(), e.getMessage());
                    results.add(IntentResult.failed(intent.intentId(), e.getMessage()));
                }
            }

            return buildResponse(results);
        } catch (Exception e) {
            logger.error("Failed to parse batch request: {}", e.getMessage());
            return ErrorResponse.PARSE_ERROR.toJson();
        }
    }

    /**
     * 解析批量请求 JSON。
     *
     * <p>支持三种格式：
     * <ul>
     *   <li>JSON 数组格式: {@code [{"intentId":"...","precisionTier":"..."},...]}</li>
     *   <li>JSON 对象格式: {@code {"intents":[...]}}</li>
     *   <li>单个格式: {@code {"intentId":"...","precisionTier":"..."}}</li>
     * </ul>
     */
    private List<IntentEvent> parseBatchRequest(String body) {
        List<IntentEvent> intents = new ArrayList<>();

        // 检查是否是 JSON 数组格式
        if (body.trim().startsWith("[")) {
            parseJsonArray(body, intents);
        } else if (body.contains("\"intents\"")) {
            // JSON 对象格式 {"intents":[...]}
            String arrayJson = extractJsonArray(body, "intents");
            if (arrayJson != null) {
                parseJsonArray(arrayJson, intents);
            }
        } else {
            // 单个 intent 格式
            IntentEvent event = parseIntentObject(body);
            if (event != null) {
                intents.add(event);
            }
        }

        return intents;
    }

    /**
     * 解析 JSON 数组中的 intent 对象。
     */
    private void parseJsonArray(String arrayJson, List<IntentEvent> intents) {
        int depth = 0;
        int objStart = -1;
        for (int i = 0; i < arrayJson.length(); i++) {
            char c = arrayJson.charAt(i);
            if (c == '{') {
                if (depth == 0) objStart = i;
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0 && objStart >= 0) {
                    String objJson = arrayJson.substring(objStart, i + 1);
                    IntentEvent event = parseIntentObject(objJson);
                    if (event != null) {
                        intents.add(event);
                    }
                    objStart = -1;
                }
            }
        }
    }

    private IntentEvent parseIntentObject(String json) {
        String intentId = extractField(json, "intentId");
        String precisionTier = extractField(json, "precisionTier");
        if (intentId != null) {
            return new IntentEvent(intentId, precisionTier != null ? precisionTier : "STANDARD");
        }
        return null;
    }

    private String buildResponse(List<IntentResult> results) {
        StringBuilder sb = new StringBuilder("{\"results\":[");
        for (int i = 0; i < results.size(); i++) {
            if (i > 0) sb.append(",");
            IntentResult r = results.get(i);
            sb.append("{\"intentId\":\"").append(escapeJson(r.intentId()))
              .append("\",\"status\":\"").append(r.status()).append("\"}");
            if (r.errorMessage() != null) {
                sb.append(",\"error\":\"").append(escapeJson(r.errorMessage())).append("\"}");
            }
        }
        sb.append("]}");
        return sb.toString();
    }

    // ── JSON 辅助方法 ──

    private static String extractJsonArray(String body, String fieldName) {
        String search = "\"" + fieldName + "\":";
        int idx = body.indexOf(search);
        if (idx < 0) return null;
        int start = body.indexOf('[', idx);
        if (start < 0) return null;
        int depth = 0;
        for (int i = start; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '[') depth++;
            else if (c == ']') depth--;
            if (depth == 0) return body.substring(start, i + 1);
        }
        return null;
    }

    private static String extractField(String json, String fieldName) {
        String search = "\"" + fieldName + "\":\"";
        int idx = json.indexOf(search);
        if (idx < 0) return null;
        int start = idx + search.length();
        int end = json.indexOf("\"", start);
        if (end < 0) return null;
        return json.substring(start, end);
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // ── 数据类型 ──

    /**
     * Intent 事件（从批量请求中解析）。
     */
    public record IntentEvent(String intentId, String precisionTier) {}

    /**
     * 单个 Intent 的处理结果。
     */
    public record IntentResult(String intentId, String status, String errorMessage) {
        public static IntentResult success(String intentId) {
            return new IntentResult(intentId, "SUCCESS", null);
        }

        public static IntentResult failed(String intentId, String error) {
            return new IntentResult(intentId, "FAILED", error);
        }

        public static IntentResult rejected(String intentId, String reason) {
            return new IntentResult(intentId, "REJECTED", reason);
        }
    }

    /**
     * 错误响应。
     */
    private enum ErrorResponse {
        EMPTY_BODY("Request body is empty"),
        NO_INTENTS("No intents found in request"),
        PARSE_ERROR("Failed to parse request body");

        private final String message;

        ErrorResponse(String message) {
            this.message = message;
        }

        String toJson() {
            return "{\"error\":\"" + message + "\"}";
        }
    }
}
