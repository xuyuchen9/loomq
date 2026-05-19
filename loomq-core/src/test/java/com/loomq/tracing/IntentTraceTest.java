package com.loomq.tracing;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class IntentTraceTest {

    private static IntentTrace createTrace(String intentId, String failureReason, Integer failureHttpStatus) {
        return new IntentTrace(
            intentId, "trace-" + intentId, PrecisionTier.STANDARD, IntentStatus.DEAD_LETTERED,
            System.currentTimeMillis(), System.currentTimeMillis(), System.currentTimeMillis(),
            System.currentTimeMillis(), System.currentTimeMillis(),
            0, 0, 0, 0,
            failureReason, failureHttpStatus
        );
    }

    @Nested
    @DisplayName("toJson 转义")
    class ToJsonEscape {

        @Test
        @DisplayName("包含 \\t \\b \\f 的失败原因应被正确转义")
        void escapesControlCharacters() {
            IntentTrace trace = createTrace("esc-1", "tab\there\b\f", null);
            String json = assertDoesNotThrow(trace::toJson);

            // Control characters should be escaped, not raw
            assertTrue(json.contains("\\t"), "Tab should be escaped as \\t");
            assertTrue(json.contains("\\b"), "Backspace should be escaped as \\b");
            assertTrue(json.contains("\\f"), "Form feed should be escaped as \\f");
        }

        @Test
        @DisplayName("包含 \" 和 \\ 的失败原因应被双转义")
        void escapesQuotesAndBackslashes() {
            IntentTrace trace = createTrace("esc-2", "key=\"value\" path\\nested", null);
            String json = assertDoesNotThrow(trace::toJson);

            assertTrue(json.contains("\\\""), "Quotes should be escaped");
            assertTrue(json.contains("\\\\"), "Backslashes should be escaped");
        }

        @Test
        @DisplayName("null 失败原因不应产生 failureReason 字段")
        void nullFailureReason() {
            IntentTrace trace = createTrace("esc-3", null, null);
            String json = assertDoesNotThrow(trace::toJson);

            assertTrue(json.contains("intentId"), "JSON should contain intentId");
            // failureReason key should not appear at all
            assertTrue(!json.contains("\"failureReason\""),
                "failureReason should not appear when null");
        }

        @Test
        @DisplayName("failureHttpStatus 应包含在 JSON 中")
        void failureHttpStatusIncluded() {
            IntentTrace trace = createTrace("esc-4", "timeout", 504);
            String json = assertDoesNotThrow(trace::toJson);

            assertTrue(json.contains("\"failureReason\":\"timeout\""));
            assertTrue(json.contains("\"failureHttpStatus\":504"));
        }

        @Test
        @DisplayName("普通文本不被错误转义")
        void normalTextUnchanged() {
            IntentTrace trace = createTrace("esc-5", "Connection refused", 502);
            String json = assertDoesNotThrow(trace::toJson);

            assertTrue(json.contains("Connection refused"));
            // Should be valid JSON structure
            assertTrue(json.startsWith("{"));
            assertTrue(json.endsWith("}"));
        }
    }

    @Nested
    @DisplayName("withFailure 工厂方法")
    class WithFailure {

        @Test
        @DisplayName("withFailure 应设置 failureReason 和 failureHttpStatus")
        void setsFailureFields() {
            IntentTrace trace = createTrace("wf-1", null, null);
            IntentTrace failed = trace.withFailure("Internal Server Error", 500);

            assertTrue(failed.toJson().contains("Internal Server Error"));
            assertTrue(failed.toJson().contains("500"));
        }
    }
}
