package com.loomq.http.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loomq.api.ErrorResponse;
import com.loomq.api.RecoveryHint;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class HttpErrorResponseSerializerRecoveryTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Nested
    @DisplayName("序列化带 RecoveryHint 的错误响应")
    class WithRecovery {

        @Test
        @DisplayName("完整 recovery 字段应正确序列化")
        void fullRecovery() throws Exception {
            RecoveryHint recovery = RecoveryHint.temporal(
                "Wait for delivery to complete",
                "GET /v1/intents/{id}",
                5000L,
                "DISPATCHING",
                List.of("DELIVERED", "DEAD_LETTERED")
            );
            ErrorResponse error = ErrorResponse.of("42205", "Cannot modify", Map.of("key", "val"), recovery);
            HttpErrorResponse response = new HttpErrorResponse(422, error);

            byte[] bytes = HttpErrorResponseSerializer.toBytes(response);
            String json = new String(bytes, StandardCharsets.UTF_8);

            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = objectMapper.readValue(json, Map.class);

            assertEquals(422, parsed.get("status"));
            @SuppressWarnings("unchecked")
            Map<String, Object> errorObj = (Map<String, Object>) parsed.get("error");
            assertEquals("42205", errorObj.get("code"));
            assertEquals("Cannot modify", errorObj.get("message"));

            @SuppressWarnings("unchecked")
            Map<String, Object> recoveryObj = (Map<String, Object>) errorObj.get("recovery");
            assertEquals("Wait for delivery to complete", recoveryObj.get("suggestion"));
            assertEquals("GET /v1/intents/{id}", recoveryObj.get("relevantEndpoint"));
            assertEquals(5000, recoveryObj.get("estimatedWaitMs"));
            assertEquals("DISPATCHING", recoveryObj.get("currentState"));
            assertTrue((Boolean) recoveryObj.get("temporal"));

            @SuppressWarnings("unchecked")
            List<String> transitions = (List<String>) recoveryObj.get("transitionsTo");
            assertEquals(2, transitions.size());
            assertTrue(transitions.contains("DELIVERED"));
            assertTrue(transitions.contains("DEAD_LETTERED"));
        }

        @Test
        @DisplayName("Raft 重定向 recovery 应包含 leaderId")
        void raftRedirectRecovery() throws Exception {
            RecoveryHint recovery = RecoveryHint.raftRedirect("Route to leader", "node-2");
            ErrorResponse error = ErrorResponse.of("50302", "Not leader", null, recovery);
            HttpErrorResponse response = new HttpErrorResponse(503, error);

            byte[] bytes = HttpErrorResponseSerializer.toBytes(response);
            String json = new String(bytes, StandardCharsets.UTF_8);

            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = objectMapper.readValue(json, Map.class);
            @SuppressWarnings("unchecked")
            Map<String, Object> errorObj = (Map<String, Object>) parsed.get("error");
            @SuppressWarnings("unchecked")
            Map<String, Object> recoveryObj = (Map<String, Object>) errorObj.get("recovery");

            assertEquals("node-2", recoveryObj.get("leaderId"));
            assertFalse((Boolean) recoveryObj.get("temporal"));
        }
    }

    @Nested
    @DisplayName("无 RecoveryHint 的错误响应")
    class WithoutRecovery {

        @Test
        @DisplayName("recovery 为 null 时应输出 null")
        void nullRecovery() throws Exception {
            ErrorResponse error = ErrorResponse.of("50000", "Internal error");
            HttpErrorResponse response = new HttpErrorResponse(500, error);

            byte[] bytes = HttpErrorResponseSerializer.toBytes(response);
            String json = new String(bytes, StandardCharsets.UTF_8);

            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = objectMapper.readValue(json, Map.class);
            @SuppressWarnings("unchecked")
            Map<String, Object> errorObj = (Map<String, Object>) parsed.get("error");

            assertEquals(null, errorObj.get("recovery"));
        }
    }

    @Nested
    @DisplayName("向后兼容性")
    class BackwardCompatibility {

        @Test
        @DisplayName("旧版 ErrorResponse.of(code, message) 仍能正常工作")
        void legacyOfMethod() throws Exception {
            ErrorResponse error = ErrorResponse.of("40401", "Not found");
            HttpErrorResponse response = new HttpErrorResponse(404, error);

            byte[] bytes = HttpErrorResponseSerializer.toBytes(response);
            String json = new String(bytes, StandardCharsets.UTF_8);

            assertTrue(json.contains("\"code\":\"40401\""));
            assertTrue(json.contains("\"message\":\"Not found\""));
            assertTrue(json.contains("\"recovery\":null"));
        }

        @Test
        @DisplayName("旧版 ErrorResponse.of(code, message, details) 仍能正常工作")
        void legacyOfWithDetailsMethod() throws Exception {
            ErrorResponse error = ErrorResponse.of("42900", "Backpressure",
                Map.of("retryAfterMs", 1000));
            HttpErrorResponse response = new HttpErrorResponse(429, error);

            byte[] bytes = HttpErrorResponseSerializer.toBytes(response);
            String json = new String(bytes, StandardCharsets.UTF_8);

            assertTrue(json.contains("\"code\":\"42900\""));
            assertTrue(json.contains("\"recovery\":null"));
        }
    }

    @Nested
    @DisplayName("estimateSize 和 writeTo")
    class SizeAndWrite {

        @Test
        @DisplayName("estimateSize 应大于实际输出")
        void estimateSizeCoversActual() {
            RecoveryHint recovery = RecoveryHint.temporal(
                "Wait", "GET /v1/intents/{id}", 500L, "DUE", List.of("DISPATCHING"));
            ErrorResponse error = ErrorResponse.of("42204", "Cannot cancel", null, recovery);
            HttpErrorResponse response = new HttpErrorResponse(422, error);

            int estimated = HttpErrorResponseSerializer.estimateSize(response);
            byte[] actual = HttpErrorResponseSerializer.toBytes(response);

            assertTrue(estimated >= actual.length,
                "estimateSize=" + estimated + " should be >= actual=" + actual.length);
        }

        @Test
        @DisplayName("writeTo 应写入 ByteBuf")
        void writeToByteBuf() {
            RecoveryHint recovery = RecoveryHint.nonTemporal("Check logs", null);
            ErrorResponse error = ErrorResponse.of("50002", "Cancel failed", null, recovery);
            HttpErrorResponse response = new HttpErrorResponse(500, error);

            var buf = Unpooled.buffer(256);
            try {
                response.writeTo(buf);
                assertTrue(buf.readableBytes() > 0);
                String json = buf.toString(StandardCharsets.UTF_8);
                assertTrue(json.contains("\"recovery\""));
            } finally {
                buf.release();
            }
        }
    }
}
