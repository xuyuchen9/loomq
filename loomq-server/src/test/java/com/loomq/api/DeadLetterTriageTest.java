package com.loomq.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Callback;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.tracing.IntentTrace;
import com.loomq.tracing.IntentTraceStore;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DeadLetterTriageTest {

    @Nested
    @DisplayName("DeathReport 构建")
    class DeathReportBuild {

        @Test
        @DisplayName("从 Intent + IntentTrace 构建完整 DeathReport")
        void fullDeathReport() {
            Intent intent = new Intent("intent_dead_1");
            intent.setExecuteAt(Instant.now().minusSeconds(3600));
            intent.setDeadline(Instant.now().minusSeconds(1800));
            intent.setPrecisionTier(PrecisionTier.HIGH);
            intent.setCallback(new Callback("https://myapp.com/hook"));
            intent.setRedelivery(new RedeliveryPolicy(3, "exponential", 1000, 60000, 2.0, false));

            intent.transitionTo(IntentStatus.SCHEDULED);
            intent.transitionTo(IntentStatus.DUE);
            intent.transitionTo(IntentStatus.DISPATCHING);

            IntentTraceStore.getInstance().recordCreated("intent_dead_1", "trace1", PrecisionTier.HIGH);
            IntentTraceStore.getInstance().recordFailure("intent_dead_1", "Connection refused", 503);
            IntentTraceStore.getInstance().updateStatus("intent_dead_1", IntentStatus.DEAD_LETTERED);

            for (int i = 0; i < 3; i++) {
                intent.incrementAttempts();
            }
            intent.transitionTo(IntentStatus.DEAD_LETTERED);

            // Simulate building via IntentHandler logic
            IntentTrace trace = IntentTraceStore.getInstance().get("intent_dead_1");
            assertNotNull(trace);
            assertEquals("Connection refused", trace.failureReason());
            assertEquals(503, trace.failureHttpStatus());

            int maxAttempts = intent.getRedelivery().getMaxAttempts();
            assertEquals(3, maxAttempts);
            assertEquals(3, intent.getAttempts());

            DeathReport report = new DeathReport(
                intent.getAttempts(),
                maxAttempts,
                intent.getUpdatedAt().toString(),
                trace.failureReason(),
                trace.failureHttpStatus(),
                intent.getCallback().getUrl(),
                intent.getPrecisionTier().name(),
                System.currentTimeMillis() - trace.createdAtMs()
            );

            assertEquals(3, report.finalAttempt());
            assertEquals(3, report.maxAttempts());
            assertEquals("Connection refused", report.failureReason());
            assertEquals(503, report.failureHttpStatus());
            assertEquals("https://myapp.com/hook", report.callbackUrl());
            assertEquals("HIGH", report.tier());
            assertTrue(report.lifetimeMs() >= 0);
        }
    }

    @Nested
    @DisplayName("IntentStatus DEAD_LETTERED → SCHEDULED revive 转换")
    class ReviveTransition {

        @Test
        @DisplayName("DEAD_LETTERED 应能转到 SCHEDULED")
        void deadLetteredToScheduled() {
            Intent intent = new Intent("intent_revive_1");
            intent.transitionTo(IntentStatus.SCHEDULED);
            intent.transitionTo(IntentStatus.DUE);
            intent.transitionTo(IntentStatus.DISPATCHING);
            intent.transitionTo(IntentStatus.DEAD_LETTERED);

            assertEquals(IntentStatus.DEAD_LETTERED, intent.getStatus());

            intent.transitionTo(IntentStatus.SCHEDULED);
            assertEquals(IntentStatus.SCHEDULED, intent.getStatus());
        }

        @Test
        @DisplayName("DEAD_LETTERED 不能转到 DUE")
        void deadLetteredCannotGoToDue() {
            Intent intent = new Intent("intent_revive_bad");
            intent.transitionTo(IntentStatus.SCHEDULED);
            intent.transitionTo(IntentStatus.DUE);
            intent.transitionTo(IntentStatus.DISPATCHING);
            intent.transitionTo(IntentStatus.DEAD_LETTERED);

            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class, () ->
                intent.transitionTo(IntentStatus.DUE));
        }
    }

    @Nested
    @DisplayName("IntentListResponse 构建")
    class IntentListResponseTest {

        @Test
        @DisplayName("正确构建带分页信息的列表响应")
        void buildListResponse() {
            Intent intent = new Intent("intent_list_1");
            intent.setExecuteAt(Instant.now().plusSeconds(60));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            intent.transitionTo(IntentStatus.SCHEDULED);

            IntentListResponse.IntentSummary summary = IntentListResponse.IntentSummary.from(intent, null);
            assertEquals("intent_list_1", summary.intentId());
            assertEquals("SCHEDULED", summary.status());
            assertNull(summary.deathReport());

            IntentListResponse response = IntentListResponse.of(List.of(summary), 100, 0, 50);
            assertEquals(1, response.intents().size());
            assertEquals(100, response.total());
            assertEquals(0, response.offset());
            assertEquals(50, response.limit());
        }

        @Test
        @DisplayName("DEAD_LETTERED intent 应包含 deathReport")
        void deadLetterWithReport() {
            Intent intent = new Intent("intent_dl_summary");
            intent.setPrecisionTier(PrecisionTier.ULTRA);
            intent.setCallback(new Callback("https://example.com/webhook"));
            intent.incrementAttempts();
            intent.incrementAttempts();
            intent.transitionTo(IntentStatus.SCHEDULED);
            intent.transitionTo(IntentStatus.DUE);
            intent.transitionTo(IntentStatus.DISPATCHING);
            intent.transitionTo(IntentStatus.DEAD_LETTERED);

            DeathReport report = new DeathReport(2, 5, Instant.now().toString(),
                "HTTP 503", 503, "https://example.com/webhook", "ULTRA", 30000L);

            IntentListResponse.IntentSummary summary = IntentListResponse.IntentSummary.from(intent, report);
            assertNotNull(summary.deathReport());
            assertEquals("HTTP 503", summary.deathReport().failureReason());
            assertEquals(503, summary.deathReport().failureHttpStatus());
            assertEquals("ULTRA", summary.deathReport().tier());
        }
    }

    @Nested
    @DisplayName("IntentTrace failure 扩展")
    class IntentTraceFailure {

        @BeforeEach
        void clearTraces() {
            IntentTraceStore.getInstance().remove("trace_fail_1");
            IntentTraceStore.getInstance().remove("trace_fail_2");
            IntentTraceStore.getInstance().remove("trace_fail_3");
        }

        @Test
        @DisplayName("withFailure 应正确记录失败原因和 HTTP 状态码")
        void withFailure() {
            IntentTraceStore.getInstance().recordCreated("trace_fail_1", "t1", PrecisionTier.FAST);
            IntentTraceStore.getInstance().recordFailure("trace_fail_1", "Connection timeout", null);

            IntentTrace trace = IntentTraceStore.getInstance().get("trace_fail_1");
            assertNotNull(trace);
            assertEquals("Connection timeout", trace.failureReason());
            assertNull(trace.failureHttpStatus());
        }

        @Test
        @DisplayName("withFailure 带 HTTP 状态码")
        void withFailureAndHttpStatus() {
            IntentTraceStore.getInstance().recordCreated("trace_fail_2", "t2", PrecisionTier.HIGH);
            IntentTraceStore.getInstance().recordFailure("trace_fail_2", "Server Error", 500);

            IntentTrace trace = IntentTraceStore.getInstance().get("trace_fail_2");
            assertNotNull(trace);
            assertEquals("Server Error", trace.failureReason());
            assertEquals(500, trace.failureHttpStatus());
        }

        @Test
        @DisplayName("toJson 应包含 failure 字段")
        void toJsonWithFailure() {
            IntentTraceStore.getInstance().recordCreated("trace_fail_3", "t3", PrecisionTier.STANDARD);
            IntentTraceStore.getInstance().recordFailure("trace_fail_3", "Bad Gateway", 502);

            IntentTrace trace = IntentTraceStore.getInstance().get("trace_fail_3");
            String json = trace.toJson();
            assertTrue(json.contains("\"failureReason\":\"Bad Gateway\""));
            assertTrue(json.contains("\"failureHttpStatus\":502"));
        }
    }

    @Nested
    @DisplayName("ErrorRecoveryAdvisor 新错误码")
    class NewErrorCodes {

        @Test
        @DisplayName("42207 不能 revive 非 DEAD_LETTERED 状态")
        void cannotRevive() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("42207",
                Map.of("currentState", IntentStatus.ACKED));
            assertNotNull(hint);
            assertTrue(hint.suggestion().contains("Only DEAD_LETTERED"));
        }

        @Test
        @DisplayName("40002 缺少 status 参数")
        void missingStatus() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("40002", null);
            assertNotNull(hint);
            assertTrue(hint.suggestion().contains("'status' query parameter is required"));
        }

        @Test
        @DisplayName("40003 无效 status 值")
        void invalidStatus() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("40003", null);
            assertNotNull(hint);
            assertTrue(hint.suggestion().contains("not recognized"));
        }
    }
}
