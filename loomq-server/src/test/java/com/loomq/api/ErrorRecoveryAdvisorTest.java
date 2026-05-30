package com.loomq.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ErrorRecoveryAdvisorTest {

    @Nested
    @DisplayName("Cannot Cancel (42204)")
    class CannotCancel {

        @Test
        @DisplayName("DISPATCHING 状态应建议等待投递完成")
        void dispatching() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("42204",
                Map.of("currentState", IntentStatus.DISPATCHING, "precisionTier", PrecisionTier.FAST));
            assertNotNull(hint);
            assertTrue(hint.temporal());
            assertTrue(hint.suggestion().contains("currently being delivered"));
            assertEquals("DISPATCHING", hint.currentState());
            assertTrue(hint.transitionsTo().contains("DELIVERED"));
            assertNotNull(hint.estimatedWaitMs());
        }

        @Test
        @DisplayName("DELIVERED 状态应说明取消不可用")
        void delivered() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("42204",
                Map.of("currentState", IntentStatus.DELIVERED));
            assertNotNull(hint);
            assertTrue(hint.temporal());
            assertTrue(hint.suggestion().contains("awaiting ACK"));
        }

        @Test
        @DisplayName("终态应说明不可取消")
        void terminal() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("42204",
                Map.of("currentState", IntentStatus.ACKED));
            assertNotNull(hint);
            assertFalse(hint.temporal());
            assertTrue(hint.suggestion().contains("terminal state"));
        }
    }

    @Nested
    @DisplayName("Cannot Modify (42205)")
    class CannotModify {

        @Test
        @DisplayName("DISPATCHING 状态应建议等待后修改")
        void dispatching() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("42205",
                Map.of("currentState", IntentStatus.DISPATCHING, "precisionTier", PrecisionTier.ULTRA));
            assertNotNull(hint);
            assertTrue(hint.temporal());
            assertTrue(hint.suggestion().contains("currently being delivered"));
            assertNotNull(hint.estimatedWaitMs());
        }

        @Test
        @DisplayName("ULTRA tier 等待时间应短于 STANDARD")
        void ultraTierWaitShorter() {
            RecoveryHint ultra = ErrorRecoveryAdvisor.advise("42205",
                Map.of("currentState", IntentStatus.DISPATCHING, "precisionTier", PrecisionTier.ULTRA));
            RecoveryHint standard = ErrorRecoveryAdvisor.advise("42205",
                Map.of("currentState", IntentStatus.DISPATCHING, "precisionTier", PrecisionTier.STANDARD));
            assertNotNull(ultra);
            assertNotNull(standard);
            assertTrue(ultra.estimatedWaitMs() < standard.estimatedWaitMs());
        }
    }

    @Nested
    @DisplayName("Backpressure (42900)")
    class Backpressure {

        @Test
        @DisplayName("应包含 retryAfterMs 并标记为时态")
        void withRetryAfterMs() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("42900",
                Map.of("retryAfterMs", 2000L));
            assertNotNull(hint);
            assertTrue(hint.temporal());
            assertEquals(2000L, hint.estimatedWaitMs());
        }

        @Test
        @DisplayName("无 retryAfterMs 时应使用默认值")
        void withoutRetryAfterMs() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("42900", null);
            assertNotNull(hint);
            assertTrue(hint.temporal());
            assertEquals(1000L, hint.estimatedWaitMs());
        }
    }

    @Nested
    @DisplayName("Not Found (40401)")
    class NotFound {

        @Test
        @DisplayName("应建议 intent 可能已被 GC")
        void shouldSuggestGc() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("40401", null);
            assertNotNull(hint);
            assertFalse(hint.temporal());
            assertTrue(hint.suggestion().contains("terminal state"));
        }
    }

    @Nested
    @DisplayName("Raft Errors")
    class RaftErrors {

        @Test
        @DisplayName("50301 读重定向应包含 leaderId")
        void readRedirect() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("50301",
                Map.of("leaderId", "node-2"));
            assertNotNull(hint);
            assertFalse(hint.temporal());
            assertEquals("node-2", hint.leaderId());
            assertTrue(hint.suggestion().contains("node-2"));
        }

        @Test
        @DisplayName("50302 写重定向应包含 leaderId")
        void writeRedirect() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("50302",
                Map.of("leaderId", "node-3"));
            assertNotNull(hint);
            assertEquals("node-3", hint.leaderId());
        }

        @Test
        @DisplayName("50303 Raft 背压应为时态")
        void backpressure() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("50303",
                Map.of("retryAfterMs", 3000L));
            assertNotNull(hint);
            assertTrue(hint.temporal());
            assertEquals(3000L, hint.estimatedWaitMs());
        }
    }

    @Nested
    @DisplayName("Revision Conflict (40902)")
    class RevisionConflict {

        @Test
        @DisplayName("应建议重新获取最新 revision")
        void shouldSuggestReFetch() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("40902", null);
            assertNotNull(hint);
            assertFalse(hint.temporal());
            assertTrue(hint.suggestion().contains("Re-fetch"));
        }
    }

    @Nested
    @DisplayName("Idempotency Conflict (40901)")
    class IdempotencyConflict {

        @Test
        @DisplayName("应建议使用不同的 key")
        void shouldSuggestDifferentKey() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("40901",
                Map.of("intentId", "intent_abc"));
            assertNotNull(hint);
            assertFalse(hint.temporal());
            assertTrue(hint.suggestion().contains("different key"));
            assertEquals("GET /v1/intents/intent_abc", hint.relevantEndpoint());
        }
    }

    @Nested
    @DisplayName("Unknown Error Code")
    class UnknownCode {

        @Test
        @DisplayName("未知错误码应返回 null")
        void unknownCodeReturnsNull() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise("99999", null);
            assertNull(hint);
        }

        @Test
        @DisplayName("null 错误码应返回 null")
        void nullCodeReturnsNull() {
            RecoveryHint hint = ErrorRecoveryAdvisor.advise(null, null);
            assertNull(hint);
        }
    }

    @Nested
    @DisplayName("RecoveryHint 工厂方法")
    class RecoveryHintFactory {

        @Test
        @DisplayName("temporal 工厂方法")
        void temporalFactory() {
            RecoveryHint hint = RecoveryHint.temporal("wait", "GET /x", 500L, "DUE", List.of("DISPATCHING"));
            assertTrue(hint.temporal());
            assertEquals("wait", hint.suggestion());
            assertEquals(500L, hint.estimatedWaitMs());
            assertEquals("DUE", hint.currentState());
            assertEquals(List.of("DISPATCHING"), hint.transitionsTo());
            assertNull(hint.leaderId());
        }

        @Test
        @DisplayName("nonTemporal 工厂方法")
        void nonTemporalFactory() {
            RecoveryHint hint = RecoveryHint.nonTemporal("fix it", "POST /x");
            assertFalse(hint.temporal());
            assertNull(hint.estimatedWaitMs());
            assertNull(hint.currentState());
            assertNull(hint.transitionsTo());
        }

        @Test
        @DisplayName("raftRedirect 工厂方法")
        void raftRedirectFactory() {
            RecoveryHint hint = RecoveryHint.raftRedirect("go to leader", "node-1");
            assertFalse(hint.temporal());
            assertEquals("node-1", hint.leaderId());
            assertEquals("go to leader", hint.suggestion());
        }
    }
}
