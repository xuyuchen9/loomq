package com.loomq.recovery.v2;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Recovery V2 单元测试
 */
class RecoveryV2Test {

    @Test
    @DisplayName("RecoveryResult 应正确创建")
    void recoveryResult() {
        RecoveryServiceV2.RecoveryResult result = new RecoveryServiceV2.RecoveryResult(
                1000, 100, 5, 3, 10, 500
        );

        assertEquals(1000, result.walRecords());
        assertEquals(100, result.recoveredTasks());
        assertEquals(5, result.inflightTasks());
        assertEquals(3, result.redispatchedTasks());
        assertEquals(10, result.skippedRecords());
        assertEquals(500, result.elapsedMs());

        String str = result.toString();
        assertTrue(str.contains("wal=1000"));
        assertTrue(str.contains("recovered=100"));
        assertTrue(str.contains("redispatched=3"));
    }

    @Test
    @DisplayName("空 RecoveryResult 应正确")
    void emptyRecoveryResult() {
        RecoveryServiceV2.RecoveryResult result = RecoveryServiceV2.RecoveryResult.empty();

        assertEquals(0, result.walRecords());
        assertEquals(0, result.recoveredTasks());
        assertEquals(0, result.inflightTasks());
        assertEquals(0, result.redispatchedTasks());
    }

    @Test
    @DisplayName("RecoveryConfig 默认配置应有效")
    void defaultConfig() {
        RecoveryServiceV2.RecoveryConfig config = RecoveryServiceV2.RecoveryConfig.defaultConfig();

        assertEquals(1000, config.batchSize());
        assertEquals(10, config.sleepMs());
        assertEquals(100, config.concurrencyLimit());
        assertFalse(config.safeMode());
    }

    @Test
    @DisplayName("RecoveryConfig 自定义配置应正确")
    void customConfig() {
        RecoveryServiceV2.RecoveryConfig config = new RecoveryServiceV2.RecoveryConfig(
                500, 5, 50, true
        );

        assertEquals(500, config.batchSize());
        assertEquals(5, config.sleepMs());
        assertEquals(50, config.concurrencyLimit());
        assertTrue(config.safeMode());
    }
}
