package com.loomq.wal.v2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CheckpointManager 单元测试
 */
class CheckpointManagerTest {

    @TempDir
    Path tempDir;

    private CheckpointManager checkpointManager;

    @BeforeEach
    void setUp() {
        checkpointManager = new CheckpointManager(tempDir, 100); // 100 records interval for testing
    }

    @AfterEach
    void tearDown() {
        if (checkpointManager != null) {
            checkpointManager.clear();
        }
    }

    @Test
    void testNoInitialCheckpoint() {
        assertFalse(checkpointManager.hasValidCheckpoint());
        assertNull(checkpointManager.getCurrentCheckpoint());
    }

    @Test
    void testCheckpointCreation() {
        checkpointManager.checkpoint(1000L, 1, 4096L, 500L);

        assertTrue(checkpointManager.hasValidCheckpoint());

        CheckpointManager.Checkpoint cp = checkpointManager.getCurrentCheckpoint();
        assertNotNull(cp);
        assertEquals(1000L, cp.lastRecordSeq());
        assertEquals(1, cp.segmentSeq());
        assertEquals(4096L, cp.segmentPosition());
        assertEquals(500L, cp.taskCount());
        assertTrue(cp.timestamp() > 0);
    }

    @Test
    void testCheckpointPersistence() {
        // 创建 checkpoint
        checkpointManager.checkpoint(5000L, 3, 8192L, 1000L);

        // 创建新的 CheckpointManager 加载持久化的数据
        CheckpointManager newManager = new CheckpointManager(tempDir, 100);

        assertTrue(newManager.hasValidCheckpoint());

        CheckpointManager.Checkpoint cp = newManager.getCurrentCheckpoint();
        assertNotNull(cp);
        assertEquals(5000L, cp.lastRecordSeq());
        assertEquals(3, cp.segmentSeq());
        assertEquals(8192L, cp.segmentPosition());
        assertEquals(1000L, cp.taskCount());
    }

    @Test
    void testRecoveryStartPosition() {
        // 无 checkpoint 时，从起点开始
        CheckpointManager.RecoveryPosition pos1 = checkpointManager.getRecoveryStartPosition();
        assertEquals(0, pos1.segmentSeq());
        assertEquals(0, pos1.segmentPosition());
        assertEquals(0, pos1.lastRecordSeq());

        // 有 checkpoint 时
        checkpointManager.checkpoint(2000L, 5, 16384L, 200L);

        CheckpointManager.RecoveryPosition pos2 = checkpointManager.getRecoveryStartPosition();
        assertEquals(5, pos2.segmentSeq());
        assertEquals(16384L, pos2.segmentPosition());
        assertEquals(2000L, pos2.lastRecordSeq());
    }

    @Test
    void testRecordWriteTrigger() {
        // 设置间隔为 10
        CheckpointManager smallIntervalManager = new CheckpointManager(tempDir, 10);

        // 前 9 次不应触发
        for (int i = 0; i < 9; i++) {
            assertFalse(smallIntervalManager.recordWrite());
        }

        // 第 10 次应触发
        assertTrue(smallIntervalManager.recordWrite());
    }

    @Test
    void testRecordWriteResetAfterCheckpoint() {
        CheckpointManager smallIntervalManager = new CheckpointManager(tempDir, 5);

        // 触发后执行 checkpoint
        for (int i = 0; i < 5; i++) {
            smallIntervalManager.recordWrite();
        }
        assertTrue(smallIntervalManager.recordWrite());

        // 执行 checkpoint
        smallIntervalManager.checkpoint(100L, 0, 0L, 10L);

        // 计数器应重置
        for (int i = 0; i < 4; i++) {
            assertFalse(smallIntervalManager.recordWrite());
        }
        assertTrue(smallIntervalManager.recordWrite());
    }

    @Test
    void testClear() {
        checkpointManager.checkpoint(1000L, 1, 4096L, 500L);
        assertTrue(checkpointManager.hasValidCheckpoint());

        checkpointManager.clear();

        assertFalse(checkpointManager.hasValidCheckpoint());
        assertNull(checkpointManager.getCurrentCheckpoint());
    }

    @Test
    void testCheckpointUpdate() {
        // 第一次 checkpoint
        checkpointManager.checkpoint(100L, 0, 1024L, 10L);

        // 第二次 checkpoint（更新）
        checkpointManager.checkpoint(200L, 1, 2048L, 20L);

        CheckpointManager.Checkpoint cp = checkpointManager.getCurrentCheckpoint();
        assertEquals(200L, cp.lastRecordSeq());
        assertEquals(1, cp.segmentSeq());
        assertEquals(2048L, cp.segmentPosition());
        assertEquals(20L, cp.taskCount());
    }
}
