package com.loomq.entity.v3;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 任务状态机 V3 测试
 *
 * 验证所有合法和非法的状态转换。
 *
 * @author loomq
 * @since v0.4
 */
class TaskStatusV3Test {

    // ========== 终态判断测试 ==========

    @Test
    @DisplayName("终态判断")
    void testIsTerminal() {
        // 非终态
        assertFalse(TaskStatusV3.PENDING.isTerminal());
        assertFalse(TaskStatusV3.SCHEDULED.isTerminal());
        assertFalse(TaskStatusV3.READY.isTerminal());
        assertFalse(TaskStatusV3.RUNNING.isTerminal());
        assertFalse(TaskStatusV3.RETRY_WAIT.isTerminal());

        // 终态
        assertTrue(TaskStatusV3.SUCCESS.isTerminal());
        assertTrue(TaskStatusV3.FAILED.isTerminal());
        assertTrue(TaskStatusV3.CANCELLED.isTerminal());
        assertTrue(TaskStatusV3.EXPIRED.isTerminal());
        assertTrue(TaskStatusV3.DEAD_LETTER.isTerminal());
    }

    // ========== 可取消判断测试 ==========

    @Test
    @DisplayName("可取消状态判断")
    void testIsCancellable() {
        // 可取消
        assertTrue(TaskStatusV3.PENDING.isCancellable());
        assertTrue(TaskStatusV3.SCHEDULED.isCancellable());
        assertTrue(TaskStatusV3.READY.isCancellable());
        assertTrue(TaskStatusV3.RETRY_WAIT.isCancellable());

        // 不可取消
        assertFalse(TaskStatusV3.RUNNING.isCancellable());
        assertFalse(TaskStatusV3.SUCCESS.isCancellable());
        assertFalse(TaskStatusV3.FAILED.isCancellable());
        assertFalse(TaskStatusV3.CANCELLED.isCancellable());
        assertFalse(TaskStatusV3.EXPIRED.isCancellable());
        assertFalse(TaskStatusV3.DEAD_LETTER.isCancellable());
    }

    // ========== 状态转换合法性测试 ==========

    @Test
    @DisplayName("PENDING 状态转换")
    void testPendingTransitions() {
        // 合法转换
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.PENDING, TaskStatusV3.SCHEDULED));
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.PENDING, TaskStatusV3.CANCELLED));

        // 非法转换
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.PENDING, TaskStatusV3.READY));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.PENDING, TaskStatusV3.RUNNING));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.PENDING, TaskStatusV3.SUCCESS));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.PENDING, TaskStatusV3.FAILED));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.PENDING, TaskStatusV3.EXPIRED));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.PENDING, TaskStatusV3.DEAD_LETTER));
    }

    @Test
    @DisplayName("SCHEDULED 状态转换")
    void testScheduledTransitions() {
        // 合法转换
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.SCHEDULED, TaskStatusV3.READY));
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.SCHEDULED, TaskStatusV3.CANCELLED));
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.SCHEDULED, TaskStatusV3.EXPIRED));

        // 非法转换
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.SCHEDULED, TaskStatusV3.PENDING));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.SCHEDULED, TaskStatusV3.RUNNING));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.SCHEDULED, TaskStatusV3.SUCCESS));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.SCHEDULED, TaskStatusV3.FAILED));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.SCHEDULED, TaskStatusV3.RETRY_WAIT));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.SCHEDULED, TaskStatusV3.DEAD_LETTER));
    }

    @Test
    @DisplayName("READY 状态转换")
    void testReadyTransitions() {
        // 合法转换
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.READY, TaskStatusV3.RUNNING));
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.READY, TaskStatusV3.CANCELLED));

        // 非法转换
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.READY, TaskStatusV3.PENDING));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.READY, TaskStatusV3.SCHEDULED));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.READY, TaskStatusV3.SUCCESS));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.READY, TaskStatusV3.FAILED));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.READY, TaskStatusV3.EXPIRED));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.READY, TaskStatusV3.RETRY_WAIT));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.READY, TaskStatusV3.DEAD_LETTER));
    }

    @Test
    @DisplayName("RUNNING 状态转换")
    void testRunningTransitions() {
        // 合法转换
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.RUNNING, TaskStatusV3.SUCCESS));
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.RUNNING, TaskStatusV3.RETRY_WAIT));
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.RUNNING, TaskStatusV3.FAILED));
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.RUNNING, TaskStatusV3.DEAD_LETTER));

        // 非法转换
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.RUNNING, TaskStatusV3.PENDING));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.RUNNING, TaskStatusV3.SCHEDULED));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.RUNNING, TaskStatusV3.READY));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.RUNNING, TaskStatusV3.CANCELLED));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.RUNNING, TaskStatusV3.EXPIRED));
    }

    @Test
    @DisplayName("RETRY_WAIT 状态转换")
    void testRetryWaitTransitions() {
        // 合法转换
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.RETRY_WAIT, TaskStatusV3.SCHEDULED));
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.RETRY_WAIT, TaskStatusV3.FAILED));
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.RETRY_WAIT, TaskStatusV3.DEAD_LETTER));
        assertTrue(TaskLifecycleV3.isValidTransition(TaskStatusV3.RETRY_WAIT, TaskStatusV3.CANCELLED));

        // 非法转换
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.RETRY_WAIT, TaskStatusV3.PENDING));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.RETRY_WAIT, TaskStatusV3.READY));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.RETRY_WAIT, TaskStatusV3.RUNNING));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.RETRY_WAIT, TaskStatusV3.SUCCESS));
        assertFalse(TaskLifecycleV3.isValidTransition(TaskStatusV3.RETRY_WAIT, TaskStatusV3.EXPIRED));
    }

    @Test
    @DisplayName("终态不允许转换")
    void testTerminalTransitions() {
        TaskStatusV3[] terminalStates = {
            TaskStatusV3.SUCCESS,
            TaskStatusV3.FAILED,
            TaskStatusV3.CANCELLED,
            TaskStatusV3.EXPIRED,
            TaskStatusV3.DEAD_LETTER
        };

        for (TaskStatusV3 terminal : terminalStates) {
            for (TaskStatusV3 target : TaskStatusV3.values()) {
                assertFalse(TaskLifecycleV3.isValidTransition(terminal, target),
                        "Terminal state " + terminal + " should not transition to " + target);
            }
        }
    }

    // ========== 生命周期管理器测试 ==========

    @Test
    @DisplayName("生命周期：正常流转")
    void testLifecycleNormalFlow() {
        TaskLifecycleV3 lifecycle = new TaskLifecycleV3("test-task-1");

        assertEquals(TaskStatusV3.PENDING, lifecycle.getStatus());

        // PENDING -> SCHEDULED
        assertTrue(lifecycle.transitionToScheduled());
        assertEquals(TaskStatusV3.SCHEDULED, lifecycle.getStatus());

        // SCHEDULED -> READY
        assertTrue(lifecycle.transitionToReady());
        assertEquals(TaskStatusV3.READY, lifecycle.getStatus());

        // READY -> RUNNING
        assertTrue(lifecycle.transitionToRunning());
        assertEquals(TaskStatusV3.RUNNING, lifecycle.getStatus());

        // RUNNING -> SUCCESS
        assertTrue(lifecycle.transitionToSuccess());
        assertEquals(TaskStatusV3.SUCCESS, lifecycle.getStatus());
        assertTrue(lifecycle.getStatus().isTerminal());
    }

    @Test
    @DisplayName("生命周期：重试流程")
    void testLifecycleRetryFlow() {
        TaskLifecycleV3 lifecycle = new TaskLifecycleV3("test-task-2");

        // 准备到 RUNNING
        lifecycle.transitionToScheduled();
        lifecycle.transitionToReady();
        lifecycle.transitionToRunning();

        // RUNNING -> RETRY_WAIT
        assertTrue(lifecycle.transitionToRetryWait(3));
        assertEquals(TaskStatusV3.RETRY_WAIT, lifecycle.getStatus());

        // RETRY_WAIT -> SCHEDULED (重新调度)
        assertTrue(lifecycle.transitionToScheduled());
        assertEquals(TaskStatusV3.SCHEDULED, lifecycle.getStatus());
    }

    @Test
    @DisplayName("生命周期：重试耗尽进入死信")
    void testLifecycleDeadLetter() {
        TaskLifecycleV3 lifecycle = new TaskLifecycleV3("test-task-3");

        // 准备到 RUNNING
        lifecycle.transitionToScheduled();
        lifecycle.transitionToReady();
        lifecycle.transitionToRunning();

        // 第 1 次执行失败 -> RETRY_WAIT
        assertTrue(lifecycle.transitionToRetryWait(3));
        assertEquals(TaskStatusV3.RETRY_WAIT, lifecycle.getStatus());
        assertEquals(1, lifecycle.getRetryCount());

        // 重新调度
        lifecycle.transitionToScheduled();
        lifecycle.transitionToReady();
        lifecycle.transitionToRunning();

        // 第 2 次执行失败 -> RETRY_WAIT
        assertTrue(lifecycle.transitionToRetryWait(3));
        assertEquals(TaskStatusV3.RETRY_WAIT, lifecycle.getStatus());
        assertEquals(2, lifecycle.getRetryCount());

        // 重新调度
        lifecycle.transitionToScheduled();
        lifecycle.transitionToReady();
        lifecycle.transitionToRunning();

        // 第 3 次执行失败 -> DEAD_LETTER (retryCount >= maxRetry)
        assertTrue(lifecycle.transitionToRetryWait(3));
        assertEquals(TaskStatusV3.DEAD_LETTER, lifecycle.getStatus());
        assertTrue(lifecycle.getStatus().isTerminal());
    }

    @Test
    @DisplayName("生命周期：取消流程")
    void testLifecycleCancel() {
        // PENDING 阶段取消
        TaskLifecycleV3 lifecycle1 = new TaskLifecycleV3("test-task-cancel-1");
        assertTrue(lifecycle1.cancel());
        assertEquals(TaskStatusV3.CANCELLED, lifecycle1.getStatus());

        // SCHEDULED 阶段取消
        TaskLifecycleV3 lifecycle2 = new TaskLifecycleV3("test-task-cancel-2");
        lifecycle2.transitionToScheduled();
        assertTrue(lifecycle2.cancel());
        assertEquals(TaskStatusV3.CANCELLED, lifecycle2.getStatus());

        // READY 阶段取消
        TaskLifecycleV3 lifecycle3 = new TaskLifecycleV3("test-task-cancel-3");
        lifecycle3.transitionToScheduled();
        lifecycle3.transitionToReady();
        assertTrue(lifecycle3.cancel());
        assertEquals(TaskStatusV3.CANCELLED, lifecycle3.getStatus());

        // RUNNING 阶段不能取消
        TaskLifecycleV3 lifecycle4 = new TaskLifecycleV3("test-task-cancel-4");
        lifecycle4.transitionToScheduled();
        lifecycle4.transitionToReady();
        lifecycle4.transitionToRunning();
        assertFalse(lifecycle4.cancel());
        assertEquals(TaskStatusV3.RUNNING, lifecycle4.getStatus());
    }

    @Test
    @DisplayName("生命周期：非法转换被拒绝")
    void testLifecycleInvalidTransition() {
        TaskLifecycleV3 lifecycle = new TaskLifecycleV3("test-task-invalid");

        // PENDING 不能直接到 READY
        assertFalse(lifecycle.transitionToReady());
        assertEquals(TaskStatusV3.PENDING, lifecycle.getStatus());

        // PENDING 不能直接到 RUNNING
        assertFalse(lifecycle.transitionToRunning());
        assertEquals(TaskStatusV3.PENDING, lifecycle.getStatus());

        // PENDING 不能直接到 SUCCESS
        assertFalse(lifecycle.transitionToSuccess());
        assertEquals(TaskStatusV3.PENDING, lifecycle.getStatus());
    }
}
