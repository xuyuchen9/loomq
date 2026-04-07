package com.loomq.recovery.v3;

import com.loomq.entity.v3.EventTypeV3;
import com.loomq.entity.v3.TaskStatusV3;
import com.loomq.entity.v3.TaskV3;
import com.loomq.store.v3.TaskStoreV3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 恢复服务 V3
 *
 * 基于 V3 状态机的恢复逻辑。
 *
 * 恢复策略：
 * | 状态        | 处理                     |
 * |-------------|--------------------------|
 * | PENDING     | 重新调度                 |
 * | SCHEDULED   | 放入调度器               |
 * | READY       | 立即执行                 |
 * | RUNNING     | 重新执行（关键改进）     |
 * | RETRY_WAIT  | 重新调度                 |
 * | 终态        | 忽略                     |
 *
 * @author loomq
 * @since v0.4
 */
public class RecoveryServiceV3 {

    private static final Logger logger = LoggerFactory.getLogger(RecoveryServiceV3.class);

    private final TaskStoreV3 taskStore;
    private final RecoveryConfig config;

    // 任务状态机（临时）
    private final Map<String, TaskRecoveryState> taskStates;

    // 统计
    private final Stats stats = new Stats();

    public RecoveryServiceV3(TaskStoreV3 taskStore, RecoveryConfig config) {
        this.taskStore = taskStore;
        this.config = config;
        this.taskStates = new ConcurrentHashMap<>();
    }

    /**
     * 从 WAL 记录恢复
     *
     * @param records WAL 记录列表
     * @return 恢复结果
     */
    public RecoveryResult recoverFromRecords(List<WalRecordV3> records) {
        long startTime = System.currentTimeMillis();

        logger.info("Starting recovery from {} WAL records...", records.size());

        if (records.isEmpty()) {
            logger.info("No WAL records found, recovery skipped");
            return RecoveryResult.empty();
        }

        // 1. 重放 WAL，构建任务状态
        for (WalRecordV3 record : records) {
            processRecord(record);
            stats.walRecords++;
        }

        // 2. 重建任务存储
        rebuildTaskStore();

        long elapsed = System.currentTimeMillis() - startTime;

        logger.info("Recovery completed in {}ms: {} tasks recovered, {} in-flight, {} redispatched",
                elapsed, stats.recoveredTasks, stats.inflightTasks, stats.redispatchedTasks);

        return new RecoveryResult(
                stats.walRecords,
                stats.recoveredTasks,
                stats.inflightTasks,
                stats.redispatchedTasks,
                elapsed
        );
    }

    /**
     * 处理单条 WAL 记录
     */
    private void processRecord(WalRecordV3 record) {
        String taskId = record.taskId();
        EventTypeV3 eventType = record.eventType();

        // 获取或创建任务状态
        TaskRecoveryState state = taskStates.computeIfAbsent(taskId, id -> new TaskRecoveryState());

        // 应用事件
        applyEvent(state, record);
        stats.processedRecords++;
    }

    /**
     * 应用事件到状态机
     */
    private void applyEvent(TaskRecoveryState state, WalRecordV3 record) {
        EventTypeV3 eventType = record.eventType();

        // 创建事件：初始化任务
        if (eventType == EventTypeV3.CREATE) {
            TaskV3 task = parseTaskFromPayload(record.payload());
            state.task = task;
            state.status = TaskStatusV3.PENDING;
        }

        // 状态转换
        switch (eventType) {
            case CREATE -> {
                state.status = TaskStatusV3.PENDING;
            }
            case SCHEDULE -> {
                if (state.status == TaskStatusV3.PENDING) {
                    state.status = TaskStatusV3.SCHEDULED;
                }
            }
            case READY -> {
                if (state.status == TaskStatusV3.SCHEDULED || state.status == TaskStatusV3.RETRY_WAIT) {
                    state.status = TaskStatusV3.READY;
                }
            }
            case DISPATCH -> {
                if (state.status == TaskStatusV3.READY) {
                    state.status = TaskStatusV3.RUNNING;
                }
            }
            case ACK -> {
                state.status = TaskStatusV3.SUCCESS;
            }
            case RETRY -> {
                if (state.status == TaskStatusV3.RUNNING) {
                    state.status = TaskStatusV3.RETRY_WAIT;
                }
            }
            case FAIL -> {
                state.status = TaskStatusV3.FAILED;
            }
            case CANCEL -> {
                state.status = TaskStatusV3.CANCELLED;
            }
            case EXPIRE -> {
                state.status = TaskStatusV3.EXPIRED;
            }
            case DEAD_LETTER -> {
                state.status = TaskStatusV3.DEAD_LETTER;
            }
            case MODIFY -> {
                // 修改事件：更新任务属性
                if (state.task != null && record.payload() != null) {
                    applyModify(state.task, record.payload());
                }
            }
            case FIRE_NOW -> {
                // 立即触发：设置为 READY
                state.status = TaskStatusV3.READY;
            }
            case CHECKPOINT -> {
                // 检查点：标记恢复位置
                state.lastCheckpointSeq = record.recordSeq();
            }
        }

        state.lastEventTime = record.eventTime();
        state.lastRecordSeq = record.recordSeq();
        state.version++;
    }

    /**
     * 重建任务存储
     */
    private void rebuildTaskStore() {
        for (Map.Entry<String, TaskRecoveryState> entry : taskStates.entrySet()) {
            TaskRecoveryState state = entry.getValue();

            // 只恢复非终态任务
            if (!isTerminalState(state.status)) {
                if (state.task != null) {
                    // 设置恢复后的状态
                    state.task.getLifecycle().forceSetStatus(state.status);
                    taskStore.add(state.task);
                    stats.recoveredTasks++;

                    // 统计 in-flight 任务
                    if (state.status == TaskStatusV3.RUNNING) {
                        stats.inflightTasks++;
                        stats.redispatchedTasks++;
                    }
                }
            }
        }
    }

    /**
     * 判断是否为终态
     */
    private boolean isTerminalState(TaskStatusV3 status) {
        return status == TaskStatusV3.SUCCESS ||
               status == TaskStatusV3.FAILED ||
               status == TaskStatusV3.CANCELLED ||
               status == TaskStatusV3.EXPIRED ||
               status == TaskStatusV3.DEAD_LETTER;
    }

    /**
     * 从 payload 解析任务
     */
    private TaskV3 parseTaskFromPayload(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        // 简化实现：实际应使用 JSON 解析
        // 这里返回一个基本任务对象
        return TaskV3.builder()
                .taskId("recovered-" + System.nanoTime())
                .webhookUrl("recovered")
                .build();
    }

    /**
     * 应用修改
     */
    private void applyModify(TaskV3 task, byte[] payload) {
        // 简化实现：实际应解析 payload 并应用修改
    }

    // ========== 内部类 ==========

    /**
     * 任务恢复状态（临时）
     */
    private static class TaskRecoveryState {
        TaskV3 task;
        TaskStatusV3 status;
        long lastEventTime;
        long lastRecordSeq;
        long lastCheckpointSeq;
        int version;
    }

    /**
     * 统计信息
     */
    public static class Stats {
        long walRecords;
        long processedRecords;
        long recoveredTasks;
        long inflightTasks;
        long redispatchedTasks;
    }

    /**
     * 恢复结果
     */
    public record RecoveryResult(
            long walRecords,
            long recoveredTasks,
            long inflightTasks,
            long redispatchedTasks,
            long elapsedMs
    ) {
        public static RecoveryResult empty() {
            return new RecoveryResult(0, 0, 0, 0, 0);
        }

        @Override
        public String toString() {
            return String.format(
                    "RecoveryResult{wal=%d, recovered=%d, inflight=%d, redispatched=%d, elapsed=%dms}",
                    walRecords, recoveredTasks, inflightTasks, redispatchedTasks, elapsedMs);
        }
    }

    /**
     * 恢复配置
     */
    public record RecoveryConfig(
            int batchSize,
            int concurrencyLimit,
            boolean safeMode
    ) {
        public static RecoveryConfig defaultConfig() {
            return new RecoveryConfig(1000, 100, false);
        }
    }

    /**
     * WAL 记录 V3
     */
    public record WalRecordV3(
            long recordSeq,
            String taskId,
            EventTypeV3 eventType,
            long eventTime,
            byte[] payload
    ) {}
}
