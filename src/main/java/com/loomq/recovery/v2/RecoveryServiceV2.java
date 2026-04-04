package com.loomq.recovery.v2;

import com.loomq.entity.EventType;
import com.loomq.entity.Task;
import com.loomq.entity.TaskStatus;
import com.loomq.scheduler.v2.PrecisionScheduler;
import com.loomq.store.TaskStore;
import com.loomq.wal.WalEngine;
import com.loomq.wal.WalRecord;
import com.loomq.wal.WalSegment;
import com.loomq.wal.v2.CheckpointManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 恢复服务 V2（带 Checkpoint 支持）
 *
 * 核心功能：
 * 1. 加载 checkpoint
 * 2. 从 checkpoint 位置 replay WAL
 * 3. 重建任务状态
 * 4. 重建调度器
 * 5. 处理 DISPATCHING 任务（重新执行）
 *
 * 恢复策略：
 * | 状态          | 处理                 |
 * |---------------|----------------------|
 * | PENDING       | 重新调度             |
 * | SCHEDULED     | 放入调度器           |
 * | DISPATCHING   | 重新执行（关键改进） |
 * | RETRY_WAIT    | 重新调度             |
 * | ACKED         | 忽略（已完成）       |
 * | CANCELLED     | 忽略                 |
 *
 * @author loomq
 * @since v0.3+
 */
public class RecoveryServiceV2 {

    private static final Logger logger = LoggerFactory.getLogger(RecoveryServiceV2.class);

    // WAL 引擎
    private final WalEngine walEngine;

    // 任务存储
    private final TaskStore taskStore;

    // 调度器
    private final PrecisionScheduler scheduler;

    // Checkpoint 管理器
    private final CheckpointManager checkpointManager;

    // 配置
    private final RecoveryConfig config;

    // 任务状态机（临时）
    private final Map<String, TaskRecoveryState> taskStates;

    // 统计
    private final Stats stats = new Stats();

    /**
     * 创建恢复服务
     */
    public RecoveryServiceV2(WalEngine walEngine, TaskStore taskStore,
                             PrecisionScheduler scheduler, CheckpointManager checkpointManager,
                             RecoveryConfig config) {
        this.walEngine = walEngine;
        this.taskStore = taskStore;
        this.scheduler = scheduler;
        this.checkpointManager = checkpointManager;
        this.config = config;
        this.taskStates = new ConcurrentHashMap<>();
    }

    /**
     * 执行恢复
     */
    public RecoveryResult recover() throws IOException {
        long startTime = System.currentTimeMillis();

        logger.info("Starting recovery...");

        // 1. 获取所有 WAL 段
        List<WalSegment> segments = walEngine.getAllSegments();
        logger.info("Found {} WAL segments", segments.size());

        if (segments.isEmpty()) {
            logger.info("No WAL segments found, recovery skipped");
            return RecoveryResult.empty();
        }

        // 2. 获取恢复起始位置（从 checkpoint 或从头开始）
        CheckpointManager.RecoveryPosition position = checkpointManager.getRecoveryStartPosition();
        long lastAppliedSeq = position.lastRecordSeq();

        logger.info("Recovery starting from segment {}, position {}, lastSeq={}",
                position.segmentSeq(), position.segmentPosition(), lastAppliedSeq);

        // 3. 过滤需要重放的段
        List<WalSegment> segmentsToReplay = filterSegments(segments, position.segmentSeq());
        logger.info("Segments to replay: {}", segmentsToReplay.size());

        // 4. 创建重放器
        WalReplayerV2 replayer = new WalReplayerV2(segmentsToReplay, position, config.safeMode());

        // 5. 重放 WAL，构建任务状态
        replayer.replay(this::processRecord, lastAppliedSeq);

        stats.walRecords = replayer.getTotalRecords();
        stats.skippedRecords = replayer.getSkippedRecords();

        // 6. 重建任务存储和调度器
        rebuildTaskStore();
        rebuildScheduler();

        long elapsed = System.currentTimeMillis() - startTime;

        logger.info("Recovery completed in {}ms: {} tasks recovered, {} in-flight, {} dispatched",
                elapsed, stats.recoveredTasks, stats.inflightTasks, stats.redispatchedTasks);

        return new RecoveryResult(
                stats.walRecords,
                stats.recoveredTasks,
                stats.inflightTasks,
                stats.redispatchedTasks,
                stats.skippedRecords,
                elapsed
        );
    }

    /**
     * 过滤需要重放的段
     */
    private List<WalSegment> filterSegments(List<WalSegment> allSegments, int startSegmentSeq) {
        List<WalSegment> result = new ArrayList<>();
        for (WalSegment segment : allSegments) {
            if (segment.getSegmentSeq() >= startSegmentSeq) {
                result.add(segment);
            }
        }
        return result;
    }

    /**
     * 处理单条 WAL 记录
     */
    private void processRecord(WalRecord record) {
        String taskId = record.getTaskId();
        EventType eventType = record.getEventType();

        // 获取或创建任务状态
        TaskRecoveryState state = taskStates.computeIfAbsent(taskId, id -> new TaskRecoveryState());

        // 应用事件
        applyEvent(state, record);

        stats.processedRecords++;
    }

    /**
     * 应用事件到状态机
     */
    private void applyEvent(TaskRecoveryState state, WalRecord record) {
        EventType eventType = record.getEventType();

        // 更新任务信息
        if (eventType == EventType.CREATE) {
            Task task = new Task();
            task.setTaskId(record.getTaskId());
            task.setBizKey(record.getBizKey());
            task.setTriggerTime(record.getEventTime());
            task.setStatus(TaskStatus.PENDING);
            task.setCreateTime(record.getEventTime());
            if (record.getPayload() != null && record.getPayload().length > 0) {
                task.setPayload(new String(record.getPayload()));
            }
            state.task = task;
            state.status = TaskStatus.PENDING;
        }

        // 状态转换
        switch (eventType) {
            case CREATE -> {
                state.status = TaskStatus.PENDING;
            }
            case DISPATCH -> {
                if (state.status == TaskStatus.PENDING || state.status == TaskStatus.SCHEDULED) {
                    state.status = TaskStatus.DISPATCHING;
                }
            }
            case ACK -> {
                state.status = TaskStatus.ACKED;
            }
            case CANCEL -> {
                state.status = TaskStatus.CANCELLED;
            }
            case RETRY -> {
                if (state.status == TaskStatus.DISPATCHING) {
                    state.status = TaskStatus.RETRY_WAIT;
                }
            }
            case FAIL -> {
                state.status = TaskStatus.FAILED_TERMINAL;
            }
        }

        state.lastEventTime = record.getEventTime();
        state.lastRecordSeq = record.getRecordSeq();
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
                    state.task.setStatus(state.status);
                    taskStore.add(state.task);
                    stats.recoveredTasks++;
                }
            }
        }
    }

    /**
     * 重建调度器（关键改进：处理 DISPATCHING 任务）
     */
    private void rebuildScheduler() {
        long now = System.currentTimeMillis();

        for (Map.Entry<String, TaskRecoveryState> entry : taskStates.entrySet()) {
            TaskRecoveryState state = entry.getValue();

            switch (state.status) {
                case PENDING, SCHEDULED -> {
                    // 重新调度
                    if (state.task != null) {
                        if (state.task.getTriggerTime() > now) {
                            // 未来任务：正常调度
                            scheduler.schedule(state.task);
                            stats.scheduledTasks++;
                        } else {
                            // 已到期任务：立即执行
                            state.task.setStatus(TaskStatus.SCHEDULED);
                            scheduler.schedule(state.task);
                            stats.scheduledTasks++;
                        }
                    }
                }
                case DISPATCHING -> {
                    // In-flight 任务：重新执行（关键改进）
                    stats.inflightTasks++;
                    if (state.task != null) {
                        // 标记为 SCHEDULED 后立即调度
                        state.task.setStatus(TaskStatus.SCHEDULED);
                        scheduler.schedule(state.task);
                        stats.redispatchedTasks++;

                        if (config.safeMode()) {
                            logger.warn("In-flight task will be redispatched: {}", entry.getKey());
                        } else {
                            logger.info("In-flight task redispatched: {}", entry.getKey());
                        }
                    }
                }
                case RETRY_WAIT -> {
                    // 重试等待中的任务：重新调度
                    if (state.task != null) {
                        state.task.setStatus(TaskStatus.SCHEDULED);
                        scheduler.schedule(state.task);
                        stats.scheduledTasks++;
                    }
                }
                default -> {
                    // 终态任务忽略
                }
            }
        }
    }

    /**
     * 判断是否为终态
     */
    private boolean isTerminalState(TaskStatus status) {
        return status == TaskStatus.ACKED ||
               status == TaskStatus.CANCELLED ||
               status == TaskStatus.FAILED_TERMINAL;
    }

    // ========== 内部类 ==========

    /**
     * 任务恢复状态（临时）
     */
    private static class TaskRecoveryState {
        Task task;
        TaskStatus status;
        long lastEventTime;
        long lastRecordSeq;
        int version;
    }

    /**
     * 统计信息
     */
    public static class Stats {
        long walRecords;
        long processedRecords;
        long skippedRecords;
        long recoveredTasks;
        long scheduledTasks;
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
            long skippedRecords,
            long elapsedMs
    ) {
        public static RecoveryResult empty() {
            return new RecoveryResult(0, 0, 0, 0, 0, 0);
        }

        @Override
        public String toString() {
            return String.format(
                    "RecoveryResult{wal=%d, recovered=%d, inflight=%d, redispatched=%d, skipped=%d, elapsed=%dms}",
                    walRecords, recoveredTasks, inflightTasks, redispatchedTasks, skippedRecords, elapsedMs);
        }
    }

    /**
     * 恢复配置
     */
    public record RecoveryConfig(
            int batchSize,
            int sleepMs,
            int concurrencyLimit,
            boolean safeMode
    ) {
        public static RecoveryConfig defaultConfig() {
            return new RecoveryConfig(1000, 10, 100, false);
        }
    }
}
