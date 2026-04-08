package com.loomq.recovery.v2;

import com.loomq.wal.WalRecord;
import com.loomq.wal.WalSegment;
import com.loomq.wal.v2.CheckpointManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.loomq.wal.WalConstants.*;

/**
 * WAL 重放器 V2（支持从 Checkpoint 位置恢复）
 *
 * 核心改进：
 * 1. 支持从指定位置开始重放（基于 checkpoint）
 * 2. 序列号去重检查
 * 3. 高性能批量读取
 *
 * 性能目标：
 * - 重放速度 > 10万 records/s
 * - 100万任务恢复 < 30s
 *
 * @author loomq
 * @since v0.3+
 */
public class WalReplayerV2 {

    private static final Logger logger = LoggerFactory.getLogger(WalReplayerV2.class);

    // 段文件列表
    private final List<WalSegment> segments;

    // 恢复起始位置
    private final CheckpointManager.RecoveryPosition startPosition;

    // 安全模式
    private final boolean safeMode;

    // 统计
    private long totalRecords = 0;
    private long skippedRecords = 0;
    private long errorRecords = 0;

    // 已应用的最大 sequence（去重）
    private long lastAppliedSequence;

    /**
     * 创建 WAL 重放器
     *
     * @param segments WAL 段文件列表
     * @param startPosition 恢复起始位置
     * @param safeMode 安全模式
     */
    public WalReplayerV2(List<WalSegment> segments,
                         CheckpointManager.RecoveryPosition startPosition,
                         boolean safeMode) {
        this.segments = new ArrayList<>(segments);
        this.segments.sort(null);  // 按序号排序
        this.startPosition = startPosition;
        this.safeMode = safeMode;
        this.lastAppliedSequence = startPosition.lastRecordSeq();

        logger.info("WalReplayerV2 created: segments={}, startSegment={}, startPos={}, lastSeq={}",
                segments.size(), startPosition.segmentSeq(),
                startPosition.segmentPosition(), startPosition.lastRecordSeq());
    }

    /**
     * 重放记录到消费者
     *
     * @param consumer 记录消费者
     * @param skipBeforeSeq 跳过此序号之前的记录
     * @return 重放的记录数
     */
    public long replay(Consumer<WalRecord> consumer, long skipBeforeSeq) throws IOException {
        long startTime = System.currentTimeMillis();

        boolean foundStartPosition = false;

        for (WalSegment segment : segments) {
            // 跳过起始段之前的段
            if (segment.getSegmentSeq() < startPosition.segmentSeq()) {
                logger.debug("Skipping segment {} (before start position)", segment.getSegmentSeq());
                continue;
            }

            // 确定段内起始位置
            long segmentStartPos = 0;
            if (!foundStartPosition && segment.getSegmentSeq() == startPosition.segmentSeq()) {
                segmentStartPos = startPosition.segmentPosition();
                foundStartPosition = true;
                logger.debug("Starting from segment {} at position {}",
                        segment.getSegmentSeq(), segmentStartPos);
            }

            // 重放段
            replaySegment(segment, segmentStartPos, consumer, skipBeforeSeq);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        double rate = elapsed > 0 ? totalRecords / (elapsed / 1000.0) : 0;

        logger.info("Replay completed: {} records, {} skipped, {} errors, took {}ms, rate={}/s",
                totalRecords, skippedRecords, errorRecords, elapsed, String.format("%.0f", rate));

        return totalRecords;
    }

    /**
     * 重放单个段文件
     */
    private void replaySegment(WalSegment segment, long startPos,
                               Consumer<WalRecord> consumer, long skipBeforeSeq) throws IOException {
        logger.debug("Replaying segment: {} from position {}", segment.getFile().getName(), startPos);

        try (FileChannel channel = FileChannel.open(segment.getFile().toPath(), StandardOpenOption.READ)) {
            long position = startPos;
            long fileSize = channel.size();

            while (position < fileSize) {
                WalRecord record = readRecord(channel, position);

                if (record == null) {
                    break;  // 文件结束或损坏
                }

                totalRecords++;

                // 去重检查：跳过已处理的记录
                if (record.getRecordSeq() <= skipBeforeSeq) {
                    skippedRecords++;
                    position += calculateRecordSize(record);
                    continue;
                }

                // 消费记录
                if (consumer != null) {
                    try {
                        consumer.accept(record);
                    } catch (Exception e) {
                        logger.error("Error consuming record at position {}: {}", position, e.getMessage());
                        errorRecords++;
                        if (safeMode) {
                            throw new IOException("Safe mode: stopping on error", e);
                        }
                    }
                }

                // 更新已应用的 sequence
                lastAppliedSequence = Math.max(lastAppliedSequence, record.getRecordSeq());

                // 计算下一条记录位置
                position += calculateRecordSize(record);
            }
        }
    }

    /**
     * 读取单条记录
     */
    private WalRecord readRecord(FileChannel channel, long position) throws IOException {
        // 读取 header
        ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
        channel.position(position);
        int headerRead = channel.read(headerBuffer);

        if (headerRead < HEADER_SIZE) {
            return null;  // 文件结束
        }

        headerBuffer.flip();

        // 解析 header
        int magic = headerBuffer.getInt();
        if (magic == SEGMENT_END_MARKER) {
            return null;  // 段结束标记
        }
        if (magic != MAGIC) {
            logger.warn("Invalid magic number at position {}: {}", position, magic);
            return null;
        }

        short recordVersion = headerBuffer.getShort();
        byte recordType = headerBuffer.get();
        int segmentSeq = headerBuffer.getInt();
        long recordSeq = headerBuffer.getLong();
        short taskIdLen = headerBuffer.getShort();
        short bizKeyLen = headerBuffer.getShort();
        long eventTime = headerBuffer.getLong();
        int payloadLen = headerBuffer.getInt();

        // 验证长度
        if (taskIdLen < 0 || taskIdLen > MAX_TASK_ID_LENGTH ||
            bizKeyLen < 0 || bizKeyLen > MAX_BIZ_KEY_LENGTH ||
            payloadLen < 0 || payloadLen > MAX_PAYLOAD_LENGTH) {
            logger.warn("Invalid record lengths at position {}: taskIdLen={}, bizKeyLen={}, payloadLen={}",
                    position, taskIdLen, bizKeyLen, payloadLen);
            return null;
        }

        // 计算总大小
        int totalSize = HEADER_SIZE + taskIdLen + bizKeyLen + payloadLen + CHECKSUM_SIZE;

        // 检查文件是否有足够数据
        if (position + totalSize > channel.size()) {
            logger.warn("Incomplete record at position {}, expected {}, available {}",
                    position, totalSize, channel.size() - position);
            return null;
        }

        // 读取完整记录
        ByteBuffer recordBuffer = ByteBuffer.allocate(totalSize);
        channel.position(position);
        int totalRead = channel.read(recordBuffer);

        if (totalRead < totalSize) {
            logger.warn("Failed to read complete record at position {}", position);
            return null;
        }

        // 解码记录
        try {
            return WalRecord.decode(recordBuffer.array());
        } catch (IllegalArgumentException e) {
            logger.warn("Failed to decode record at position {}: {}", position, e.getMessage());
            return null;
        }
    }

    /**
     * 计算记录大小
     */
    private int calculateRecordSize(WalRecord record) {
        int taskIdLen = record.getTaskId() != null ?
                record.getTaskId().getBytes(java.nio.charset.StandardCharsets.UTF_8).length : 0;
        int bizKeyLen = record.getBizKey() != null ?
                record.getBizKey().getBytes(java.nio.charset.StandardCharsets.UTF_8).length : 0;
        int payloadLen = record.getPayload() != null ? record.getPayload().length : 0;
        return HEADER_SIZE + taskIdLen + bizKeyLen + payloadLen + CHECKSUM_SIZE;
    }

    // Getters

    public long getTotalRecords() {
        return totalRecords;
    }

    public long getSkippedRecords() {
        return skippedRecords;
    }

    public long getErrorRecords() {
        return errorRecords;
    }

    public long getLastAppliedSequence() {
        return lastAppliedSequence;
    }
}
