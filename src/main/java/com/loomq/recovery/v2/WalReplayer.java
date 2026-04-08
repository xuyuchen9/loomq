package com.loomq.recovery.v2;

import com.loomq.wal.WalRecord;
import com.loomq.wal.WalSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import static com.loomq.wal.WalConstants.*;

/**
 * WAL 重放器（V0.3 高性能版本）
 *
 * 核心功能：
 * 1. 顺序读取所有 WAL 段文件
 * 2. 解析 WalRecord 并过滤损坏数据
 * 3. 支持批量重放优化
 * 4. 支持从指定位置恢复
 *
 * 性能目标：
 * - 重放速度 > 10万 records/s
 * - 100万任务恢复 < 30s
 *
 * @author loomq
 * @since v0.3
 */
public class WalReplayer {

    private static final Logger logger = LoggerFactory.getLogger(WalReplayer.class);

    // WAL 目录
    private final Path walDir;

    // 段文件列表
    private final List<WalSegment> segments;

    // 安全模式（遇到错误时停止）
    private final boolean safeMode;

    // 已应用的最大 sequence
    private long lastAppliedSequence = -1;

    // 统计
    private long totalRecords = 0;
    private long skippedRecords = 0;
    private long errorRecords = 0;

    /**
     * 创建 WAL 重放器
     *
     * @param segments WAL 段文件列表
     * @param safeMode 安全模式（遇到错误停止）
     */
    public WalReplayer(List<WalSegment> segments, boolean safeMode) {
        this.walDir = null;
        this.segments = new ArrayList<>(segments);
        this.segments.sort(null);  // 按序号排序
        this.safeMode = safeMode;

        logger.info("WalReplayer created with {} segments, safeMode={}", segments.size(), safeMode);
    }

    /**
     * 重放所有记录
     *
     * @return 重放的记录数
     */
    public long replayAll() throws IOException {
        return replayAll(null);
    }

    /**
     * 重放所有记录到消费者
     *
     * @param consumer 记录消费者
     * @return 重放的记录数
     */
    public long replayAll(Consumer<WalRecord> consumer) throws IOException {
        long startTime = System.currentTimeMillis();

        for (WalSegment segment : segments) {
            replaySegment(segment, consumer);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        double rate = totalRecords / (elapsed / 1000.0);

        logger.info("Replay completed: {} records, {} skipped, {} errors, took {}ms, rate={}/s",
                totalRecords, skippedRecords, errorRecords, elapsed, String.format("%.0f", rate));

        return totalRecords;
    }

    /**
     * 重放单个段文件
     */
    private void replaySegment(WalSegment segment, Consumer<WalRecord> consumer) throws IOException {
        logger.debug("Replaying segment: {}", segment.getFile().getName());

        try (FileChannel channel = FileChannel.open(segment.getFile().toPath())) {
            long position = 0;
            long fileSize = channel.size();

            while (position < fileSize) {
                WalRecord record = readRecord(channel, position);

                if (record == null) {
                    break;  // 文件结束
                }

                totalRecords++;

                // 去重检查
                if (record.getRecordSeq() <= lastAppliedSequence) {
                    skippedRecords++;
                    position += calculateRecordSize(record);
                    continue;
                }

                // 消费记录
                if (consumer != null) {
                    try {
                        consumer.accept(record);
                    } catch (Exception e) {
                        logger.error("Error consuming record at position {}", position, e);
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
            logger.warn("Invalid magic number at position {}", position);
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
            logger.warn("Invalid record lengths at position {}", position);
            return null;
        }

        // 计算总大小
        int totalSize = HEADER_SIZE + taskIdLen + bizKeyLen + payloadLen + CHECKSUM_SIZE;

        // 读取完整记录
        ByteBuffer recordBuffer = ByteBuffer.allocate(totalSize);
        channel.position(position);
        int totalRead = channel.read(recordBuffer);

        if (totalRead < totalSize) {
            logger.warn("Incomplete record at position {}", position);
            return null;
        }

        // 解码记录
        return WalRecord.decode(recordBuffer.array());
    }

    /**
     * 计算记录大小
     */
    private int calculateRecordSize(WalRecord record) {
        int taskIdLen = record.getTaskId() != null ? record.getTaskId().getBytes().length : 0;
        int bizKeyLen = record.getBizKey() != null ? record.getBizKey().getBytes().length : 0;
        int payloadLen = record.getPayload() != null ? record.getPayload().length : 0;
        return HEADER_SIZE + taskIdLen + bizKeyLen + payloadLen + CHECKSUM_SIZE;
    }

    /**
     * 获取记录迭代器（流式处理）
     */
    public Iterator<WalRecord> iterator() {
        return new WalRecordIterator();
    }

    /**
     * WAL 记录迭代器
     */
    private class WalRecordIterator implements Iterator<WalRecord> {
        private int currentSegmentIndex = 0;
        private FileChannel currentChannel;
        private long currentPosition = 0;
        private WalRecord nextRecord;
        private boolean finished = false;

        WalRecordIterator() {
            if (!segments.isEmpty()) {
                try {
                    currentChannel = FileChannel.open(segments.get(0).getFile().toPath());
                } catch (IOException e) {
                    logger.error("Failed to open first segment", e);
                    finished = true;
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (finished) return false;
            if (nextRecord != null) return true;

            nextRecord = readNext();
            if (nextRecord == null) {
                finished = true;
            }
            return nextRecord != null;
        }

        @Override
        public WalRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            WalRecord record = nextRecord;
            nextRecord = null;
            return record;
        }

        private WalRecord readNext() {
            while (currentSegmentIndex < segments.size()) {
                try {
                    WalRecord record = readRecord(currentChannel, currentPosition);
                    if (record != null) {
                        currentPosition += calculateRecordSize(record);
                        totalRecords++;
                        return record;
                    }

                    // 当前段读完，切换下一个
                    currentChannel.close();
                    currentSegmentIndex++;
                    currentPosition = 0;

                    if (currentSegmentIndex < segments.size()) {
                        currentChannel = FileChannel.open(
                                segments.get(currentSegmentIndex).getFile().toPath());
                    }

                } catch (IOException e) {
                    logger.error("Error reading segment {}", currentSegmentIndex, e);
                    finished = true;
                    return null;
                }
            }
            return null;
        }
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
