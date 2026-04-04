package com.loomq.wal.v2;

import com.loomq.config.WalConfig;
import com.loomq.entity.EventType;
import com.loomq.wal.WalRecord;
import com.loomq.wal.WalSegment;
import com.loomq.wal.WalConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 同步 WAL 写入器（底层实现）
 * 被异步写入器调用，负责实际的磁盘写入
 */
public class SyncWalWriter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(SyncWalWriter.class);

    private final Path dataDir;
    private final long segmentSizeBytes;

    private final List<WalSegment> segments;
    private WalSegment currentSegment;

    private volatile boolean closed = false;

    public SyncWalWriter(WalConfig config) throws IOException {
        this.dataDir = Path.of(config.dataDir());
        this.segmentSizeBytes = config.segmentSizeMb() * 1024L * 1024L;
        this.segments = new ArrayList<>();

        Files.createDirectories(dataDir);
        loadOrInitializeSegments();

        logger.info("SyncWalWriter initialized, segments={}", segments.size());
    }

    private void loadOrInitializeSegments() throws IOException {
        File[] files = dataDir.toFile().listFiles((dir, name) ->
                name.endsWith(WalConstants.FILE_EXTENSION));

        if (files != null && files.length > 0) {
            for (File file : files) {
                int seq = WalSegment.parseSegmentSeq(file.getName());
                WalSegment segment = new WalSegment(file, seq, segmentSizeBytes, true);
                segments.add(segment);
            }
            segments.sort(null);

            // 重新打开最后一个段
            if (!segments.isEmpty()) {
                WalSegment last = segments.remove(segments.size() - 1);
                last.close();
                currentSegment = new WalSegment(last.getFile(), last.getSegmentSeq(),
                        segmentSizeBytes, false);
            }
        }

        if (currentSegment == null) {
            createNewSegment();
        }
    }

    private void createNewSegment() throws IOException {
        int seq = segments.size();
        File file = dataDir.resolve(WalSegment.getFileName(seq)).toFile();
        currentSegment = new WalSegment(file, seq, segmentSizeBytes);
        logger.info("Created new WAL segment: {}", file.getName());
    }

    /**
     * 追加记录（同步）
     */
    public long append(String taskId, String bizKey, EventType eventType,
                       long eventTime, byte[] payload) throws IOException {

        if (closed) {
            throw new IllegalStateException("Writer is closed");
        }

        if (currentSegment.isFull()) {
            rollover();
        }

        WalRecord record = WalRecord.create(
                currentSegment.getSegmentSeq(),
                currentSegment.getRecordSeq(),
                taskId, bizKey, eventType, eventTime, payload
        );

        return currentSegment.write(record);
    }

    /**
     * 刷盘（fsync）
     */
    public void sync() throws IOException {
        if (currentSegment != null) {
            currentSegment.sync();
        }
    }

    private void rollover() throws IOException {
        currentSegment.writeEndMarker();
        currentSegment.markReadOnly();
        segments.add(currentSegment);
        createNewSegment();
    }

    public List<WalSegment> getAllSegments() {
        List<WalSegment> all = new ArrayList<>(segments);
        if (currentSegment != null) {
            all.add(currentSegment);
        }
        return all;
    }

    /**
     * 获取当前段序号
     */
    public int getSegmentSeq() {
        return currentSegment != null ? currentSegment.getSegmentSeq() : 0;
    }

    /**
     * 获取当前位置
     */
    public long getPosition() {
        return currentSegment != null ? currentSegment.size() : 0;
    }

    @Override
    public void close() {
        closed = true;
        try {
            if (currentSegment != null) {
                currentSegment.sync();
                currentSegment.close();
            }
        } catch (IOException e) {
            logger.error("Close current segment failed", e);
        }

        for (WalSegment segment : segments) {
            segment.close();
        }

        logger.info("SyncWalWriter closed");
    }
}
