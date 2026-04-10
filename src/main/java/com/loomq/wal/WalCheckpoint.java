package com.loomq.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * WAL 检查点管理器
 *
 * 负责 flushedPosition 的持久化存储
 *
 * 文件格式（固定 32 字节）：
 * +--------+--------+--------+--------+
 * | magic  | flushedPosition | recordSeq |
 * | (4B)   | (8B)            | (8B)      |
 * +--------+--------+--------+--------+
 * | segmentSeq | checksum (CRC32) | padding |
 * | (4B)       | (4B)              | (4B)    |
 * +--------+--------+--------+--------+
 *
 * @author loomq
 * @since v0.7.0
 */
public class WalCheckpoint {

    private static final Logger logger = LoggerFactory.getLogger(WalCheckpoint.class);

    private static final int MAGIC = 0x57414C43;  // "WALC"
    private static final int CHECKPOINT_SIZE = 32;

    private final Path checkpointPath;
    private final FileChannel channel;

    // 检查点数据
    private volatile long flushedPosition;
    private volatile long recordSeq;
    private volatile int segmentSeq;

    public WalCheckpoint(Path walDir) throws IOException {
        this.checkpointPath = walDir.resolve("checkpoint.meta");

        // 确保目录存在
        Files.createDirectories(walDir);

        // 打开或创建检查点文件
        if (Files.exists(checkpointPath)) {
            this.channel = FileChannel.open(checkpointPath,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            load();
        } else {
            this.channel = FileChannel.open(checkpointPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            this.flushedPosition = 0;
            this.recordSeq = 0;
            this.segmentSeq = 0;
        }

        logger.info("WalCheckpoint initialized: path={}, flushedPos={}, recordSeq={}, segmentSeq={}",
                checkpointPath, flushedPosition, recordSeq, segmentSeq);
    }

    /**
     * 加载检查点
     */
    private void load() throws IOException {
        if (channel.size() < CHECKPOINT_SIZE) {
            logger.warn("Checkpoint file too small, starting fresh");
            this.flushedPosition = 0;
            this.recordSeq = 0;
            this.segmentSeq = 0;
            return;
        }

        ByteBuffer buffer = ByteBuffer.allocate(CHECKPOINT_SIZE);
        channel.position(0);
        channel.read(buffer);
        buffer.flip();

        int magic = buffer.getInt();
        if (magic != MAGIC) {
            logger.warn("Invalid checkpoint magic, starting fresh");
            this.flushedPosition = 0;
            this.recordSeq = 0;
            this.segmentSeq = 0;
            return;
        }

        this.flushedPosition = buffer.getLong();
        this.recordSeq = buffer.getLong();
        this.segmentSeq = buffer.getInt();
        int storedChecksum = buffer.getInt();

        // 验证校验和
        int calculatedChecksum = calculateChecksum(flushedPosition, recordSeq, segmentSeq);
        if (storedChecksum != calculatedChecksum) {
            logger.warn("Checkpoint checksum mismatch, starting fresh");
            this.flushedPosition = 0;
            this.recordSeq = 0;
            this.segmentSeq = 0;
            return;
        }

        logger.info("Checkpoint loaded: flushedPos={}, recordSeq={}, segmentSeq={}",
                flushedPosition, recordSeq, segmentSeq);
    }

    /**
     * 保存检查点
     */
    public synchronized void save(long flushedPosition, long recordSeq, int segmentSeq) throws IOException {
        this.flushedPosition = flushedPosition;
        this.recordSeq = recordSeq;
        this.segmentSeq = segmentSeq;

        int checksum = calculateChecksum(flushedPosition, recordSeq, segmentSeq);

        ByteBuffer buffer = ByteBuffer.allocate(CHECKPOINT_SIZE);
        buffer.putInt(MAGIC);
        buffer.putLong(flushedPosition);
        buffer.putLong(recordSeq);
        buffer.putInt(segmentSeq);
        buffer.putInt(checksum);
        buffer.putInt(0);  // padding
        buffer.flip();

        channel.position(0);
        channel.write(buffer);
        channel.force(true);

        logger.debug("Checkpoint saved: flushedPos={}, recordSeq={}, segmentSeq={}",
                flushedPosition, recordSeq, segmentSeq);
    }

    /**
     * 获取已刷盘位置
     */
    public long getFlushedPosition() {
        return flushedPosition;
    }

    /**
     * 获取记录序列号
     */
    public long getRecordSeq() {
        return recordSeq;
    }

    /**
     * 获取段序列号
     */
    public int getSegmentSeq() {
        return segmentSeq;
    }

    /**
     * 计算校验和
     */
    private int calculateChecksum(long flushedPosition, long recordSeq, int segmentSeq) {
        CRC32 crc32 = new CRC32();
        ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.putLong(flushedPosition);
        buffer.putLong(recordSeq);
        buffer.putInt(segmentSeq);
        crc32.update(buffer.array());
        return (int) crc32.getValue();
    }

    /**
     * 关闭检查点文件
     */
    public void close() throws IOException {
        channel.close();
        logger.info("WalCheckpoint closed");
    }
}
