package com.loomq.infrastructure.wal;

import com.loomq.config.WalConfig;
import com.loomq.spi.WalAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32;

/**
 * 极简 WAL 写入器 — 段文件轮转支持。
 *
 * 格式：| Length (4B) | Payload (N) | CRC32 (4B) |
 * 总计：8 字节固定头部开销。
 *
 * 段文件：wal-000001.bin, wal-000002.bin, ... 每个段独立 mmap。
 * Snapshot 后截断旧段释放磁盘空间。
 *
 * 核心设计：
 * 1. 不做字段解析，只做字节搬运
 * 2. 批量刷盘摊平 fsync 开销
 * 3. MemorySegment 零拷贝写入
 * 4. 段文件轮转，支持截断
 * 5. 实现 WalAccessor SPI 供服务层读取
 *
 * @author loomq
 * @since v0.6.1
 */
public class SimpleWalWriter implements AutoCloseable, WalAccessor {

    private static final Logger logger = LoggerFactory.getLogger(SimpleWalWriter.class);

    // ========== 格式常量 ==========
    private static final int HEADER_SIZE = 4;
    private static final int CHECKSUM_SIZE = 4;
    private static final int RECORD_OVERHEAD = HEADER_SIZE + CHECKSUM_SIZE;
    private static final int TERM_PREFIX_SIZE = Long.BYTES;

    public static final int RECORD_FIXED_OVERHEAD = HEADER_SIZE + CHECKSUM_SIZE;

    public static int recordLength(int payloadLength) {
        return RECORD_FIXED_OVERHEAD + payloadLength;
    }

    // ========== 默认配置 ==========
    private static final long DEFAULT_SEGMENT_SIZE = 256 * 1024 * 1024L; // 256MB per segment
    private static final long DEFAULT_FLUSH_THRESHOLD = 64 * 1024;       // 64KB
    private static final long DEFAULT_FLUSH_INTERVAL_MS = 10;

    // ========== 文件管理 ==========
    private final Path dataDir;
    private final String shardId;
    private final long segmentSize;        // 每段最大字节
    private final Arena arena;

    // ========== 段文件管理 ==========
    private final List<Segment> segments = new CopyOnWriteArrayList<>();
    private volatile Segment currentSegment;
    private int nextSegmentIndex = 1;

    // ========== 写入状态（全局偏移）==========
    private final AtomicLong writePosition = new AtomicLong(0);
    private volatile long flushedPosition = 0;
    private volatile long lastFsyncTimestampMs = 0;

    // ========== Raft 元数据 ==========
    private final Path raftMetaPath;
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private volatile long lastLogEntryTerm = 0;
    private volatile long snapshotIndex = 0;
    private volatile long snapshotTerm = 0;
    private volatile long snapshotOffset = 0;

    // ========== 刷盘协调 ==========
    private final StripedCondition flushConditions;
    private final Thread flushThread;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long flushThreshold;
    private final long flushIntervalNs;
    private volatile boolean flushRequested = false;

    // ========== 统计 ==========
    private final Stats stats = new Stats();

    public static class Stats {
        private final AtomicLong writeCount = new AtomicLong(0);
        private final AtomicLong flushCount = new AtomicLong(0);
        private final AtomicLong totalFlushTimeNs = new AtomicLong(0);
        private final AtomicLong totalBytesWritten = new AtomicLong(0);

        public void recordWrite(int bytes) {
            writeCount.incrementAndGet();
            totalBytesWritten.addAndGet(bytes);
        }

        public void recordFlush(long elapsedNs) {
            flushCount.incrementAndGet();
            totalFlushTimeNs.addAndGet(elapsedNs);
        }

        public long getWriteCount() { return writeCount.get(); }
        public long getFlushCount() { return flushCount.get(); }
        public long getTotalBytesWritten() { return totalBytesWritten.get(); }
        public double getAvgFlushTimeMs() {
            long flushes = flushCount.get();
            return flushes > 0 ? (totalFlushTimeNs.get() / (double) flushes) / 1_000_000.0 : 0;
        }
    }

    /**
     * 单个 WAL 段。
     */
    private static class Segment {
        final int index;
        final Path path;
        final long startGlobalOffset;  // 全局偏移起始
        final long size;               // 文件大小（bytes）
        FileChannel channel;
        MemorySegment mappedRegion;
        volatile boolean closed;

        Segment(int index, Path path, long startGlobalOffset, long size) {
            this.index = index;
            this.path = path;
            this.startGlobalOffset = startGlobalOffset;
            this.size = size;
        }

        /** 全局偏移的最后位置（不含） */
        long endGlobalOffset() {
            return startGlobalOffset + size;
        }

        /** 全局偏移转段内偏移 */
        long toLocal(long globalPos) {
            return globalPos - startGlobalOffset;
        }
    }

    public SimpleWalWriter(WalConfig config, String shardId) throws IOException {
        this.dataDir = Path.of(config.dataDir(), shardId);
        this.shardId = shardId;
        this.segmentSize = config.memorySegmentInitialSizeMb() * 1024L * 1024L;
        this.flushThreshold = config.memorySegmentFlushThresholdKb() * 1024L;
        this.flushIntervalNs = config.memorySegmentFlushIntervalMs() * 1_000_000L;

        this.flushConditions = new StripedCondition(config.memorySegmentStripeCount());
        this.arena = Arena.ofShared();

        Files.createDirectories(dataDir);

        // Raft 元数据持久化文件
        this.raftMetaPath = dataDir.resolve("raft_meta");
        loadRaftMeta();

        // Recover existing segments if the process is restarting on an existing WAL dir.
        // If nothing exists yet, create the initial segment at offset 0.
        loadExistingSegments();
        if (segments.isEmpty()) {
            rotateToNewSegment(0);
        } else {
            currentSegment = segments.get(segments.size() - 1);
            nextSegmentIndex = currentSegment.index + 1;
            logger.info("Recovered WAL: segments={}, writePosition={}, lastLogEntryTerm={}",
                segments.size(), writePosition.get(), lastLogEntryTerm);
        }

        this.flushThread = Thread.ofPlatform()
            .name("wal-flusher-" + shardId)
            .daemon(true)
            .unstarted(this::flushLoop);

        logger.info("SimpleWalWriter created: dir={}, segmentSize={}MB, flushThreshold={}KB",
            dataDir, segmentSize / 1024 / 1024, flushThreshold / 1024);
    }

    // ========== 构造函数 ==========

    private void rotateToNewSegment(long startGlobalOffset) throws IOException {
        String filename = String.format("wal-%06d.bin", nextSegmentIndex);
        Path path = dataDir.resolve(filename);

        FileChannel channel = FileChannel.open(path,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        channel.truncate(segmentSize);

        MemorySegment mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, segmentSize, arena);

        Segment seg = new Segment(nextSegmentIndex, path, startGlobalOffset, segmentSize);
        seg.channel = channel;
        seg.mappedRegion = mapped;

        segments.add(seg);
        currentSegment = seg;
        nextSegmentIndex++;
        writePosition.set(startGlobalOffset);

        logger.debug("Created WAL segment: {} (global offset {})", filename, startGlobalOffset);
    }

    // ========== 段文件管理 ==========

    private void loadExistingSegments() throws IOException {
        List<Path> existing = new ArrayList<>();
        try (var stream = Files.list(dataDir)) {
            stream.filter(path -> {
                    String name = path.getFileName().toString();
                    return name.startsWith("wal-") && name.endsWith(".bin");
                })
                .sorted(Comparator.comparingInt(path -> extractSegmentIndex(path.getFileName().toString())))
                .forEach(existing::add);
        }

        long recoveredWritePosition = 0;
        long recoveredLastLogEntryTerm = 0;
        int maxSegmentIndex = 0;

        for (Path path : existing) {
            int segmentIndex = extractSegmentIndex(path.getFileName().toString());
            long segmentStart = ((long) segmentIndex - 1L) * segmentSize;
            Segment seg = new Segment(segmentIndex, path, segmentStart, segmentSize);
            seg.channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            seg.mappedRegion = seg.channel.map(FileChannel.MapMode.READ_WRITE, 0, segmentSize, arena);

            SegmentRecovery recovery = scanSegment(seg);
            segments.add(seg);

            recoveredWritePosition = segmentStart + recovery.usedBytes;
            if (recovery.usedBytes > 0) {
                recoveredLastLogEntryTerm = recovery.lastEntryTerm;
            }

            maxSegmentIndex = Math.max(maxSegmentIndex, segmentIndex);
        }

        if (!segments.isEmpty()) {
            currentSegment = segments.get(segments.size() - 1);
            nextSegmentIndex = maxSegmentIndex + 1;
            writePosition.set(recoveredWritePosition);
            flushedPosition = recoveredWritePosition;
            lastLogEntryTerm = recoveredLastLogEntryTerm;
        }
    }

    private SegmentRecovery scanSegment(Segment seg) throws IOException {
        long localPos = 0;
        long lastTerm = 0;
        while (localPos + RECORD_OVERHEAD <= seg.size) {
            ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE)
                .order(java.nio.ByteOrder.nativeOrder());
            int headerBytes = seg.channel.read(headerBuf, localPos);
            if (headerBytes != HEADER_SIZE) {
                break;
            }
            headerBuf.flip();
            int payloadLen = headerBuf.getInt();
            if (payloadLen <= 0) {
                break;
            }

            long recordSize = RECORD_OVERHEAD + payloadLen;
            if (localPos + recordSize > seg.size) {
                break;
            }

            ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen);
            int payloadBytes = seg.channel.read(payloadBuf, localPos + HEADER_SIZE);
            if (payloadBytes != payloadLen) {
                break;
            }
            payloadBuf.flip();
            byte[] payload = new byte[payloadLen];
            payloadBuf.get(payload);

            ByteBuffer crcBuf = ByteBuffer.allocate(CHECKSUM_SIZE)
                .order(java.nio.ByteOrder.nativeOrder());
            int crcBytes = seg.channel.read(crcBuf, localPos + HEADER_SIZE + payloadLen);
            if (crcBytes != CHECKSUM_SIZE) {
                break;
            }
            crcBuf.flip();
            int storedCrc = crcBuf.getInt();
            int computedCrc = calculateCrc(payload);
            if (storedCrc != computedCrc) {
                break;
            }

            if (payload.length >= TERM_PREFIX_SIZE) {
                lastTerm = ByteBuffer.wrap(payload, 0, TERM_PREFIX_SIZE).getLong();
            }
            localPos += recordSize;
        }

        return new SegmentRecovery(localPos, lastTerm);
    }

    private int extractSegmentIndex(String filename) {
        if (!filename.startsWith("wal-") || !filename.endsWith(".bin")) {
            throw new IllegalArgumentException("Invalid WAL segment filename: " + filename);
        }
        String indexPart = filename.substring(4, filename.length() - 4);
        return Integer.parseInt(indexPart);
    }

    private long writeInternal(byte[] payload) {
        if (closed.get()) {
            throw new IllegalStateException("Writer is closed");
        }

        int payloadLen = payload.length;
        int recordSize = RECORD_OVERHEAD + payloadLen;

        // Ensure capacity BEFORE claiming position so the claim falls in the current segment
        ensureSegmentCapacity(writePosition.get() + recordSize);

        long startPos = writePosition.getAndAdd(recordSize);

        Segment seg = currentSegment;
        // If a concurrent rotation changed currentSegment between position claim
        // and volatile read, startPos may belong to the previous segment.
        // Fall back to findSegment() which walks the (immutable during write) segment list.
        if (startPos < seg.startGlobalOffset || startPos + recordSize > seg.endGlobalOffset()) {
            seg = findSegment(startPos);
            if (seg == null) {
                throw new IllegalStateException(
                    "No segment found for claimed position " + startPos);
            }
        }

        long localPos = seg.toLocal(startPos);

        MemorySegment region = seg.mappedRegion;
        var INT_UNALIGNED = java.lang.foreign.ValueLayout.JAVA_INT.withByteAlignment(1);

        region.set(INT_UNALIGNED, localPos, payloadLen);

        MemorySegment payloadSegment = MemorySegment.ofArray(payload);
        MemorySegment.copy(payloadSegment, 0, region, localPos + HEADER_SIZE, payloadLen);

        int crc = calculateCrc(payload);
        region.set(INT_UNALIGNED, localPos + HEADER_SIZE + payloadLen, crc);

        stats.recordWrite(recordSize);
        return startPos;
    }

    /** 确保当前段有足够空间，必要时轮转 */
    private void ensureSegmentCapacity(long globalEndPos) {
        Segment seg = currentSegment;
        if (globalEndPos <= seg.endGlobalOffset()) {
            return;
        }

        synchronized (this) {
            seg = currentSegment;
            if (globalEndPos <= seg.endGlobalOffset()) {
                return;
            }

            try {
                // 刷当前段
                seg.mappedRegion.force();
                // 从全局角度记录刷盘进度：当前段刷盘后最小保证至少到 segment.startGlobalOffset
                // 实际上更安全的是只记录之前 flushedPosition，段切换不改变已确认的刷盘位置

                rotateToNewSegment(seg.endGlobalOffset());
            } catch (IOException e) {
                throw new RuntimeException("Failed to rotate WAL segment", e);
            }
        }
    }

    // ========== 写入 API ==========

    public void start() {
        if (running.compareAndSet(false, true)) {
            flushThread.start();
            logger.info("SimpleWalWriter started");
        }
    }

    public CompletableFuture<Long> writeAsync(byte[] payload) {
        long position = writeInternal(payload);
        return CompletableFuture.completedFuture(position);
    }

    public CompletableFuture<Long> writeDurable(byte[] payload) {
        long position = writeInternal(payload);
        awaitFlush(position);
        return CompletableFuture.completedFuture(position);
    }

    private void awaitFlush(long position) {
        if (flushedPosition >= position) {
            return;
        }

        flushRequested = true;
        LockSupport.unpark(flushThread);

        long deadline = System.nanoTime() + 5_000_000_000L;
        while (flushedPosition < position) {
            if (System.nanoTime() > deadline) {
                throw new RuntimeException("Timeout waiting for flush, position=" + position + ", flushed=" + flushedPosition);
            }

            long remainingNs = deadline - System.nanoTime();
            if (remainingNs <= 0) break;

            try {
                boolean signaled = flushConditions.awaitNanos(
                    position, () -> flushedPosition,
                    Math.min(remainingNs, 100_000_000L));
                if (!signaled) {
                    // Re-prod the flush thread in case it went back to sleep
                    LockSupport.unpark(flushThread);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for flush", e);
            }
        }
    }

    private int calculateCrc(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return (int) crc32.getValue();
    }

    // ========== 刷盘 ==========

    private void flushLoop() {
        logger.info("Flush loop started, threshold={}KB, interval={}ms",
            flushThreshold / 1024, flushIntervalNs / 1_000_000);

        while (running.get()) {
            try {
                long currentWritePos = writePosition.get();
                long currentFlushPos = flushedPosition;
                long pendingBytes = currentWritePos - currentFlushPos;

                boolean shouldFlush = pendingBytes >= flushThreshold ||
                                      (pendingBytes > 0 && flushRequested);

                if (shouldFlush) {
                    long startNs = System.nanoTime();
                    // Flush current segment (only segment with unflushed data)
                    Segment seg = currentSegment;
                    if (seg != null) {
                        seg.mappedRegion.force();
                    }
                    long elapsedNs = System.nanoTime() - startNs;

                    flushedPosition = currentWritePos;
                    lastFsyncTimestampMs = System.currentTimeMillis();
                    flushRequested = false;

                    stats.recordFlush(elapsedNs);
                    flushConditions.signalRange(currentFlushPos, currentWritePos);

                    logger.debug("Flushed {} bytes in {} µs", pendingBytes, elapsedNs / 1000);
                } else if (pendingBytes > 0) {
                    LockSupport.parkNanos(flushIntervalNs);
                } else {
                    LockSupport.parkNanos(flushIntervalNs);
                }

            } catch (Exception e) {
                logger.error("Flush loop error", e);
                LockSupport.parkNanos(10_000_000L);
            }
        }

        logger.info("Flush loop ended");
    }

    /**
     * 删除 globalOffset 之前的所有段文件。
     * 仅在 globalOffset 已被快照覆盖后调用，调用方确保数据安全。
     */
    public synchronized void truncateBefore(long globalOffset) {
        if (globalOffset <= 0) return;

        List<Segment> toRemove = new ArrayList<>();
        for (Segment seg : segments) {
            // 保留包含 globalOffset 及之后的段
            if (seg.endGlobalOffset() <= globalOffset && seg != currentSegment) {
                toRemove.add(seg);
            }
        }

        for (Segment seg : toRemove) {
            try {
                seg.mappedRegion.force();
                seg.channel.close();
                Files.deleteIfExists(seg.path);
                segments.remove(seg);
                logger.info("Truncated WAL segment: {} (offset range {} - {})",
                    seg.path.getFileName(), seg.startGlobalOffset, seg.endGlobalOffset());
            } catch (IOException e) {
                logger.error("Failed to truncate WAL segment: {}", seg.path, e);
            }
        }
    }

    // ========== 截断 ==========

    /**
     * 将写入位置回退到 globalOffset，丢弃之后的所有数据。
     *
     * Raft 日志截断使用：删除 globalOffset 之后的所有段文件，
     * 在 globalOffset 处创建新段，重置 writePosition 和 flushedPosition。
     *
     * @param globalOffset 新的写入起始位置（必须 <= 当前 writePosition）
     */
    @Override
    public synchronized void resetTo(long globalOffset) {
        if (globalOffset < 0) {
            throw new IllegalArgumentException("globalOffset must be >= 0: " + globalOffset);
        }
        long currentPos = writePosition.get();
        if (globalOffset > currentPos) {
            throw new IllegalArgumentException(
                "Cannot reset to future position: " + globalOffset + " > " + currentPos);
        }
        if (globalOffset == currentPos) {
            return; // nothing to do
        }

        // Close and remove segments that start strictly after the truncation point.
        // The segment containing globalOffset is kept so the writer can overwrite
        // from that position without creating overlapping segments.
        List<Segment> toRemove = new ArrayList<>();
        Segment keepSegment = null;
        for (Segment seg : segments) {
            if (seg.startGlobalOffset <= globalOffset && seg.endGlobalOffset() > globalOffset) {
                if (keepSegment == null || seg.startGlobalOffset > keepSegment.startGlobalOffset) {
                    keepSegment = seg;
                }
            } else if (seg.startGlobalOffset > globalOffset) {
                toRemove.add(seg);
            }
        }
        for (Segment seg : toRemove) {
            try {
                if (seg.mappedRegion != null) {
                    seg.mappedRegion.force();
                }
                if (seg.channel != null && seg.channel.isOpen()) {
                    seg.channel.close();
                }
                Files.deleteIfExists(seg.path);
                segments.remove(seg);
                logger.info("resetTo removed segment: {} (offset range {} - {})",
                    seg.path.getFileName(), seg.startGlobalOffset, seg.endGlobalOffset());
            } catch (IOException e) {
                logger.error("Failed to remove segment {} during resetTo", seg.path, e);
            }
        }

        if (keepSegment == null) {
            for (Segment seg : segments) {
                if (seg.startGlobalOffset <= globalOffset) {
                    keepSegment = seg;
                }
            }
        }
        if (keepSegment == null) {
            throw new IllegalStateException("No WAL segment available after resetTo(" + globalOffset + ")");
        }

        long localOffset = globalOffset - keepSegment.startGlobalOffset;
        if (localOffset < 0 || localOffset > keepSegment.size) {
            throw new IllegalStateException(
                "resetTo(" + globalOffset + ") resolved invalid local offset " + localOffset +
                " for segment " + keepSegment.index);
        }
        if (localOffset < keepSegment.size) {
            keepSegment.mappedRegion.asSlice(localOffset, keepSegment.size - localOffset).fill((byte) 0);
            keepSegment.mappedRegion.force();
        }

        currentSegment = keepSegment;
        writePosition.set(globalOffset);
        flushedPosition = globalOffset;
        logger.info("WAL reset to offset {}, current segment {}", globalOffset, currentSegment.index);
    }

    private void loadRaftMeta() {
        try {
            if (Files.exists(raftMetaPath)) {
                List<String> lines = Files.readAllLines(raftMetaPath);
                if (lines.size() >= 2) {
                    currentTerm = Long.parseLong(lines.get(0));
                    votedFor = lines.get(1).isEmpty() ? null : lines.get(1);
                }
                if (lines.size() >= 3) {
                    lastLogEntryTerm = Long.parseLong(lines.get(2));
                }
                if (lines.size() >= 4) {
                    snapshotIndex = Long.parseLong(lines.get(3));
                }
                if (lines.size() >= 5) {
                    snapshotTerm = Long.parseLong(lines.get(4));
                }
                if (lines.size() >= 6) {
                    snapshotOffset = Long.parseLong(lines.get(5));
                }
                // Backward-compatible: pre-existing 2-line files leave lastLogEntryTerm at 0
            }
        } catch (Exception e) {
            logger.warn("Failed to load Raft metadata, starting fresh", e);
        }
    }

    // ========== WalAccessor 实现 ==========

    @Override
    public long getWritePosition() {
        return writePosition.get();
    }

    @Override
    public long getFlushedPosition() {
        return flushedPosition;
    }

    @Override
    public byte[] readRecord(long position, int recordLength) throws IOException {
        if (position < 0 || recordLength <= RECORD_OVERHEAD) {
            throw new IllegalArgumentException(
                "Invalid position=" + position + ", recordLength=" + recordLength);
        }

        int payloadLen = recordLength - RECORD_OVERHEAD;

        // 定位段文件
        Segment seg = findSegment(position);
        if (seg == null) {
            throw new IOException("No segment found for position " + position);
        }

        long localPos = seg.toLocal(position);
        FileChannel channel = seg.channel;

        ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE)
            .order(java.nio.ByteOrder.nativeOrder());
        int headerBytes = channel.read(headerBuf, localPos);
        if (headerBytes != HEADER_SIZE) {
            throw new IOException("Failed to read header at position " + position);
        }
        headerBuf.flip();
        int storedLen = headerBuf.getInt();
        if (storedLen != payloadLen) {
            throw new IOException(
                "Length mismatch at position " + position +
                ": stored=" + storedLen + ", expected=" + payloadLen);
        }

        ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen);
        int payloadBytes = channel.read(payloadBuf, localPos + HEADER_SIZE);
        if (payloadBytes != payloadLen) {
            throw new IOException("Failed to read payload at position " + position);
        }
        payloadBuf.flip();
        byte[] payload = new byte[payloadLen];
        payloadBuf.get(payload);

        ByteBuffer crcBuf = ByteBuffer.allocate(CHECKSUM_SIZE)
            .order(java.nio.ByteOrder.nativeOrder());
        int crcBytes = channel.read(crcBuf, localPos + HEADER_SIZE + payloadLen);
        if (crcBytes != CHECKSUM_SIZE) {
            throw new IOException("Failed to read CRC at position " + position);
        }
        crcBuf.flip();
        int storedCrc = crcBuf.getInt();
        int computedCrc = calculateCrc(payload);
        if (storedCrc != computedCrc) {
            throw new IOException(
                "CRC mismatch at position " + position +
                ": stored=0x" + Integer.toHexString(storedCrc) +
                ", computed=0x" + Integer.toHexString(computedCrc));
        }

        return payload;
    }

    @Override
    public List<WalSegment> listSegments() {
        List<WalSegment> result = new ArrayList<>(segments.size());
        for (Segment seg : segments) {
            result.add(new WalSegment(
                seg.index, seg.path,
                seg.startGlobalOffset, seg.endGlobalOffset(),
                seg.size));
        }
        return Collections.unmodifiableList(result);
    }

    /** 根据全局偏移查找所在段 */
    private Segment findSegment(long globalPos) {
        for (Segment seg : segments) {
            if (globalPos >= seg.startGlobalOffset && globalPos < seg.endGlobalOffset()) {
                return seg;
            }
        }
        return null;
    }

    // ========== 健康状态 ==========

    public Stats getStats() {
        return stats;
    }

    public long getUnflushedBytes() {
        return writePosition.get() - flushedPosition;
    }

    public long getLastFsyncMsAgo() {
        long last = lastFsyncTimestampMs;
        return last > 0 ? System.currentTimeMillis() - last : 0;
    }

    public String getWalHealth() {
        long unflushed = getUnflushedBytes();
        long ago = getLastFsyncMsAgo();
        if (unflushed > flushThreshold * 3 || ago > 5000) return "CRITICAL";
        if (unflushed > flushThreshold || ago > flushIntervalNs / 1_000_000 * 3) return "WARNING";
        return "OK";
    }

    // ========== Raft 元数据 ==========

    private void saveRaftMeta() {
        Path tmpPath = raftMetaPath.resolveSibling(raftMetaPath.getFileName() + ".tmp");
        try {
            Files.writeString(tmpPath,
                currentTerm + "\n" + (votedFor != null ? votedFor : "") + "\n" +
                lastLogEntryTerm + "\n" + snapshotIndex + "\n" + snapshotTerm + "\n" + snapshotOffset + "\n");
            Files.move(tmpPath, raftMetaPath,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            logger.error("Failed to save Raft metadata", e);
            try { Files.deleteIfExists(tmpPath); } catch (IOException ignored) {}
        }
    }

    @Override
    public long getLastLogTerm() { return currentTerm; }

    @Override
    public void setCurrentTerm(long term) {
        this.currentTerm = term;
    }

    @Override
    public void setTermAndVotedFor(long term, String votedFor) {
        this.currentTerm = term;
        this.votedFor = votedFor;
    }

    @Override
    public String getVotedFor() { return votedFor; }

    @Override
    public void setVotedFor(String nodeId) {
        this.votedFor = nodeId;
    }

    @Override
    public long writeEntry(byte[] data) {
        long startPos = writeAsync(data).join();
        int recordLen = RECORD_FIXED_OVERHEAD + data.length;
        // Extract term from the Raft-framed data (first 8 bytes = term as big-endian long)
        if (data.length >= 8) {
            lastLogEntryTerm = java.nio.ByteBuffer.wrap(data, 0, 8).getLong();
        }
        return startPos + recordLen; // Return end position = log index (caller doesn't need to know record format)
    }

    @Override
    public long getLastLogEntryTerm() {
        return lastLogEntryTerm;
    }

    @Override
    public long getSnapshotIndex() {
        return snapshotIndex;
    }

    @Override
    public long getSnapshotTerm() {
        return snapshotTerm;
    }

    @Override
    public long getSnapshotOffset() {
        return snapshotOffset;
    }

    @Override
    public synchronized void setSnapshotMetadata(long snapshotIndex, long snapshotTerm) {
        setSnapshotMetadata(snapshotIndex, snapshotTerm, getWritePosition());
    }

    @Override
    public synchronized void setSnapshotMetadata(long snapshotIndex, long snapshotTerm, long snapshotOffset) {
        this.snapshotIndex = Math.max(0, snapshotIndex);
        this.snapshotTerm = Math.max(0, snapshotTerm);
        this.snapshotOffset = Math.max(0, snapshotOffset);
        saveRaftMeta();
    }

    /**
     * 同步持久化 Raft 元数据（currentTerm、votedFor）到磁盘。
     *
     * Raft §5.2 要求：节点在回复任何 RPC 之前，必须确保持久化 currentTerm 和 votedFor。
     * saveRaftMeta 写入原子临时文件后 rename，保证调用返回后元数据已落盘。
     */
    @Override
    public void persistRaftMeta() {
        saveRaftMeta();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        logger.info("Closing SimpleWalWriter...");

        running.set(false);
        flushConditions.signalAllStripes();
        flushThread.interrupt();

        try {
            flushThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 最终刷盘所有段
        for (Segment seg : segments) {
            try {
                if (seg.mappedRegion != null) {
                    seg.mappedRegion.force();
                }
            } catch (Exception e) {
                logger.error("Final flush failed for segment {}", seg.path, e);
            }
        }

        // 关闭所有段
        for (Segment seg : segments) {
            try {
                if (seg.channel != null && seg.channel.isOpen()) {
                    seg.channel.close();
                }
            } catch (IOException e) {
                logger.error("Close channel failed for segment {}", seg.path, e);
            }
        }

        arena.close();

        // 最后同步保存 Raft 元数据保证最新状态落盘
        saveRaftMeta();

        logger.info("SimpleWalWriter closed, writes={}, flushes={}",
            stats.getWriteCount(), stats.getFlushCount());
    }

    // ========== 关闭 ==========

    private static class SegmentRecovery {
        final long usedBytes;
        final long lastEntryTerm;

        SegmentRecovery(long usedBytes, long lastEntryTerm) {
            this.usedBytes = usedBytes;
            this.lastEntryTerm = lastEntryTerm;
        }
    }
}
