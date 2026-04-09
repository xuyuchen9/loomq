package com.loomq.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

/**
 * 无锁环形缓冲区（MPSC 模型）
 *
 * 设计要点：
 * 1. 多生产者单消费者（MPSC）
 * 2. 基于序列号（Sequence）的并发控制
 * 3. 2的幂次方容量，使用位运算替代取模
 * 4. 批量消费支持（drain）
 *
 * 性能目标：
 * - 单线程写入：> 100万 ops/s
 * - 批量消费：最小化锁竞争
 *
 * @param <T> 元素类型
 * @author loomq
 * @since v0.3
 */
public class RingBuffer<T> {

    private static final Logger logger = LoggerFactory.getLogger(RingBuffer.class);

    // 默认容量：16K（经验值，可根据场景调整）
    public static final int DEFAULT_CAPACITY = 16384;

    // 容量（2的幂次方）
    private final int capacity;

    // 掩码（用于位运算替代取模）
    private final long mask;

    // 环形数组
    private final AtomicReferenceArray<T> buffer;

    // 生产者序列（多线程并发写入）
    private final AtomicLong writeSequence;

    // 消费者序列（单线程消费）
    private final AtomicLong readSequence;

    // 统计信息
    private final Stats stats;

    /**
     * 创建默认容量的 RingBuffer
     */
    public RingBuffer() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * 创建指定容量的 RingBuffer
     *
     * @param capacity 容量（必须是2的幂次方）
     */
    public RingBuffer(int capacity) {
        // 确保容量是2的幂次方
        if (capacity <= 0 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException(
                    "Capacity must be a power of 2, got: " + capacity);
        }

        this.capacity = capacity;
        this.mask = capacity - 1;
        this.buffer = new AtomicReferenceArray<>(capacity);
        this.writeSequence = new AtomicLong(-1);  // 起始为 -1，第一次写入后为 0
        this.readSequence = new AtomicLong(-1);
        this.stats = new Stats();

        logger.info("RingBuffer created, capacity: {}", capacity);
    }

    /**
     * 写入元素（生产者调用）
     *
     * @param item 要写入的元素
     * @return true 如果写入成功，false 如果缓冲区已满
     */
    public boolean offer(T item) {
        if (item == null) {
            throw new NullPointerException("Item cannot be null");
        }

        long currentWrite;
        long nextWrite;

        do {
            currentWrite = writeSequence.get();
            nextWrite = currentWrite + 1;

            // 检查是否满（未读元素数 >= 容量）
            long wrapPoint = nextWrite - capacity;
            long currentRead = readSequence.get();

            if (wrapPoint > currentRead) {
                // 缓冲区已满
                stats.recordOverflow();
                return false;
            }

        } while (!writeSequence.compareAndSet(currentWrite, nextWrite));

        // 写入数组（序列号已获取，可以放心写入）
        int index = (int) (nextWrite & mask);
        buffer.lazySet(index, item);

        stats.recordWrite();
        return true;
    }

    /**
     * 写入元素（带超时等待）
     *
     * @param item 要写入的元素
     * @param timeout 超时时间
     * @param unit 时间单位
     * @return true 如果写入成功，false 如果超时
     */
    public boolean offerWithTimeout(T item, long timeout, TimeUnit unit) {
        if (item == null) {
            throw new NullPointerException("Item cannot be null");
        }

        long deadline = System.nanoTime() + unit.toNanos(timeout);

        while (System.nanoTime() < deadline) {
            if (offer(item)) {
                return true;
            }

            // 短暂自旋等待
            Thread.yield();
        }

        // 超时
        return false;
    }

    /**
     * 读取单个元素（消费者调用）
     *
     * @return 元素，如果没有则返回 null
     */
    public T poll() {
        long currentRead = readSequence.get() + 1;
        long currentWrite = writeSequence.get();

        // 检查是否有可读数据
        if (currentRead > currentWrite) {
            return null;  // 缓冲区为空
        }

        int index = (int) (currentRead & mask);
        T item = buffer.get(index);

        if (item == null) {
            // 生产者尚未写入（理论上不应该发生，因为序列号已保证）
            return null;
        }

        // 清空该位置
        buffer.lazySet(index, null);
        readSequence.lazySet(currentRead);

        stats.recordRead(1);
        return item;
    }

    /**
     * 批量消费元素（高效，推荐用法）
     *
     * @param consumer 元素消费者
     * @param maxBatchSize 最大批量大小
     * @return 实际消费的数量
     */
    public int drain(Consumer<T> consumer, int maxBatchSize) {
        if (maxBatchSize <= 0) {
            return 0;
        }

        long currentRead = readSequence.get();
        long currentWrite = writeSequence.get();

        // 计算可读数量
        long available = currentWrite - currentRead;
        if (available <= 0) {
            return 0;
        }

        int batchSize = (int) Math.min(available, maxBatchSize);

        for (int i = 0; i < batchSize; i++) {
            long sequence = currentRead + i + 1;
            int index = (int) (sequence & mask);

            T item = buffer.get(index);
            if (item == null) {
                // 生产者尚未写入，停止消费
                batchSize = i;
                break;
            }

            // 消费元素
            consumer.accept(item);

            // 清空该位置
            buffer.lazySet(index, null);
        }

        // 更新读序列
        if (batchSize > 0) {
            readSequence.lazySet(currentRead + batchSize);
            stats.recordRead(batchSize);
        }

        return batchSize;
    }

    /**
     * 批量消费并收集到列表
     *
     * @param maxBatchSize 最大批量大小
     * @return 收集到的元素列表
     */
    public List<T> drainToList(int maxBatchSize) {
        List<T> result = new ArrayList<>(maxBatchSize);
        drain(result::add, maxBatchSize);
        return result;
    }

    /**
     * 获取当前元素数量（近似值）
     *
     * @return 元素数量
     */
    public int size() {
        long write = writeSequence.get();
        long read = readSequence.get();
        return (int) Math.max(0, write - read);
    }

    /**
     * 检查是否为空
     *
     * @return true 如果为空
     */
    public boolean isEmpty() {
        return writeSequence.get() == readSequence.get();
    }

    /**
     * 检查是否已满
     *
     * @return true 如果已满
     */
    public boolean isFull() {
        return size() >= capacity;
    }

    /**
     * 获取容量
     *
     * @return 容量
     */
    public int capacity() {
        return capacity;
    }

    /**
     * 获取剩余空间
     *
     * @return 剩余空间
     */
    public int remaining() {
        return capacity - size();
    }

    /**
     * 清空缓冲区
     */
    public void clear() {
        drain(item -> {}, capacity);
        stats.reset();
    }

    /**
     * 获取统计信息
     *
     * @return 统计信息
     */
    public Stats getStats() {
        return stats;
    }

    /**
     * 统计信息（静态内部类）
     */
    public static class Stats {
        private long writeCount = 0;
        private long readCount = 0;
        private long overflowCount = 0;

        synchronized void recordWrite() {
            writeCount++;
        }

        synchronized void recordRead(int count) {
            readCount += count;
        }

        synchronized void recordOverflow() {
            overflowCount++;
        }

        synchronized void reset() {
            writeCount = 0;
            readCount = 0;
            overflowCount = 0;
        }

        public synchronized long getWriteCount() {
            return writeCount;
        }

        public synchronized long getReadCount() {
            return readCount;
        }

        public synchronized long getOverflowCount() {
            return overflowCount;
        }

        @Override
        public synchronized String toString() {
            return String.format("Stats{writes=%d, reads=%d, overflows=%d}",
                    writeCount, readCount, overflowCount);
        }
    }
}
