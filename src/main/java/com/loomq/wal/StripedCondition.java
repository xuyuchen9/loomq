package com.loomq.wal;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * 分段条件变量 - 用于高效等待/通知
 *
 * 核心设计：
 * 1. 按 position 分段，减少锁竞争
 * 2. 支持 [startPos, endPos] 范围唤醒
 * 3. 零对象分配（AQS 节点复用）
 * 4. 虚拟线程友好（ReentrantLock 不 pin）
 * 5. 精确唤醒（只唤醒有等待者的分段）
 *
 * @author loomq
 * @since v0.6.0
 */
final class StripedCondition {

    private final Lock[] locks;
    private final Condition[] conditions;
    private final int mask;
    private final int stripeCount;

    // 等待者位图（精确唤醒）
    private final java.util.concurrent.atomic.AtomicLongArray waitingStripes;
    private final java.util.concurrent.atomic.AtomicLong totalWaiters;

    /**
     * 创建分段条件变量
     * @param stripes 分段数（会向上取整为 2 的幂）
     */
    StripedCondition(int stripes) {
        // 向上取整为 2 的幂，便于位运算
        int size = 1;
        while (size < stripes) size <<= 1;
        this.stripeCount = size;
        this.mask = size - 1;

        this.locks = new ReentrantLock[size];
        this.conditions = new Condition[size];

        // 初始化等待者位图（每个 long 可表示 64 个分段）
        int bitmapSize = (size + 63) / 64;
        this.waitingStripes = new java.util.concurrent.atomic.AtomicLongArray(bitmapSize);
        this.totalWaiters = new java.util.concurrent.atomic.AtomicLong(0);

        for (int i = 0; i < size; i++) {
            locks[i] = new ReentrantLock();
            conditions[i] = locks[i].newCondition();
        }
    }

    /**
     * 等待刷盘完成
     *
     * @param position 等待的刷盘位置
     * @param flushedPositionSupplier 获取最新刷盘位置的函数
     * @return 刷盘完成后的 flushedPosition
     */
    long await(long position, LongSupplier flushedPositionSupplier) throws InterruptedException {
        // 快速路径：已刷盘
        long currentFlushed = flushedPositionSupplier.getAsLong();
        if (currentFlushed >= position) {
            return currentFlushed;
        }

        int idx = stripeIndex(position);
        Lock lock = locks[idx];

        lock.lock();
        try {
            // 再次检查条件（在锁内，避免竞态）
            if ((currentFlushed = flushedPositionSupplier.getAsLong()) >= position) {
                return currentFlushed;
            }

            // 标记该分段有等待者（在锁内，确保不会丢失信号）
            markWaiting(idx, true);

            try {
                // 循环检查条件，直到满足
                while ((currentFlushed = flushedPositionSupplier.getAsLong()) < position) {
                    conditions[idx].await();
                }
                return currentFlushed;
            } finally {
                // 清除等待者标记
                markWaiting(idx, false);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 带超时的等待刷盘完成
     *
     * @param position 等待的刷盘位置
     * @param flushedPositionSupplier 获取最新刷盘位置的函数
     * @param timeoutNanos 超时时间（纳秒）
     * @return true 如果收到信号，false 如果超时
     */
    boolean awaitNanos(long position, LongSupplier flushedPositionSupplier, long timeoutNanos) throws InterruptedException {
        // 快速路径：已刷盘
        long currentFlushed = flushedPositionSupplier.getAsLong();
        if (currentFlushed >= position) {
            return true;
        }

        int idx = stripeIndex(position);
        Lock lock = locks[idx];

        lock.lock();
        try {
            // 再次检查条件
            if ((currentFlushed = flushedPositionSupplier.getAsLong()) >= position) {
                return true;
            }

            // 标记等待者
            markWaiting(idx, true);

            try {
                long remaining = timeoutNanos;
                while ((currentFlushed = flushedPositionSupplier.getAsLong()) < position) {
                    if (remaining <= 0) {
                        return false; // 超时
                    }
                    remaining = conditions[idx].awaitNanos(remaining);
                }
                return true; // 条件满足
            } finally {
                markWaiting(idx, false);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 范围唤醒（优化版 - 只唤醒有等待者的分段）
     *
     * 由于 position 单调递增，简化为遍历所有分段检查是否有等待者
     *
     * @param startPos 起始位置
     * @param endPos 结束位置
     */
    void signalRange(long startPos, long endPos) {
        // 快速路径：无等待者
        if (totalWaiters.get() == 0) {
            return;
        }

        // 遍历所有分段，唤醒有等待者的分段
        for (int i = 0; i < stripeCount; i++) {
            if (hasWaiter(i)) {
                signalAll(i);
            }
        }
    }

    /**
     * 唤醒所有分段的等待者
     *
     * 用于关闭时确保所有等待者被唤醒
     */
    void signalAllStripes() {
        for (int i = 0; i < stripeCount; i++) {
            signalAll(i);
        }
    }

    private void signalAll(int idx) {
        Lock lock = locks[idx];
        lock.lock();
        try {
            conditions[idx].signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 标记/清除等待者
     */
    private void markWaiting(int idx, boolean waiting) {
        int wordIdx = idx / 64;
        int bitIdx = idx % 64;
        long bitMask = 1L << bitIdx;

        if (waiting) {
            // 设置位
            waitingStripes.getAndAccumulate(wordIdx, bitMask, (old, m) -> old | m);
            totalWaiters.incrementAndGet();
        } else {
            // 清除位
            waitingStripes.getAndAccumulate(wordIdx, ~bitMask, (old, m) -> old & m);
            totalWaiters.decrementAndGet();
        }
    }

    /**
     * 检查分段是否有等待者
     */
    private boolean hasWaiter(int idx) {
        int wordIdx = idx / 64;
        int bitIdx = idx % 64;
        long bitMask = 1L << bitIdx;
        return (waitingStripes.get(wordIdx) & bitMask) != 0;
    }

    private int stripeIndex(long position) {
        return (int) (position & mask);
    }

    /**
     * 获取分段数
     */
    int getStripeCount() {
        return stripeCount;
    }

    /**
     * 获取当前等待者数量
     */
    long getWaiterCount() {
        return totalWaiters.get();
    }
}
