package com.loomq.wal;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 刷盘代 - 用于 Group Commit
 *
 * 第一性原理设计：
 * 1. DURABLE 写入的本质：写入内存 + 等待刷盘完成
 * 2. 多个等待者可以共享同一个刷盘周期
 * 3. 用"代"标识刷盘周期，等待者等待代完成
 *
 * @author loomq
 * @since v0.6.0
 */
public class FlushEpoch {

    // 当前代 ID（每次刷盘后递增）
    private final AtomicLong currentEpoch = new AtomicLong(0);

    // 已完成的代 ID
    private volatile long completedEpoch = 0;

    // 条件变量用于唤醒等待者
    private final Lock lock = new ReentrantLock();
    private final Condition epochChanged = lock.newCondition();

    /**
     * 获取当前代 ID
     * 写入者在写入数据前调用，用于后续等待
     */
    public long currentEpoch() {
        return currentEpoch.get();
    }

    /**
     * 等待指定代完成
     *
     * @param epoch 等待的代 ID
     * @return 实际完成的代 ID
     */
    public long awaitEpoch(long epoch) throws InterruptedException {
        // 快速路径：代已完成
        if (completedEpoch > epoch) {
            return completedEpoch;
        }

        lock.lock();
        try {
            // 再次检查（在锁内）
            while (completedEpoch <= epoch) {
                epochChanged.await();
            }
            return completedEpoch;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 开启新一代（刷盘线程调用）
     *
     * @return 新的代 ID
     */
    public long startNewEpoch() {
        return currentEpoch.incrementAndGet();
    }

    /**
     * 完成当前代（刷盘线程调用）
     *
     * @param epoch 完成的代 ID
     */
    public void completeEpoch(long epoch) {
        completedEpoch = epoch;

        // 唤醒所有等待者
        lock.lock();
        try {
            epochChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取已完成的代 ID
     */
    public long completedEpoch() {
        return completedEpoch;
    }
}
