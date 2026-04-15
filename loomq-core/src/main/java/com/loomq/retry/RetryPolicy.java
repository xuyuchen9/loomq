package com.loomq.retry;

/**
 * 重试策略接口
 *
 * 定义任务执行失败后的重试行为。
 *
 * @author loomq
 * @since v0.4
 */
public interface RetryPolicy {

    /**
     * 计算下次重试的延迟时间
     *
     * @param retryCount 当前重试次数（从1开始）
     * @return 下次重试的延迟毫秒数，-1 表示不再重试
     */
    long nextRetryDelay(int retryCount);

    /**
     * 获取最大重试次数
     */
    int maxRetries();

    /**
     * 获取策略名称
     */
    String name();

    /**
     * 判断是否还可以重试
     */
    default boolean canRetry(int retryCount) {
        return retryCount < maxRetries();
    }
}
