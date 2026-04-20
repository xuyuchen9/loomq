package com.loomq.spi;

import com.loomq.domain.intent.Intent;

/**
 * 投递处理器 SPI 接口
 *
 * 内核通过此接口投递 Intent，具体实现由壳子提供。
 * 内核不关心投递方式（HTTP、消息队列、本地回调等）。
 *
 * 设计原则：
 * - core 模块只定义接口，不提供实现
 * - server 模块提供 HTTP 投递实现
 * - 嵌入式场景可提供本地回调实现
 *
 * @author loomq
 * @since v0.7.1
 */
@FunctionalInterface
public interface DeliveryHandler {

    /**
     * 投递 Intent
     *
     * @param intent 待投递的 Intent
     * @return 投递结果
     */
    DeliveryResult deliver(Intent intent);

    /**
     * 投递结果
     */
    enum DeliveryResult {
        /**
         * 投递成功
         * Intent 应转换为 DELIVERED -> ACKED 状态
         */
        SUCCESS,

        /**
         * 投递失败，需要重试
         * Intent 应调度重投
         */
        RETRY,

        /**
         * 投递失败，进入死信队列
         * Intent 应转换为 DEAD_LETTERED 状态
         */
        DEAD_LETTER,

        /**
         * 投递失败，任务已过期
         * Intent 应转换为 EXPIRED 状态
         */
        EXPIRED
    }
}
