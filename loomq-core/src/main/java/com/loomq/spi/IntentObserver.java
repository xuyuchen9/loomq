package com.loomq.spi;

import com.loomq.domain.intent.Intent;
import com.loomq.spi.DeliveryHandler.DeliveryResult;

/**
 * Intent 生命周期观察器。
 *
 * 服务层（复制、锁服务、agent 调度器等）通过实现此接口，
 * 在不侵入内核调度逻辑的前提下观测 intent 状态变化。
 *
 * 注意：回调在调度线程中执行，观察器实现应保持轻量，
 * 避免同步阻塞操作。如需执行重操作，应异步分派。
 */
public interface IntentObserver {

    /** Intent 进入 SCHEDULED 状态 */
    void onScheduled(Intent intent);

    /** Intent 投递成功 */
    void onDelivered(Intent intent, DeliveryResult result);

    /** Intent 进入死信队列 */
    void onDeadLettered(Intent intent);

    /** Intent 过期 */
    void onExpired(Intent intent);

    /** Intent 投递异常 */
    void onDeliveryFailed(Intent intent, Throwable error);
}
