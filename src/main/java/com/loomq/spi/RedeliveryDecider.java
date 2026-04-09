package com.loomq.spi;

/**
 * 重投决策器 SPI 接口
 *
 * 允许用户通过实现此接口自定义是否继续重投。
 * 核心引擎不包含任何业务分支判断。
 *
 * @author loomq
 * @since v0.5.0
 */
@FunctionalInterface
public interface RedeliveryDecider {

    /**
     * 决定当前 Delivery 是否应该继续重投。
     *
     * @param context 投递上下文，包含 HTTP 状态、响应头、异常信息等。
     * @return true 继续重投，false 停止并进入终态（如 DEAD_LETTERED）。
     */
    boolean shouldRedeliver(DeliveryContext context);

    /**
     * 获取决策器名称
     */
    default String getName() {
        return getClass().getSimpleName();
    }
}
