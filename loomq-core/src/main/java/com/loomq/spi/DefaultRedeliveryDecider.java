package com.loomq.spi;

/**
 * 默认重投决策器
 *
 * 基于 HTTP 状态码和异常类型的默认实现
 *
 * @author loomq
 * @since v0.5.0
 */
public class DefaultRedeliveryDecider implements RedeliveryDecider {

    @Override
    public boolean shouldRedeliver(DeliveryContext context) {
        // 1. 如果有明确的成功响应，不重投
        if (context.isSuccess()) {
            return false;
        }

        // 2. 超时/连接错误 → 重投
        if (context.isTimeout() || context.isConnectionError()) {
            return true;
        }

        // 3. 服务器错误 (5xx) → 重投
        if (context.isServerError()) {
            return true;
        }

        // 4. 客户端错误 (4xx) → 不重投（业务错误）
        if (context.isClientError()) {
            return false;
        }

        // 5. 其他情况（如未知异常）→ 重投
        if (context.hasException()) {
            return true;
        }

        // 6. 默认不重投
        return false;
    }

    @Override
    public String getName() {
        return "DefaultRedeliveryDecider";
    }
}
