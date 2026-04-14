package com.loomq.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重试策略测试
 *
 * @author loomq
 * @since v0.4
 */
class RetryPolicyTest {

    @Test
    @DisplayName("固定间隔策略：基本功能")
    void testFixedIntervalPolicy() {
        FixedIntervalPolicy policy = new FixedIntervalPolicy(3, 5000);

        assertEquals(3, policy.maxRetries());
        assertEquals(5000, policy.getIntervalMs());
        assertEquals("fixed", policy.name());

        // 验证每次重试延迟相同
        // maxRetries=3 意味着最多重试3次，retryCount 0,1,2 可以重试
        assertEquals(5000, policy.nextRetryDelay(0));
        assertEquals(5000, policy.nextRetryDelay(1));
        assertEquals(5000, policy.nextRetryDelay(2));

        // 超过最大重试次数
        assertEquals(-1, policy.nextRetryDelay(3));
        assertEquals(-1, policy.nextRetryDelay(4));
    }

    @Test
    @DisplayName("固定间隔策略：canRetry 判断")
    void testFixedIntervalCanRetry() {
        FixedIntervalPolicy policy = new FixedIntervalPolicy(3, 1000);

        assertTrue(policy.canRetry(0));
        assertTrue(policy.canRetry(1));
        assertTrue(policy.canRetry(2));
        assertFalse(policy.canRetry(3));
        assertFalse(policy.canRetry(4));
    }

    @Test
    @DisplayName("固定间隔策略：默认策略")
    void testFixedIntervalDefault() {
        FixedIntervalPolicy policy = FixedIntervalPolicy.defaultPolicy();

        assertEquals(3, policy.maxRetries());
        assertEquals(5000, policy.getIntervalMs());
    }

    @Test
    @DisplayName("固定间隔策略：参数校验")
    void testFixedIntervalValidation() {
        // 负数重试次数
        assertThrows(IllegalArgumentException.class, () -> new FixedIntervalPolicy(-1, 1000));

        // 负数间隔
        assertThrows(IllegalArgumentException.class, () -> new FixedIntervalPolicy(3, -1));

        // 合法参数
        assertDoesNotThrow(() -> new FixedIntervalPolicy(0, 0));
    }

    @Test
    @DisplayName("指数退避策略：基本功能")
    void testExponentialBackoffPolicy() {
        ExponentialBackoffPolicy policy = new ExponentialBackoffPolicy(5, 1000, 2.0, 60000);

        assertEquals(5, policy.maxRetries());
        assertEquals(1000, policy.getInitialDelayMs());
        assertEquals(2.0, policy.getBase());
        assertEquals(60000, policy.getMaxDelayMs());
        assertEquals("exponential", policy.name());
    }

    @Test
    @DisplayName("指数退避策略：延迟计算")
    void testExponentialBackoffDelay() {
        ExponentialBackoffPolicy policy = new ExponentialBackoffPolicy(5, 1000, 2.0, 60000);

        // delay = initialDelay * base^retryCount
        // maxRetries=5 意味着最多重试5次，retryCount 0,1,2,3,4 可以重试
        // retry 0: 1000 * 2^0 = 1000
        // retry 1: 1000 * 2^1 = 2000
        // retry 2: 1000 * 2^2 = 4000
        // retry 3: 1000 * 2^3 = 8000
        // retry 4: 1000 * 2^4 = 16000

        assertEquals(1000, policy.nextRetryDelay(0));
        assertEquals(2000, policy.nextRetryDelay(1));
        assertEquals(4000, policy.nextRetryDelay(2));
        assertEquals(8000, policy.nextRetryDelay(3));
        assertEquals(16000, policy.nextRetryDelay(4));

        // 超过最大重试次数
        assertEquals(-1, policy.nextRetryDelay(5));
        assertEquals(-1, policy.nextRetryDelay(6));
    }

    @Test
    @DisplayName("指数退避策略：最大延迟限制")
    void testExponentialBackoffMaxDelay() {
        // 设置较小的最大延迟
        ExponentialBackoffPolicy policy = new ExponentialBackoffPolicy(10, 1000, 2.0, 5000);

        // 延迟应该被限制在 5000ms
        // delay = initialDelay * base^retryCount
        assertEquals(1000, policy.nextRetryDelay(0));  // 1000
        assertEquals(2000, policy.nextRetryDelay(1));  // 2000
        assertEquals(4000, policy.nextRetryDelay(2));  // 4000
        assertEquals(5000, policy.nextRetryDelay(3));  // 8000 被限制为 5000
        assertEquals(5000, policy.nextRetryDelay(4));  // 16000 被限制为 5000
    }

    @Test
    @DisplayName("指数退避策略：默认策略")
    void testExponentialBackoffDefault() {
        ExponentialBackoffPolicy policy = ExponentialBackoffPolicy.defaultPolicy();

        assertEquals(5, policy.maxRetries());
        assertEquals(1000, policy.getInitialDelayMs());
        assertEquals(2.0, policy.getBase());
        assertEquals(60000, policy.getMaxDelayMs());
    }

    @Test
    @DisplayName("指数退避策略：参数校验")
    void testExponentialBackoffValidation() {
        // 负数重试次数
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBackoffPolicy(-1, 1000, 2.0, 60000));

        // 非正初始延迟
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBackoffPolicy(5, 0, 2.0, 60000));
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBackoffPolicy(5, -1, 2.0, 60000));

        // 基数 <= 1
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBackoffPolicy(5, 1000, 1.0, 60000));
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBackoffPolicy(5, 1000, 0.5, 60000));

        // 最大延迟小于初始延迟
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBackoffPolicy(5, 1000, 2.0, 500));

        // 合法参数
        assertDoesNotThrow(() -> new ExponentialBackoffPolicy(0, 1, 1.1, 1));
    }

    @Test
    @DisplayName("重试策略工厂：创建固定间隔策略")
    void testFactoryCreateFixed() {
        RetryPolicy policy = RetryPolicyFactory.create(
                RetryPolicyFactory.PolicyType.FIXED,
                3, 5000, 1000, 2.0, 60000
        );

        assertTrue(policy instanceof FixedIntervalPolicy);
        assertEquals(3, policy.maxRetries());
        assertEquals(5000, ((FixedIntervalPolicy) policy).getIntervalMs());
    }

    @Test
    @DisplayName("重试策略工厂：创建指数退避策略")
    void testFactoryCreateExponential() {
        RetryPolicy policy = RetryPolicyFactory.create(
                RetryPolicyFactory.PolicyType.EXPONENTIAL,
                5, 5000, 1000, 2.0, 60000
        );

        assertTrue(policy instanceof ExponentialBackoffPolicy);
        assertEquals(5, policy.maxRetries());
        assertEquals(1000, ((ExponentialBackoffPolicy) policy).getInitialDelayMs());
    }

    @Test
    @DisplayName("重试配置：默认配置")
    void testRetryConfigDefault() {
        RetryPolicyFactory.RetryConfig config = RetryPolicyFactory.RetryConfig.defaultConfig();

        assertEquals(RetryPolicyFactory.PolicyType.FIXED, config.type());
        assertEquals(3, config.maxRetries());
        assertEquals(5000, config.fixedIntervalMs());
    }

    @Test
    @DisplayName("重试配置：固定间隔快捷创建")
    void testRetryConfigFixed() {
        RetryPolicyFactory.RetryConfig config = RetryPolicyFactory.RetryConfig.fixed(5, 10000);

        assertEquals(RetryPolicyFactory.PolicyType.FIXED, config.type());
        assertEquals(5, config.maxRetries());
        assertEquals(10000, config.fixedIntervalMs());
    }

    @Test
    @DisplayName("重试配置：指数退避快捷创建")
    void testRetryConfigExponential() {
        RetryPolicyFactory.RetryConfig config = RetryPolicyFactory.RetryConfig.exponential(5, 500, 2.0, 30000);

        assertEquals(RetryPolicyFactory.PolicyType.EXPONENTIAL, config.type());
        assertEquals(5, config.maxRetries());
        assertEquals(500, config.initialDelayMs());
        assertEquals(2.0, config.base());
        assertEquals(30000, config.maxDelayMs());
    }
}
