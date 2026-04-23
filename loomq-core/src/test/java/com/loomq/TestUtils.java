package com.loomq.test;

import com.loomq.config.WalConfig;

import java.nio.file.Path;
import java.util.Random;

/**
 * 测试工具类 - 提供通用的测试辅助方法
 */
public final class TestUtils {

    private static final Random RANDOM = new Random();

    private TestUtils() {}

    /**
     * 创建测试用的 WalConfig
     */
    public static WalConfig createWalConfig(Path dataDir, long batchFlushIntervalMs) {
        return WalConfig.defaultConfig()
                .withDataDir(dataDir.toString())
                .withBatchFlushIntervalMs(batchFlushIntervalMs);
    }

    /**
     * 创建快速刷盘的 WalConfig (用于测试)
     */
    public static WalConfig createFastFlushWalConfig(Path dataDir) {
        return createWalConfig(dataDir, 10); // 10ms 刷盘间隔
    }

    /**
     * 创建延迟刷盘的 WalConfig (用于积压测试)
     */
    public static WalConfig createSlowFlushWalConfig(Path dataDir) {
        return createWalConfig(dataDir, 1000); // 1s 刷盘间隔
    }

    /**
     * 生成随机字节数组
     */
    public static byte[] randomBytes(int size) {
        byte[] bytes = new byte[size];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    /**
     * 生成随机 intentId
     */
    public static String randomIntentId() {
        return "intent-" + System.nanoTime() + "-" + RANDOM.nextInt(10000);
    }

    /**
     * 生成测试用的 payload
     */
    public static byte[] testPayload(String prefix) {
        return (prefix + "-" + System.currentTimeMillis()).getBytes();
    }

    /**
     * 等待条件满足或超时
     */
    public static boolean awaitCondition(java.util.function.BooleanSupplier condition,
                                          long timeoutMs, long pollIntervalMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }
}
