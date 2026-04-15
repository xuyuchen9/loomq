package com.loomq.embedded;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

/**
 * LoomQ v0.7.0 嵌入式使用 Demo
 *
 * 演示如何在应用内直接嵌入 loomq-core，无需 HTTP 服务。
 *
 * @author loomq
 * @since v0.7.0
 */
public class EmbeddedDemo {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedDemo.class);

    public static void main(String[] args) throws Exception {
        logger.info("╔════════════════════════════════════════════════════════╗");
        logger.info("║       LoomQ v0.7.0 Embedded Mode Demo                  ║");
        logger.info("║       嵌入式内核使用示例                                ║");
        logger.info("╚════════════════════════════════════════════════════════╝");

        // 创建 IntentStore（内核核心组件）
        IntentStore intentStore = new IntentStore();

        // 演示：创建和存储 Intent
        demoCreateIntent(intentStore);

        // 演示：查询 Intent
        demoQueryIntent(intentStore);

        logger.info("\n✅ EmbeddedDemo 完成！");
        logger.info("   Core 模块可以独立使用，依赖极少");
    }

    /**
     * 创建 Intent 示例
     */
    private static void demoCreateIntent(IntentStore intentStore) {
        logger.info("\n📝 [Demo 1] 创建 Intent");

        // 创建 Intent（使用带 ID 的构造函数）
        Intent intent = new Intent("order:timeout:12345");
        intent.setExecuteAt(Instant.now().plus(Duration.ofMinutes(30)));
        intent.setPrecisionTier(PrecisionTier.STANDARD);

        // 保存 Intent
        intentStore.save(intent);

        logger.info("   创建 Intent: {}", intent.getIntentId());
        logger.info("   执行时间: {}", intent.getExecuteAt());
        logger.info("   精度档位: {} ({}ms 窗口)",
            intent.getPrecisionTier(),
            intent.getPrecisionTier().getPrecisionWindowMs());
    }

    /**
     * 查询 Intent 示例
     */
    private static void demoQueryIntent(IntentStore intentStore) {
        logger.info("\n🔍 [Demo 2] 查询 Intent 状态");

        // 查询指定 Intent
        Intent found = intentStore.findById("order:timeout:12345");
        if (found != null) {
            logger.info("   找到 Intent: {}", found.getIntentId());
            logger.info("   状态: {}", found.getStatus());
            logger.info("   剩余时间: {}ms",
                Duration.between(Instant.now(), found.getExecuteAt()).toMillis());
        }

        // 统计信息
        logger.info("   IntentStore 统计:");
        logger.info("     - 待处理: {}", intentStore.getPendingCount());
    }
}
