package com.loomq.v2;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * V0.2 监控测试
 *
 * 验证目标：
 * 1. 指标采集正确
 * 2. 告警阈值生效
 * 3. Prometheus 格式正确
 */
class MonitoringV2Test {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("监控测试 - 指标采集")
    void testMetricsCollection() throws Exception {
        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║              V0.2 监控测试 - 指标采集                           ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();

        Path walDir = tempDir.resolve("wal-metrics");

        LoomqConfigV2 config = new LoomqConfigV2(
                1000, 1000, 0, 100, 5000, 3, 100, 5000, 10000,
                walDir.toString(), "batch",
                LoomqConfigV2.AckLevel.ASYNC
        );

        LoomqEngineV2 engine = new LoomqEngineV2(config);
        engine.start();

        MonitoringServiceV2 monitoring = MonitoringServiceV2.getInstance();

        // 创建任务
        System.out.println("创建 100 任务...");
        for (int i = 0; i < 100; i++) {
            var task = com.loomq.entity.Task.builder()
                    .taskId("metrics-" + i)
                    .bizKey("metrics-biz-" + i)
                    .webhookUrl("http://localhost:9999/webhook")
                    .triggerTime(System.currentTimeMillis() + 3600000)
                    .build();
            engine.createTask(task);
        }

        Thread.sleep(500);

        // 获取指标
        var stats = engine.getStats();
        System.out.println();
        System.out.println("引擎统计:");
        System.out.printf("  - 调度器: buckets=%d, readyQueue=%d, scheduled=%d%n",
                stats.scheduler().bucketCount(), stats.scheduler().readyQueueSize(), stats.scheduler().totalScheduled());
        System.out.printf("  - 分发器: queueSize=%d, permits=%d%n",
                stats.dispatcher().queueSize(), stats.dispatcher().availablePermits());
        System.out.printf("  - WAL: events=%d, batches=%d, fsyncs=%d%n",
                stats.wal().totalEvents(), stats.wal().totalBatches(), stats.wal().totalFsyncs());

        System.out.println();
        System.out.println("========== 验证结果 ==========");

        // 验证指标
        assertTrue(stats.scheduler().totalScheduled() >= 100, "任务应已调度");
        assertTrue(stats.wal().totalEvents() >= 100, "WAL 事件应已记录");

        System.out.println("指标采集: ✅ 通过");

        engine.stop();
    }

    @Test
    @DisplayName("监控测试 - Prometheus 导出")
    void testPrometheusExport() throws Exception {
        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║              V0.2 监控测试 - Prometheus 导出                    ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();

        Path walDir = tempDir.resolve("wal-prom");

        LoomqConfigV2 config = new LoomqConfigV2(
                1000, 1000, 0, 100, 5000, 3, 100, 5000, 10000,
                walDir.toString(), "batch",
                LoomqConfigV2.AckLevel.ASYNC
        );

        LoomqEngineV2 engine = new LoomqEngineV2(config);
        engine.start();

        MonitoringServiceV2 monitoring = MonitoringServiceV2.getInstance();

        // 创建任务
        for (int i = 0; i < 50; i++) {
            var task = com.loomq.entity.Task.builder()
                    .taskId("prom-" + i)
                    .bizKey("prom-biz-" + i)
                    .webhookUrl("http://localhost:9999/webhook")
                    .triggerTime(System.currentTimeMillis() + 3600000)
                    .build();
            engine.createTask(task);
        }

        Thread.sleep(500);

        // 导出 Prometheus 指标
        String metrics = monitoring.exportPrometheusMetrics();

        System.out.println("Prometheus 指标 (部分):");
        System.out.println("---");

        // 只打印前 30 行
        String[] lines = metrics.split("\n");
        int printLines = Math.min(30, lines.length);
        for (int i = 0; i < printLines; i++) {
            System.out.println(lines[i]);
        }
        if (lines.length > printLines) {
            System.out.println("... (共 " + lines.length + " 行)");
        }
        System.out.println("---");

        System.out.println();
        System.out.println("========== 验证结果 ==========");

        // 验证格式
        assertTrue(metrics.contains("# HELP"), "应包含 HELP 注释");
        assertTrue(metrics.contains("# TYPE"), "应包含 TYPE 注释");
        assertTrue(metrics.contains("loomq_v2_tasks_created_total"), "应包含任务创建指标");
        assertTrue(metrics.contains("loomq_v2_wake_latency_ms_p95"), "应包含唤醒延迟指标");
        assertTrue(metrics.contains("loomq_v2_total_latency_ms_p95"), "应包含总延迟指标");
        assertTrue(metrics.contains("loomq_v2_ringbuffer_usage_percent"), "应包含 RingBuffer 指标");

        System.out.println("Prometheus 格式: ✅ 通过");

        engine.stop();
    }

    @Test
    @DisplayName("监控测试 - 告警状态")
    void testAlertStatus() throws Exception {
        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║              V0.2 监控测试 - 告警状态                           ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();

        Path walDir = tempDir.resolve("wal-alert");

        LoomqConfigV2 config = new LoomqConfigV2(
                1000, 1000, 0, 100, 5000, 3, 100, 5000, 10000,
                walDir.toString(), "batch",
                LoomqConfigV2.AckLevel.ASYNC
        );

        LoomqEngineV2 engine = new LoomqEngineV2(config);
        engine.start();

        MonitoringServiceV2 monitoring = MonitoringServiceV2.getInstance();

        // 获取告警状态
        var alertStatus = monitoring.getAlertStatus();

        System.out.println("告警阈值:");
        System.out.printf("  - P95 唤醒延迟: %d ms%n", alertStatus.wakeLatencyThresholdMs());
        System.out.printf("  - P95 总延迟: %d ms%n", alertStatus.totalLatencyThresholdMs());
        System.out.printf("  - Webhook 超时率: %.1f%%%n", alertStatus.timeoutRateThreshold());
        System.out.printf("  - WAL 大小: %d bytes (%d MB)%n",
                alertStatus.walSizeThresholdBytes(), alertStatus.walSizeThresholdBytes() / 1024 / 1024);
        System.out.printf("  - 背压阈值: %.1f%%%n", alertStatus.backpressureThreshold());

        System.out.println();
        System.out.println("告警状态:");
        System.out.printf("  - 唤醒延迟告警: %s%n",
                alertStatus.wakeLatencyAlert() ? "⚠️ 激活" : "✅ 正常");
        System.out.printf("  - 总延迟告警: %s%n",
                alertStatus.totalLatencyAlert() ? "⚠️ 激活" : "✅ 正常");
        System.out.printf("  - Webhook 超时告警: %s%n",
                alertStatus.timeoutRateAlert() ? "⚠️ 激活" : "✅ 正常");
        System.out.printf("  - WAL 大小告警: %s%n",
                alertStatus.walSizeAlert() ? "⚠️ 激活" : "✅ 正常");
        System.out.printf("  - 背压告警: %s%n",
                alertStatus.backpressureAlert() ? "⚠️ 激活" : "✅ 正常");

        System.out.println();
        System.out.println("========== 验证结果 ==========");

        // 验证告警阈值设置正确（不验证具体状态，因为单例会被其他测试污染）
        assertEquals(50, alertStatus.wakeLatencyThresholdMs(), "唤醒延迟阈值应为 50ms");
        assertEquals(5000, alertStatus.totalLatencyThresholdMs(), "总延迟阈值应为 5000ms");
        assertEquals(5.0, alertStatus.timeoutRateThreshold(), 0.1, "超时率阈值应为 5%");
        assertEquals(75.0, alertStatus.backpressureThreshold(), 0.1, "背压阈值应为 75%");

        System.out.println("告警阈值: ✅ 通过");

        engine.stop();
    }

    @Test
    @DisplayName("监控测试 - 延迟指标")
    void testLatencyMetrics() throws Exception {
        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║              V0.2 监控测试 - 延迟指标                           ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();

        MonitoringServiceV2 monitoring = MonitoringServiceV2.getInstance();

        // 重置监控状态（单例模式需要清理）
        monitoring.reset();

        // 模拟延迟记录
        System.out.println("记录延迟样本...");

        // 唤醒延迟样本
        for (int i = 0; i < 100; i++) {
            long latency = (long) (Math.random() * 100);  // 0-100ms
            monitoring.recordWakeLatency(latency);
        }

        // Webhook 延迟样本
        for (int i = 0; i < 100; i++) {
            long latency = (long) (Math.random() * 500);  // 0-500ms
            monitoring.recordWebhookLatency(latency);
        }

        // 总延迟样本
        for (int i = 0; i < 100; i++) {
            long latency = (long) (Math.random() * 1000);  // 0-1000ms
            monitoring.recordTotalLatency(latency);
        }

        System.out.println();
        System.out.println("P95 延迟:");
        System.out.printf("  - 唤醒延迟: %d ms%n", monitoring.calculateP95WakeLatency());
        System.out.printf("  - Webhook 延迟: %d ms%n", monitoring.calculateP95WebhookLatency());
        System.out.printf("  - 总延迟: %d ms%n", monitoring.calculateP95TotalLatency());

        System.out.println();
        System.out.println("========== 验证结果 ==========");

        // P95 应该在合理范围内（考虑桶边界）
        assertTrue(monitoring.calculateP95WakeLatency() <= 250, "唤醒延迟 P95 应在合理范围内");
        assertTrue(monitoring.calculateP95WebhookLatency() <= 1000, "Webhook 延迟 P95 应在合理范围内");
        assertTrue(monitoring.calculateP95TotalLatency() <= 2500, "总延迟 P95 应在合理范围内");

        System.out.println("延迟指标: ✅ 通过");
    }
}
