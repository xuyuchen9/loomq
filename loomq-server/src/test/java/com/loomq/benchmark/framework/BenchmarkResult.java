package com.loomq.benchmark.framework;

import java.time.Instant;
import java.util.Map;

/**
 * 原子化性能测试结果
 *
 * 记录单个测试项的完整性能数据，便于后续分析和对比。
 *
 * @author loomq
 * @since v0.6.2
 */
public record BenchmarkResult(
    // 基本信息
    String name,           // 测试名称
    String category,       // 测试分类 (storage, scheduler_add, scheduler_scan, ringbuffer, json, http, etc.)
    Instant timestamp,     // 测试时间

    // 测试参数
    int threads,           // 并发线程数
    int totalRequests,     // 总请求数
    long durationMs,       // 测试时长 (ms)

    // 吞吐量指标
    double qps,            // 每秒操作数

    // 延迟指标 (ms)
    double avgLatencyMs,   // 平均延迟
    double p50LatencyMs,   // 中位延迟
    double p90LatencyMs,   // P90 延迟
    double p95LatencyMs,   // P95 延迟
    double p99LatencyMs,   // P99 延迟

    // 结果统计
    int successCount,      // 成功数
    int failureCount,      // 失败数

    // 额外指标 (按测试类型不同)
    // 例如:
    // - storage: {"operation": "save"}
    // - scheduler: {"tier": "STANDARD", "bucketCount": 1000}
    // - ringbuffer: {"capacity": 16384, "overflowRate": "0.01%"}
    // - memory: {"bytesPerObject": 256, "totalBytes": 2560000}
    Map<String, Object> extraMetrics
) {
    /**
     * 获取摘要行（用于表格输出）
     */
    public String toSummaryLine() {
        return String.format("| %-32s | %6d | %12.0f | %7.3f | %7.3f | %7.3f | %7.3f |",
            truncate(name, 32), threads, qps, avgLatencyMs, p50LatencyMs, p95LatencyMs, p99LatencyMs);
    }

    /**
     * 获取详细行（用于分类输出）
     */
    public String toDetailedLine() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-32s: ", truncate(name, 32)));

        if (qps > 0) {
            sb.append(String.format("QPS=%,.0f ", qps));
        }

        if (avgLatencyMs > 0) {
            sb.append(String.format("Avg=%.3fms ", avgLatencyMs));
        }

        if (p99LatencyMs > 0) {
            sb.append(String.format("P99=%.3fms ", p99LatencyMs));
        }

        // 添加额外指标
        if (extraMetrics != null && !extraMetrics.isEmpty()) {
            for (Map.Entry<String, Object> entry : extraMetrics.entrySet()) {
                sb.append(String.format("%s=%s ", entry.getKey(), entry.getValue()));
            }
        }

        return sb.toString();
    }

    /**
     * 获取 CSV 行
     */
    public String toCsvLine() {
        return String.format("%s,%s,%s,%d,%d,%d,%.2f,%.6f,%.6f,%.6f,%.6f,%.6f,%d,%d",
            name, category, timestamp,
            threads, totalRequests, durationMs,
            qps, avgLatencyMs, p50LatencyMs, p90LatencyMs, p95LatencyMs, p99LatencyMs,
            successCount, failureCount);
    }

    /**
     * CSV 头
     */
    public static String csvHeader() {
        return "name,category,timestamp,threads,totalRequests,durationMs,qps,avgMs,p50Ms,p90Ms,p95Ms,p99Ms,success,failure";
    }

    /**
     * 判断是否为存储层测试
     */
    public boolean isStorageTest() {
        return "storage".equals(category);
    }

    /**
     * 判断是否为调度层测试
     */
    public boolean isSchedulerTest() {
        return category != null && category.startsWith("scheduler");
    }

    /**
     * 判断是否为持久化层测试
     */
    public boolean isPersistenceTest() {
        return "ringbuffer".equals(category) || "persistence".equals(category);
    }

    /**
     * 判断是否为 JSON 层测试
     */
    public boolean isJsonTest() {
        return "json".equals(category);
    }

    /**
     * 判断是否为 HTTP 层测试
     */
    public boolean isHttpTest() {
        return "http".equals(category);
    }

    /**
     * 获取瓶颈分析
     *
     * 根据测试结果分析可能的瓶颈点
     */
    public String getBottleneckAnalysis() {
        StringBuilder sb = new StringBuilder();

        // 分析 QPS
        if (qps > 0) {
            if (qps < 10000) {
                sb.append("⚠️ QPS 偏低 (<10K), ");
            } else if (qps < 100000) {
                sb.append("✓ QPS 正常 (10K-100K), ");
            } else if (qps < 1000000) {
                sb.append("✓ QPS 良好 (100K-1M), ");
            } else {
                sb.append("✓ QPS 优秀 (>1M), ");
            }
        }

        // 分析延迟
        if (p99LatencyMs > 0) {
            if (p99LatencyMs > 100) {
                sb.append("⚠️ P99 延迟偏高 (>100ms)");
            } else if (p99LatencyMs > 10) {
                sb.append("P99 延迟正常 (10-100ms)");
            } else {
                sb.append("P99 延迟良好 (<10ms)");
            }
        }

        // 分析失败率
        if (failureCount > 0 && successCount > 0) {
            double failureRate = (double) failureCount / (successCount + failureCount) * 100;
            if (failureRate > 5) {
                sb.append(String.format(", ⚠️ 失败率 %.1f%%", failureRate));
            }
        }

        return sb.length() > 0 ? sb.toString() : "无数据";
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return "";
        return s.length() <= maxLen ? s : s.substring(0, maxLen - 3) + "...";
    }
}
