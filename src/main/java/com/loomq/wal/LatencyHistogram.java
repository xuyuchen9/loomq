package com.loomq.wal;

import java.util.StringJoiner;
import java.util.concurrent.atomic.LongAdder;

/**
 * 延迟直方图 - 用于统计延迟分布
 *
 * 桶边界：[0-10µs], [10-50µs], [50-100µs], [100-500µs], [500µs-1ms], [1-5ms], [5-10ms], [10ms+]
 *
 * @author loomq
 * @since v0.6.0
 */
public class LatencyHistogram {

    // 延迟桶边界（纳秒）
    private static final long[] BUCKET_BOUNDS_NS = {
        10_000,      // 10µs
        50_000,      // 50µs
        100_000,     // 100µs
        500_000,     // 500µs
        1_000_000,   // 1ms
        5_000_000,   // 5ms
        10_000_000   // 10ms
    };

    // 桶名称（用于日志/监控）
    private static final String[] BUCKET_NAMES = {
        "0-10us", "10-50us", "50-100us", "100-500us", "500us-1ms", "1-5ms", "5-10ms", "10ms+"
    };

    private final LongAdder[] buckets;
    private final LongAdder totalLatency = new LongAdder();
    private final LongAdder count = new LongAdder();

    public LatencyHistogram() {
        this.buckets = new LongAdder[BUCKET_BOUNDS_NS.length + 1];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new LongAdder();
        }
    }

    /**
     * 记录延迟
     *
     * @param latencyNs 延迟（纳秒）
     */
    public void record(long latencyNs) {
        totalLatency.add(latencyNs);
        count.increment();

        // 找到对应的桶
        for (int i = 0; i < BUCKET_BOUNDS_NS.length; i++) {
            if (latencyNs < BUCKET_BOUNDS_NS[i]) {
                buckets[i].increment();
                return;
            }
        }
        buckets[buckets.length - 1].increment();
    }

    /**
     * 计算百分位延迟
     *
     * @param percentile 百分位（0.0-1.0）
     * @return 延迟（纳秒）
     */
    public long getPercentile(double percentile) {
        long total = count.sum();
        if (total == 0) {
            return 0;
        }

        long target = (long) (total * percentile);
        long cumulative = 0;

        for (int i = 0; i < buckets.length; i++) {
            cumulative += buckets[i].sum();
            if (cumulative >= target) {
                // 返回桶边界的近似值
                return i < BUCKET_BOUNDS_NS.length ? BUCKET_BOUNDS_NS[i] : BUCKET_BOUNDS_NS[BUCKET_BOUNDS_NS.length - 1] * 2;
            }
        }
        return BUCKET_BOUNDS_NS[BUCKET_BOUNDS_NS.length - 1] * 2;
    }

    /**
     * 获取平均延迟（纳秒）
     */
    public double getAverage() {
        long c = count.sum();
        return c > 0 ? (double) totalLatency.sum() / c : 0;
    }

    /**
     * 获取 P50 延迟（纳秒）
     */
    public long getP50() {
        return getPercentile(0.50);
    }

    /**
     * 获取 P99 延迟（纳秒）
     */
    public long getP99() {
        return getPercentile(0.99);
    }

    /**
     * 获取 P99.9 延迟（纳秒）
     */
    public long getP999() {
        return getPercentile(0.999);
    }

    /**
     * 获取记录数
     */
    public long getCount() {
        return count.sum();
    }

    /**
     * 重置统计
     */
    public void reset() {
        for (LongAdder bucket : buckets) {
            bucket.reset();
        }
        totalLatency.reset();
        count.reset();
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(", ", "LatencyHistogram[", "]");
        sj.add("count=" + count.sum());
        sj.add("avg=" + String.format("%.1f", getAverage() / 1000.0) + "us");
        sj.add("p50=" + getP50() / 1000 + "us");
        sj.add("p99=" + getP99() / 1000 + "us");

        // 桶分布
        StringBuilder buckets = new StringBuilder("buckets=[");
        for (int i = 0; i < this.buckets.length; i++) {
            if (i > 0) buckets.append(", ");
            buckets.append(BUCKET_NAMES[i]).append("=").append(this.buckets[i].sum());
        }
        buckets.append("]");
        sj.add(buckets.toString());

        return sj.toString();
    }
}
