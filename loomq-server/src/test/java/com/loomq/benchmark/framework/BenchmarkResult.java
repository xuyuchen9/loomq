package com.loomq.benchmark.framework;

/**
 * Unified benchmark result record used by all protocol and scheduler benchmarks.
 */
public record BenchmarkResult(
    String name,
    double qps,
    double avgLatency,
    long p50,
    long p90,
    long p95,
    long p99,
    int success,
    int fail,
    int threads
) {}
