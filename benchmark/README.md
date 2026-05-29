# LoomQ 性能基准测试

## 目录结构

```
benchmark/
├── scripts/                    # 测试脚本
│   ├── benchmark.ps1          # PowerShell 脚本 (Windows)
│   ├── benchmark.sh           # Bash 脚本 (Linux/macOS)
│   ├── run-all.ps1            # 一键全量测试 (Windows)
│   └── run-all.sh             # 一键全量测试 (Linux/macOS)
├── results/                    # 测试结果
│   ├── history.csv            # 历史记录 (55 列宽格式)
│   ├── reports/               # 测试报告 (JSON + TXT)
│   └── logs/                  # 完整运行日志
├── post_intent.lua            # wrk 压测脚本
└── README.md                   # 本文档
```

## 快速开始

### Windows (PowerShell)

```powershell
# 运行全部场景 (internal + HTTP + scheduler)
.\benchmark\scripts\benchmark.ps1

# 仅运行内部测试（无需启动服务）
.\benchmark\scripts\benchmark.ps1 -InternalOnly

# 快速模式
.\benchmark\scripts\benchmark.ps1 -Quick

# 查看历史对比
.\benchmark\scripts\benchmark.ps1 -Compare

# 一键全量测试 (快测 + 慢测 + 集成 + 压测)
.\benchmark\scripts\run-all.ps1
.\benchmark\scripts\run-all.ps1 -Quick
```

### Linux/macOS (Bash)

```bash
# 运行全部场景
./benchmark/scripts/benchmark.sh

# 仅运行调度器压测
./benchmark/scripts/benchmark.sh --scenario=scheduler

# 快速模式
./benchmark/scripts/benchmark.sh --quick

# 查看历史对比
./benchmark/scripts/benchmark.sh --compare

# 一键全量测试
./benchmark/scripts/run-all.sh
./benchmark/scripts/run-all.sh --quick
```

## 参数说明

### benchmark.ps1

| 参数 | 说明 |
|------|------|
| `-InternalOnly` | 仅运行内部组件测试 |
| `-Quick` | 快速测试模式 |
| `-Compare` | 显示历史对比 |
| `-NoCompile` | 跳过编译步骤 |
| `-Workload <name>` | 负载分布: uniform, prod-typical, burst-ultra, mixed-heavy (默认: uniform) |
| `-VerboseOutput` | 显示完整场景日志 |
| `-KeepOpen` | 运行结束后保持窗口 |
| `-NoPause` | 不暂停窗口 |

### benchmark.sh

| 参数 | 说明 |
|------|------|
| `--quick` | 快速测试模式 |
| `--full` | 完整测试模式 (默认) |
| `--scenario=<name>` | 场景选择: all, internal, http, scheduler (默认: all) |
| `--workload=<name>` | 负载分布: uniform, prod-typical, burst-ultra, mixed-heavy (默认: uniform) |
| `--no-compile` | 跳过编译 |
| `--compare` | 显示历史对比 |
| `--verbose` | 显示完整输出 |

### run-all.ps1 / run-all.sh

| 参数 | 说明 |
|------|------|
| `-Quick` / `--quick` | 快速模式 (跳过大压测) |
| `-SkipBenchmark` / `--no-bench` | 跳过所有压测 |

## 强制落库

所有测试运行的结果**始终**持久化到 `benchmark/results/` 目录：

- **CSV**: `history.csv` — 55 列宽格式，每次运行追加一行
- **JSON**: `reports/benchmark-*.json` 或 `reports/full-suite-*.json` — 完整结构化数据
- **TXT**: `reports/benchmark-*.txt` 或 `reports/full-suite-*.txt` — 人类可读摘要

## 报告自动轮转

每次运行脚本时，自动删除超过 10 组的旧报告和日志（保留最近 10 组）。每组包含一个 JSON + TXT 文件对。

## CSV Schema (55 列)

### 运行标识 (6 列)

| 列名 | 来源 | 说明 |
|------|------|------|
| `timestamp` | 系统 | ISO 8601 时间戳 |
| `commit` | Git | 短哈希 |
| `branch` | Git | 分支名 |
| `mode` | 参数 | quick 或 full |
| `java_version` | RESULT_ENV | JDK 版本 |
| `os_name` | RESULT_ENV | 操作系统 |

### 内部基准 (1 列)

| 列名 | 说明 |
|------|------|
| `internal_qps` | 内部 WAL/scheduler 吞吐量 (仅 benchmark.ps1) |

### HTTP 投递 (4 列)

| 列名 | 说明 |
|------|------|
| `http_peak_qps` | HTTP 峰值 QPS (仅 benchmark.ps1) |
| `http_best_p99_ms` | HTTP 最佳 P99 延迟 |
| `http_worst_p99_ms` | HTTP 最差 P99 延迟 |
| `http_fail_rate` | HTTP 失败率 |

### gRPC 投递 (4 列)

| 列名 | 说明 |
|------|------|
| `grpc_peak_qps` | gRPC 峰值 QPS (仅 benchmark.ps1) |
| `grpc_best_p99_ms` | gRPC 最佳 P99 延迟 |
| `grpc_worst_p99_ms` | gRPC 最差 P99 延迟 |
| `grpc_fail_rate` | gRPC 失败率 |

### Per-Tier 核心指标 (7 列 × 5 层级 = 35 列)

对每个层级 T (ULTRA, FAST, HIGH, STANDARD, ECONOMY):

| 列名 | 来源 | 说明 |
|------|------|------|
| `T_qps` | RESULT_ROW | 吞吐量 |
| `T_p95_ms` | RESULT_LATENCY | 唤醒延迟 p95 |
| `T_p99_ms` | RESULT_LATENCY | 唤醒延迟 p99 |
| `T_e2e_p95_ms` | RESULT_E2E_LATENCY | 端到端延迟 p95 |
| `T_e2e_p99_ms` | RESULT_E2E_LATENCY | 端到端延迟 p99 |
| `T_util_pct` | RESULT_SEMAPHORE | 信号量利用率 |
| `T_backpressure` | RESULT_ROW | 背压事件数 |

### 全局聚合 (5 列)

| 列名 | 来源 | 说明 |
|------|------|------|
| `completion_rate` | RESULT | 完成率 |
| `total_qps` | RESULT_SYSTEM_QPS | 系统总 QPS |
| `global_p95_total_ms` | RESULT_GLOBAL_LATENCY | 全局 p95 总延迟 |
| `vt_reduction_pct` | RESULT_OPTIMIZATION | VT 减少百分比 |
| `cohort_wake_events` | RESULT_COHORT | Cohort 唤醒事件数 |

详细数据（p50/p75/p90/p999/max/mean/samples、队列、生命周期、批次、追踪、借用等）保留在 JSON 报告中。

## 测试场景

### 1) In-process upper bound

直接调用 IntentStore 和 BucketGroup，测量存储和调度层的理论极限。

### 2) HTTP create path

通过 HTTP 接口创建 Intent，测量 Netty + JSON + 存储的完整链路吞吐。

### 3) Scheduler trigger path

模拟调度器触发 + Webhook 回调的完整链路，包含五层精度档位的差异化行为。这是最接近生产负载的测试。

## SLO 验证

端到端延迟 (executeAt → webhook received) 的 SLO 阈值:

| 档位 | p95 | p99 |
|------|-----|-----|
| ULTRA | 50ms | 100ms |
| FAST | 150ms | 250ms |
| HIGH | 600ms | 1000ms |
| STANDARD | 1500ms | 2200ms |
| ECONOMY | 3500ms | 5000ms |

## 常见问题

### Q: 为什么 scheduler QPS 远低于 in-process QPS?

调度器压测包含 HTTP 回调延迟（mock server 模拟 5ms 延迟），理论 QPS ≈ 并发数 / (batch_dwell + HTTP_RTT)。in-process 测试不包含网络和回调开销。

### Q: history.csv 被截断/重建了?

CSV header 一致性检查会验证列数和列名。如果检测到旧格式（列数或列名不匹配），会自动重建 header。历史数据在 JSON 报告中保留。
