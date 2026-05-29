# LoomQ Benchmark Suite

性能基准测试套件，覆盖内部吞吐、HTTP/gRPC 投递、调度器触发、压力测试四个维度。

## 快速开始

```bash
# 1. 构建项目
mvn clean package -DskipTests

# 2. 运行基准测试（Windows）
benchmark\benchmark.bat

# 3. 运行基准测试（Linux/macOS）
./benchmark/scripts/benchmark.sh

# 4. 查看结果
cat benchmark/results/reports/latest-summary.txt
```

---

## 目录结构

```
benchmark/
├── README.md                        # 本文档
├── config.json                      # SLO 阈值、超时、线程梯度配置
├── benchmark.bat                    # Windows 入口 → benchmark.ps1
├── test-suite.bat                   # Windows 入口 → test-suite.ps1
├── stress.bat                       # Windows 入口 → stress.ps1
├── post_intent.lua                  # wrk HTTP 压测脚本
├── scripts/
│   ├── benchmark.ps1                # 基准测试主脚本（Windows）
│   ├── benchmark.sh                 # 基准测试主脚本（Linux/macOS）
│   ├── test-suite.ps1               # 全量测试套件（Windows）
│   ├── test-suite.sh                # 全量测试套件（Linux/macOS）
│   ├── stress.ps1                   # 压力测试入口（Windows）
│   ├── stress.sh                    # 压力测试入口（Linux/macOS）
│   └── lib/
│       ├── ui.ps1 / ui.sh           # UI 工具（颜色、符号、日志）
│       ├── util.ps1 / util.sh       # 通用工具（Git、Java、解析）
│       ├── server.ps1 / server.sh   # 服务器生命周期管理
│       ├── output.ps1 / output.sh   # 输出格式化（CSV、JSON、TXT）
│       └── i18n/
│           └── report-zh.json       # 中文翻译
└── results/                         # 测试结果（gitignored）
    ├── history.csv                  # 历史记录（55 列宽格式）
    ├── stress-history.csv           # 压力测试历史
    ├── reports/                     # JSON + TXT 报告
    ├── logs/                        # 场景日志、服务器日志
    ├── runtime/                     # 临时服务器数据
    └── m2repo/                      # 隔离 Maven 本地仓库
```

---

## 脚本说明

### benchmark.ps1 / benchmark.sh — 基准测试

运行四个基准测试场景，测量系统吞吐量和延迟。

**用法（Windows）：**
```powershell
.\benchmark\scripts\benchmark.ps1 [Options]
```

**用法（Linux/macOS）：**
```bash
./benchmark/scripts/benchmark.sh [Options]
```

**参数：**

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-Quick` / `--quick` | 快速测试模式 | false |
| `-Scenario` / `--scenario=<name>` | 场景：all, internal, http, grpc, scheduler | all |
| `-Workload` / `--workload=<name>` | 工作负载：uniform, prod-typical, burst-ultra, mixed-heavy | uniform |
| `-NoCompile` / `--no-compile` | 跳过编译 | false |
| `-VerboseOutput` / `--verbose` | 显示完整场景输出 | false |
| `-Stress` / `--stress` | 启用压力测试模式 | false |
| `-StressOnly <type>` / `--stress-only=<type>` | 仅运行指定压力测试：http, grpc | - |
| `-StressThreads` / `--stress-threads=<list>` | 压力测试线程梯度 | 500,1000,2000,3000 |
| `-StressDuration` / `--stress-duration=<sec>` | 每梯度持续时间 | 30 |
| `-StressCooldown` / `--stress-cooldown=<ms>` | 梯度间冷却时间 | 5000 |
| `-GrpcTier` / `--grpc-tier=<tier>` | gRPC 测试精度档位 | STANDARD |
| `-GrpcWorkerThreads` / `--grpc-worker-threads=<n>` | gRPC 工作线程数 | auto |
| `-Profile` / `--profile` | 启用 JFR 性能分析 | false |
| `-Compare` / `--compare` | 仅查看历史对比 | false |

**测试场景：**

| 场景 | 说明 | 需要服务器 |
|------|------|-----------|
| internal | 进程内上限（WAL、存储、Observer 开销） | 否 |
| HTTP | HTTP 创建路径（含压力测试） | 是 |
| gRPC | gRPC 创建路径（含压力测试） | 是 |
| scheduler | 调度器触发路径（MockServer） | 否 |

**示例：**
```powershell
# 全量测试
.\benchmark\scripts\benchmark.ps1

# 仅 gRPC 压力测试
.\benchmark\scripts\benchmark.ps1 -Stress -StressOnly grpc

# 快速调度器测试
.\benchmark\scripts\benchmark.ps1 -Scenario scheduler -Quick
```

---

### test-suite.ps1 / test-suite.sh — 全量测试套件

运行四阶段测试：快测 → 慢测 → 集成测试 → 基准测试。

**用法：**
```powershell
.\benchmark\scripts\test-suite.ps1 [-Quick] [-SkipBenchmark]
```

```bash
./benchmark/scripts/test-suite.sh [--quick] [--no-bench]
```

**阶段：**

| 阶段 | Maven Profile | 说明 |
|------|--------------|------|
| 1 | fast-tests | 快速单元测试 |
| 2 | slow-tests | 慢速测试（PrecisionScheduler, RaftNode, BackPressure） |
| 3 | integration-tests | 集成测试 |
| 4 | SchedulerTriggerBenchmarkWithMockServer | 调度器基准测试 |

---

### stress.ps1 / stress.sh — 压力测试

gRPC 压力测试便捷入口，等价于 `benchmark.ps1 -Stress -StressOnly grpc`。

**用法：**
```powershell
.\benchmark\scripts\stress.ps1 [-StressThreads "500,1000,2000"] [-StressDuration 30]
```

```bash
./benchmark/scripts/stress.sh --stress-threads 500,1000,2000 --stress-duration 30
```

---

## 配置说明

配置文件：`benchmark/config.json`

```json
{
  "slo": {
    "ULTRA":    { "p95_wakeup_us": 15000,  "p99_wakeup_us": 25000,  "p95_e2e_ms": 50,   "p99_e2e_ms": 100  },
    "FAST":     { "p95_wakeup_us": 30000,  "p99_wakeup_us": 50000,  "p95_e2e_ms": 100,  "p99_e2e_ms": 200  },
    "HIGH":     { "p95_wakeup_us": 50000,  "p99_wakeup_us": 80000,  "p95_e2e_ms": 200,  "p99_e2e_ms": 400  },
    "STANDARD": { "p95_wakeup_us": 200000, "p99_wakeup_us": 400000, "p95_e2e_ms": 800,  "p99_e2e_ms": 1500 },
    "ECONOMY":  { "p95_wakeup_us": 400000, "p99_wakeup_us": 800000, "p95_e2e_ms": 1500, "p99_e2e_ms": 3000 }
  },
  "timeouts": { "internal_sec": 120, "http_sec": 120, "grpc_sec": 120, "scheduler_sec": 300 },
  "stress": { "default_threads": [500, 1000, 2000, 3000], "default_duration_sec": 30, "default_cooldown_ms": 5000 },
  "rotation": { "keep_recent": 10 }
}
```

**SLO 说明：**
- `p95_wakeup_us` / `p99_wakeup_us`：唤醒延迟（微秒），用于调度器精度评估
- `p95_e2e_ms` / `p99_e2e_ms`：端到端延迟（毫秒），用于整体投递评估

---

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
| `internal_qps` | 内部 WAL/scheduler 吞吐量（仅 benchmark.ps1） |

### HTTP 投递 (4 列)

| 列名 | 说明 |
|------|------|
| `http_peak_qps` | HTTP 峰值 QPS |
| `http_best_p99_ms` | HTTP 最佳 P99 延迟 |
| `http_worst_p99_ms` | HTTP 最差 P99 延迟 |
| `http_fail_rate` | HTTP 失败率 |

### gRPC 投递 (4 列)

| 列名 | 说明 |
|------|------|
| `grpc_peak_qps` | gRPC 峰值 QPS |
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
| `total_qps` | RESULT_SYSTEM_QPS | 总吞吐量 |
| `global_p95_total_ms` | RESULT_GLOBAL_LATENCY | 全局 p95 延迟 |
| `vt_reduction_pct` | RESULT_OPTIMIZATION | VT 减少百分比 |
| `cohort_wake_events` | RESULT_COHORT | Cohort 唤醒事件数 |

---

## 依赖说明

| 工具 | 用途 | 必需 |
|------|------|------|
| JDK 25+ | Java 编译和运行 | 是 |
| Maven 3.9+ | 构建系统 | 是 |
| Git | 提交哈希、分支名 | 是 |
| PowerShell 5.1+ | Windows 脚本执行 | Windows |
| Bash | Linux/macOS 脚本执行 | Linux/macOS |
| wrk | HTTP 负载测试（post_intent.lua） | 否 |
| jcmd / jfr | JFR 性能分析 | 否 |

---

## 已知限制

1. **bash 脚本不支持 HTTP/gRPC 基准测试** — 需要服务器生命周期管理，目前仅 Windows PowerShell 支持。Linux/macOS 用户可手动启动服务器后使用 wrk 压测。

2. **macOS 兼容性** — bash 脚本使用 `sed` 替代 `grep -oP`，兼容 macOS BSD grep。

3. **JFR 分析** — Windows 上 `jcmd attach` 可能失败（Start-Process 启动的 JVM）。建议使用代码级埋点替代。

---

## 常见问题

**Q: 如何只运行调度器测试？**
```powershell
.\benchmark\scripts\benchmark.ps1 -Scenario scheduler
```

**Q: 如何查看历史对比？**
```powershell
.\benchmark\scripts\benchmark.ps1 -Compare
```

**Q: 如何运行压力测试？**
```powershell
.\benchmark\scripts\stress.ps1
# 或
.\benchmark\scripts\stress.bat
```

**Q: CSV 文件在哪里？**
```
benchmark/results/history.csv
```

**Q: 如何清理旧报告？**
报告自动轮转，保留最近 10 组。手动清理：
```bash
rm -rf benchmark/results/reports/*
rm -rf benchmark/results/logs/*
rm -rf benchmark/results/runtime/*
```
