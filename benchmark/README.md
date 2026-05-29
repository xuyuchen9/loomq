# LoomQ Benchmark Suite

性能基准测试套件，覆盖内部吞吐、HTTP/gRPC 创建路径、调度器触发、压力测试四个维度。

## 快速开始

```bash
# Windows
benchmark\benchmark.bat

# Linux/macOS
./benchmark/scripts/benchmark.sh

# 快速验证
benchmark\benchmark.bat --quick
```

输出：
- **Excel 报告**: `benchmark/results/reports/benchmark-report-{timestamp}.xlsx`
- **MD 报告**: `benchmark/results/reports/benchmark-report-{timestamp}.md`

---

## 目录结构

```
benchmark/
├── README.md
├── benchmark.bat                    # Windows 入口
├── benchmark.sh                     # Linux/macOS 入口
├── config.json                      # SLO 阈值配置
├── scripts/
│   ├── benchmark.ps1                # 主脚本（Windows）
│   ├── benchmark.sh                 # 主脚本（Linux/macOS）
│   └── lib/
│       ├── ui.ps1 / ui.sh           # 终端 UI
│       ├── util.ps1 / util.sh       # 工具函数
│       ├── server.ps1 / server.sh   # 服务器生命周期
│       ├── report.ps1 / report.sh   # Excel + MD 报告生成
│       └── gen_excel.py             # Python Excel 生成器
└── results/
    ├── reports/                     # Excel + MD 报告
    ├── logs/                        # 场景日志
    ├── runtime/                     # 临时服务器数据
    └── m2repo/                      # 隔离 Maven 仓库
```

---

## 用法

### Windows

```powershell
.\benchmark\scripts\benchmark.ps1 [Options]
```

### Linux/macOS

```bash
./benchmark/scripts/benchmark.sh [Options]
```

### 参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-Quick` / `--quick` | 快速测试模式 | false |
| `-Stress` / `--stress` | 包含压力测试 | false |
| `-StressOnly <type>` / `--stress-only=<type>` | 仅压力测试: http / grpc | - |
| `-Scenario <name>` / `--scenario=<name>` | 场景: all / internal / create / scheduler | all |
| `-GrpcTier <tier>` / `--grpc-tier=<tier>` | gRPC 精度档位 | STANDARD |
| `-NoCompile` / `--no-compile` | 跳过编译 | false |
| `-Compare` / `--compare` | 查看上次报告 | false |
| `-VerboseOutput` / `--verbose` | 显示完整日志 | false |

### 测试场景

| 场景 | 说明 | 需要服务器 |
|------|------|-----------|
| internal | IntentStore 吞吐、内存、冷热交换、WAL、存储引擎 | 否 |
| create | HTTP + gRPC 创建路径吞吐和延迟 | 是 |
| scheduler | 调度器触发精度（5 档位唤醒延迟 + E2E） | 是 |
| stress | 多线程梯度压力扫描，拐点检测 | 是 |

### 示例

```powershell
# 全量测试
.\benchmark\scripts\benchmark.ps1

# 快速验证
.\benchmark\scripts\benchmark.ps1 -Quick

# 全量 + 压力测试
.\benchmark\scripts\benchmark.ps1 -Stress

# 仅 gRPC 压力测试
.\benchmark\scripts\benchmark.ps1 -Stress -StressOnly grpc

# 仅内部组件测试
.\benchmark\scripts\benchmark.ps1 -Scenario internal

# 查看上次报告
.\benchmark\scripts\benchmark.ps1 -Compare
```

---

## 输出格式

### Excel 报告（6 个 Sheet）

| Sheet | 内容 |
|-------|------|
| Summary | 吞吐量对比（HTTP vs gRPC）、回归对比 |
| Create Path | 各线程数下的 QPS、P50/P90/P99 |
| Scheduler | 5 档位的唤醒延迟、E2E 延迟、利用率、背压 |
| Internal | IntentStore QPS、内存、WAL、存储引擎对比 |
| SLO | 各档位 SLO 验证（目标 vs 实际，PASS/FAIL） |
| Environment | 时间、Commit、分支、Java、OS、CPU |

### MD 报告

中文 Markdown 格式，结构与 Excel 对应，适合嵌入 GitHub Release Notes。

---

## 配置

`config.json` 定义 SLO 阈值：

```json
{
  "slo": {
    "ULTRA":    { "p95_wakeup_us": 15000,  "p99_wakeup_us": 25000,  "p95_e2e_ms": 50,   "p99_e2e_ms": 100  },
    "FAST":     { "p95_wakeup_us": 30000,  "p99_wakeup_us": 50000,  "p95_e2e_ms": 100,  "p99_e2e_ms": 200  },
    "HIGH":     { "p95_wakeup_us": 50000,  "p99_wakeup_us": 80000,  "p95_e2e_ms": 200,  "p99_e2e_ms": 400  },
    "STANDARD": { "p95_wakeup_us": 200000, "p99_wakeup_us": 400000, "p95_e2e_ms": 800,  "p99_e2e_ms": 1500 },
    "ECONOMY":  { "p95_wakeup_us": 400000, "p99_wakeup_us": 800000, "p95_e2e_ms": 1500, "p99_e2e_ms": 3000 }
  }
}
```

---

## 依赖

| 工具 | 用途 | 必需 |
|------|------|------|
| JDK 25+ | 编译和运行 | 是 |
| Maven 3.9+ | 构建系统 | 是 |
| Python 3 + xlsxwriter | Excel 报告生成 | 是（Linux/macOS） |
| PowerShell 5.1+ | Windows 脚本 | Windows |

安装 xlsxwriter：
```bash
pip install xlsxwriter
```

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

**Q: Excel 生成失败？**
确认 Python 3 和 xlsxwriter 已安装：
```bash
python3 --version
pip install xlsxwriter
```

**Q: 如何清理旧报告？**
报告自动轮转（保留最近 10 组）。手动清理：
```bash
rm -rf benchmark/results/reports/*
rm -rf benchmark/results/logs/*
```
