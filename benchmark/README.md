# LoomQ 原子化性能基准测试

## 目录结构

```
benchmark/
├── scripts/                    # 测试脚本
│   ├── benchmark.ps1          # PowerShell 脚本 (Windows)
│   ├── benchmark.sh           # Bash 脚本 (Linux/Mac)
│   └── benchmark.cmd          # CMD 脚本 (Windows)
├── results/                    # 测试结果
│   ├── history.csv            # 历史记录
│   └── reports/               # 测试报告
│       └── report-*.md
├── post_intent.lua            # wrk 压测脚本
└── README.md                   # 本文档
```

## 快速开始

### Windows (PowerShell)

```powershell
# 进入项目根目录
cd D:\Development\Projects\Personal\loomq

# 运行测试
.\benchmark\scripts\benchmark.ps1

# 仅运行内部测试（无需启动服务）
.\benchmark\scripts\benchmark.ps1 -InternalOnly

# 查看历史对比
.\benchmark\scripts\benchmark.ps1 -Compare

# 自动保存结果
.\benchmark\scripts\benchmark.ps1 -Save

# 预览模式（不保存）
.\benchmark\scripts\benchmark.ps1 -NoSave

# 查看帮助
.\benchmark\scripts\benchmark.ps1 -Help
```

### Linux/Mac (Bash)

```bash
# 进入项目根目录
cd /path/to/loomq

# 运行测试
./benchmark/scripts/benchmark.sh

# 查看历史对比
./benchmark/scripts/benchmark.sh --compare
```

## 参数说明

| 参数 | 说明 |
|------|------|
| `-InternalOnly` | 仅运行内部组件测试 |
| `-Quick` | 快速测试模式 |
| `-Compare` | 仅显示历史对比 |
| `-NoCompile` | 跳过编译步骤 |
| `-Save` | 自动保存结果 |
| `-NoSave` | 预览模式，不保存 |

## 测试架构

```
客户端请求
    ↓
┌─────────────────────────────────────────────────────────┐
│  HTTP 层                                                │
│  ├── Netty 连接处理                                     │
│  └── 路由匹配                                           │
├─────────────────────────────────────────────────────────┤
│  JSON 层                                                │
│  ├── 请求反序列化 (Jackson)                             │
│  └── 响应序列化 (手写/Jackson)                          │
├─────────────────────────────────────────────────────────┤
│  存储层                                                 │
│  ├── IntentStore.save() 新建                           │
│  ├── IntentStore.save() 更新                           │
│  └── IntentStore.findById() 查询                       │
├─────────────────────────────────────────────────────────┤
│  调度层                                                 │
│  ├── BucketGroup.add() 入桶 (按精度档位)               │
│  └── BucketGroup.scanDue() 扫描到期                    │
├─────────────────────────────────────────────────────────┤
│  持久化层                                               │
│  ├── RingBuffer.offer() 写入                           │
│  ├── RingBuffer.drain() 消费                           │
│  └── CRC32 校验                                        │
└─────────────────────────────────────────────────────────┘
```

## 原子测试项说明

### 1. 存储层测试 (`storage`)

| 测试名称 | 关注点 | 预期性能 |
|----------|--------|----------|
| IntentStore.save (new) | 新建 Intent 的写入性能 | >500K/s |
| IntentStore.save (update) | 更新已有 Intent 的性能 | >300K/s |
| IntentStore.findById | 按 ID 查询性能 | >5M/s |
| ConcurrentHashMap.put | 底层数据结构写入性能 | >10M/s |
| ConcurrentHashMap.get | 底层数据结构读取性能 | >20M/s |

### 2. 调度层测试 (`scheduler_add`, `scheduler_scan`)

| 测试名称 | 关注点 | 预期性能 |
|----------|--------|----------|
| BucketGroup.add ULTRA | 10ms 精度档位入桶性能 | >200K/s |
| BucketGroup.add FAST | 50ms 精度档位入桶性能 | >200K/s |
| BucketGroup.add HIGH | 100ms 精度档位入桶性能 | >200K/s |
| BucketGroup.add STANDARD | 500ms 精度档位入桶性能 | >200K/s |
| BucketGroup.add ECONOMY | 1000ms 精度档位入桶性能 | >200K/s |
| BucketGroup.scanDue * | 各档位扫描到期任务性能 | >100K/s |

### 3. 持久化层测试 (`ringbuffer`, `persistence`)

| 测试名称 | 关注点 | 预期性能 |
|----------|--------|----------|
| RingBuffer.offer 16K | 16K 容量写入性能 | >10M/s |
| RingBuffer.drain 100 | 批量消费性能 | >1M/s |
| CRC32 256B | 256 字节校验性能 | >5M/s |

### 4. JSON 层测试 (`json`)

| 测试名称 | 关注点 | 预期性能 |
|----------|--------|----------|
| Jackson.readValue CreateRequest | 请求反序列化性能 | >200K/s |
| String.format JSON | 字符串格式化性能 | >500K/s |

## 性能基线 (v0.6.1 参考)

### 存储层

| 操作 | 1 线程 | 10 线程 | 50 线程 |
|------|--------|---------|---------|
| IntentStore.save (new) | ~500K/s | ~800K/s | ~1.2M/s |
| IntentStore.findById | ~5M/s | ~6M/s | ~6M/s |

### 调度层 (各精度档位)

| 档位 | BucketGroup.add | BucketGroup.scanDue |
|------|-----------------|---------------------|
| ULTRA | ~200K/s | ~100K/s |
| FAST | ~200K/s | ~100K/s |
| HIGH | ~200K/s | ~100K/s |
| STANDARD | ~200K/s | ~100K/s |
| ECONOMY | ~200K/s | ~100K/s |

### 持久化层

| 操作 | 性能 |
|------|------|
| RingBuffer.offer 16K | ~18M/s |
| CRC32 256B | ~5M/s |

## 瓶颈分析指南

### 定位瓶颈

当发现整体性能下降时：

1. **查看 HTTP 层测试** → 是否网络/协议问题
2. **查看存储层测试** → 是否内存结构问题
3. **查看调度层测试** → 是否调度算法问题
4. **查看持久化层测试** → 是否 IO 问题

### 性能对比

```
原子测试 QPS vs HTTP 接口 QPS

IntentStore.save: 1,000,000/s
POST /v1/intents:    15,000/s

差距: 66x → HTTP+JSON 层是主要瓶颈
```

## 保存策略

测试完成后会询问是否保存结果：

1. **版本变化时**：建议保存，便于对比
2. **预览模式**：仅查看结果，不保存
3. **自动保存**：使用 `-Save` 参数

## 输出文件

- **报告**: `benchmark/results/reports/report-YYYYMMDD-HHMMSS.md`
- **历史**: `benchmark/results/history.csv`

## 常见问题

### Q: HTTP QPS 远低于原子测试 QPS

**原因**: HTTP 协议开销 + JSON 序列化开销

**排查**:
1. 对比 `Jackson.readValue` 性能
2. 对比 `IntentStore.save` 性能
3. 检查网络延迟

### Q: 高并发下性能下降

**原因**: 锁竞争或资源争用

**排查**:
1. 对比不同线程数测试结果
2. 查看 `ConcurrentHashMap` vs `IntentStore` 差距
3. 检查 GC 日志
