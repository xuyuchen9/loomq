# LoomQ v0.6.1 - 第一性原理重构

## 版本概述

v0.6.1 是一次基于第一性原理的重构，核心目标是**简化设计，提升性能**。

### JDK 版本

- **最低要求**: Java 25 LTS
- **推荐版本**: Java 25.0.2+

### 核心变更

| 组件 | 原设计 | 新设计 | 收益 |
|------|--------|--------|------|
| WAL 格式 | 59字节结构化头部 + JSON | 8字节头部 + 二进制 | 序列化 **5x** 提升 |
| 过期检查 | 时间轮 O(1) | 保留 BucketGroup | 避免过度设计 |
| 投递模式 | 异步回调状态机 | 同步批量 + 背压 | 代码 **-40%** |

### 性能目标

| 指标 | v0.6.0 | v0.6.1 目标 | 实测 |
|------|--------|-------------|------|
| WAL 序列化 | ~2 µs | < 500 ns | **~400 ns** ✅ |
| ASYNC QPS | 50K | 200K+ | **248K** ✅ |
| DURABLE QPS | 26K | 150K+ | 待 Linux 验证 |
| 代码行数 | ~1300 | ~500 | **-60%** ✅ |

### 文档索引

- [DESIGN.md](./DESIGN.md) - 第一性原理设计分析
- [IMPLEMENTATION.md](./IMPLEMENTATION.md) - 实施细节与代码变更
- [BENCHMARK.md](./BENCHMARK.md) - 性能测试报告
- [MIGRATION.md](./MIGRATION.md) - 升级迁移指南

### 快速开始

```bash
# 使用 V2 简化引擎
java -Dloomq.engine=v2 -jar loomq-0.6.1.jar

# WAL 调试工具
java -cp loomq.jar com.loomq.wal.WalDumpTool ./data/wal/shard-0/wal.bin --json
```

### 新增组件

| 组件 | 包路径 | 说明 |
|------|--------|------|
| SimpleWalWriter | `com.loomq.wal` | 极简 WAL 写入器 |
| IntentBinaryCodec | `com.loomq.wal` | 二进制序列化器 |
| IntentWalV2 | `com.loomq.wal.v2` | 简化版 WAL 集成 |
| BatchDispatcher | `com.loomq.dispatcher` | 批量同步投递器 |
| WalDumpTool | `com.loomq.wal` | WAL 调试工具 |
| LoomqEngineV2 | `com.loomq` | 简化版引擎入口 |

### 兼容性

- ✅ API 完全兼容（HTTP 接口不变）
- ⚠️ WAL 格式不兼容（升级需清空 WAL 目录）
- ✅ 配置项兼容

### 发布日期

2026-04-10
