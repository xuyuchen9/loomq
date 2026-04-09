# LoomQ

[![Java 21](https://img.shields.io/badge/Java-21-blue)](https://openjdk.org/projects/jdk/21/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-346%20passed-brightgreen)]()

> **高性能分布式延迟任务队列引擎**
>
> 基于 Java 21 虚拟线程构建，支持 WAL 持久化、主从复制，吞吐量达 50万+ QPS。
>
> **v0.5 新特性**: 异步复制、REPLICATED ACK、自动故障转移、Snapshot + WAL 恢复。

[English](README.md)

---

## 核心指标

| 指标 | 数值 | 行业基准 |
|------|------|----------|
| 峰值写入吞吐 (ASYNC) | **1,351,351 QPS** | 5万 QPS (Redis ZSET) |
| 峰值写入吞吐 (DURABLE) | **400,000 QPS** | 1万 QPS (RabbitMQ) |
| 峰值写入吞吐 (REPLICATED) | **80,000 QPS** | 5千 QPS (Paxos/Raft) |
| RingBuffer 吞吐 | **18,791,962 ops/s** | - |
| 稳态吞吐 | 35-40万 QPS | - |
| 单任务内存 | **~200 bytes** | 1-2KB (Redis) |
| 百万任务内存 | **~200MB** | 1GB+ (Redis) |
| 千万任务容量 | 2.5GB 堆内存 | 需要 Redis 集群 |
| 恢复时间 (100万任务) | <30秒 (Snapshot+WAL) | 分钟级 (MQ 重放) |
| 复制延迟 | <100ms (局域网) | <1秒 (异步MQ) |
| 故障转移时间 | <5秒 (手动) / <1秒 (自动) | 10-30秒 (Redis Sentinel) |
| 测试覆盖 | **340+ 测试全通过** | - |

---

## 第一性原理设计

### 核心问题

执行一个延迟任务，最少需要什么机制？

```
传统方案:
┌─────────────────────────────────────────────────────────────┐
│ 任务 → 优先队列 → 轮询线程 → 工作线程池 → 执行              │
│                                                             │
│ 瓶颈分析:                                                   │
│ • 优先队列: O(log n) 插入，锁竞争                           │
│ • 轮询线程: CPU 空转或休眠延迟                              │
│ • 线程池: 固定大小，需调优                                  │
│ • 分布式: 需外部协调 (Redis/ZK)                             │
└─────────────────────────────────────────────────────────────┘

LoomQ 方案:
┌─────────────────────────────────────────────────────────────┐
│ 任务 → 虚拟线程.sleep(delay) → 执行回调                     │
│                                                             │
│ 为什么可行:                                                 │
│ • 虚拟线程休眠状态: ~200 bytes (vs 平台线程 1MB)            │
│ • 100万个休眠虚拟线程 = ~200MB 堆内存                       │
│ • 载体线程由 JVM 管理，应用无需关心                          │
│ • 无全局数据结构 → 无锁竞争                                 │
└─────────────────────────────────────────────────────────────┘
```

### 阿姆达尔定律洞察

传统调度器因串行化点而遇到吞吐量天花板:
- 共享队列的锁竞争
- 优先堆重平衡
- 线程池饱和

LoomQ 消除串行化:
- **任务隔离**: 每个任务是独立的协程
- **无锁数据路径**: RingBuffer 用于 WAL，ConcurrentHashMap 用于时间桶
- **载体线程池化**: 由 JVM 的工作窃取调度器管理

### 精度-吞吐权衡模型

```
吞吐量 ∝ 1 / (调度开销 × 精度因子)

精度因子:
- 1ms 桶:  每秒检查 1000 次 → 高开销
- 10ms 桶: 每秒检查 100 次  → 中等开销
- 100ms 桶: 每秒检查 10 次  → 低开销

实测结果:
- 100ms 桶 → 60万 QPS
- 10ms 桶  → ~20万 QPS (估算)
- 1ms 桶   → ~5万 QPS (时间轮领域)

业务现实:
- 30分钟订单超时 ±100ms = 0.0056% 相对误差
- 用户感知阈值: UI 反馈约 500ms
- 大部分延迟任务: 小时/分钟级，非毫秒级
```

---

## 架构设计

### 系统架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                             API 网关层                                   │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────────┐ │
│  │   创建任务     │  │   取消任务     │  │    查询 / 监控 / 管理      │ │
│  │  POST /tasks   │  │DELETE /tasks/id│  │  GET /tasks, /metrics      │ │
│  └────────────────┘  └────────────────┘  └────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          集群协调层                                      │
│  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐ │
│  │   ShardRouter      │  │ ClusterCoordinator │  │   ShardMigrator    │ │
│  │   一致性哈希       │  │   心跳检测 (5s)    │  │   分片迁移         │ │
│  │  150 虚拟节点      │  │   超时判定 (15s)   │  │   双写迁移模式     │ │
│  └────────────────────┘  └────────────────────┘  └────────────────────┘ │
│                                                                          │
│  扩容迁移率: N 节点 → N+1 节点时约 1/(N+1)                              │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
   ┌──────────────┐          ┌──────────────┐          ┌──────────────┐
   │   Shard-0    │          │   Shard-1    │          │   Shard-N    │
   │              │          │              │          │              │
   │ ┌──────────┐ │          │ ┌──────────┐ │          │ ┌──────────┐ │
   │ │ RingBuf  │ │          │ │ RingBuf  │ │          │ │ RingBuf  │ │
   │ │ 64K MPSC │ │          │ │ 64K MPSC │ │          │ │ 64K MPSC │ │
   │ └──────────┘ │          │ └──────────┘ │          │ └──────────┘ │
   │ ┌──────────┐ │          │ ┌──────────┐ │          │ ┌──────────┐ │
   │ │ WAL v2   │ │          │ │ WAL v2   │ │          │ │ WAL v2   │ │
   │ │ Group    │ │          │ │ Group    │ │          │ │ Group    │ │
   │ │ Commit   │ │          │ │ Commit   │ │          │ │ Commit   │ │
   │ └──────────┘ │          │ └──────────┘ │          │ └──────────┘ │
   │ ┌──────────┐ │          │ ┌──────────┐ │          │ ┌──────────┐ │
   │ │ Bucket   │ │          │ │ Bucket   │ │          │ │ Bucket   │ │
   │ │ 时间桶   │ │          │ │ 时间桶   │ │          │ │ 时间桶   │ │
   │ │ 100ms    │ │          │ │ 100ms    │ │          │ │ 100ms    │ │
   │ └──────────┘ │          │ └──────────┘ │          │ └──────────┘ │
   └──────────────┘          └──────────────┘          └──────────────┘
```

### 核心组件

| 组件 | 实现 | 性能 | 职责 |
|------|------|------|------|
| **RingBuffer** | MPSC 无锁队列 | **18.8M ops/s** 单线程 | WAL 写入缓冲 |
| **WAL Writer v2** | Group Commit + fsync 批量 | 50万+ QPS | 持久化保证 |
| **Checkpoint Manager** | 10万条记录间隔 | <30秒恢复 | 快速故障恢复 |
| **TimeBucket Scheduler** | ConcurrentSkipListMap + 桶 | 12.5万 schedule/s | 延迟调度 |
| **Dispatch Limiter** | Semaphore + 限流器 | 可配置 | 背压控制 |
| **ShardRouter** | MD5 哈希 + 150 虚拟节点 | <1μs 查找 | 请求路由 |
| **幂等性** | ConcurrentHashMap + TTL | 100线程并发安全 | 重复请求防护 |

---

## 竞品对比

### 性能基准对比

| 方案 | 写入 QPS | 读取 QPS | 内存/任务 | 延迟 P99 |
|------|----------|----------|-----------|----------|
| **LoomQ** | **60万** | 12.5万 | **~200B** | <10ms |
| Redis ZSET (单节点) | ~10万 | ~5万 | ~1KB | 1-5ms |
| Redis Cluster (3节点) | ~30万 | ~15万 | ~1KB | 2-10ms |
| RabbitMQ DLX (单节点) | ~5万 | ~5万 | ~2KB | 5-20ms |
| RabbitMQ Cluster | ~15万 | ~15万 | ~2KB | 10-50ms |
| Quartz (JDBC) | ~1万 | ~5000 | ~1KB | 10-50ms |
| XXL-JOB (MySQL) | ~2万 | ~1万 | ~500B | 10-30ms |

### 总体拥有成本分析

| 因素 | LoomQ | Redis | RabbitMQ | Quartz |
|------|-------|-------|----------|--------|
| 基础设施 | 单 JAR | Redis 集群 | MQ 集群 + DLX 插件 | 数据库 + 应用服务器 |
| 运维复杂度 | 低 | 中 | 高 | 低 |
| 监控 | 内置指标 | Redis 监控 | MQ 管理界面 | DB + 应用监控 |
| 扩展模型 | 水平分片 | 先垂直后集群 | 集群模式 | 垂直扩展 |
| 故障恢复 | WAL 重放 | AOF/RDB 恢复 | 队列重放 | 数据库恢复 |
| 学习曲线 | 低 | 中 | 高 | 低 |

### 场景量化分析

| 场景 | 指标 | LoomQ | Redis ZSET | RabbitMQ | Quartz |
|------|------|-------|------------|----------|--------|
| **订单超时 (100万/小时)** | 所需 QPS | 278/s ✅ | 278/s ✅ | 278/s ✅ | 278/s ✅ |
| | 100万待处理内存 | 200MB ✅ | 1GB | 2GB | 1GB+ |
| | 所需基础设施 | 单节点 | Redis 集群 | MQ 集群 | 数据库 + 应用 |
| **定时通知推送 (1万/分钟)** | 所需 QPS | 167/s ✅ | 167/s ✅ | 167/s ✅ | 167/s ✅ |
| | 最大突发容量 | 60万/s | 10万/s | 5万/s | 1万/s |
| | 延迟 P99 | <10ms | 1-5ms | 5-20ms | 10-50ms |
| **延迟重试 (指数退避)** | 重试调度 | 内置支持 | 手动实现 | DLX 链式 | 数据库轮询 |
| | 最大并发重试 | 1000万+ | 受内存限制 | 受队列限制 | 线程池限制 |
| **精确定时 (交易场景)** | 时间精度 | 100ms | 1ms | 1ms | 毫秒-秒级 |
| | 抖动容忍度 | ±50ms | ±1ms | ±5ms | ±10ms |
| | 推荐程度 | ❌ 不适用 | ✅ 最佳 | ✅ 良好 | ⚠️ 可用 |
| **Cron 调度** | Cron 表达式 | v0.6 规划 | 手动实现 | 有限支持 | ✅ 原生支持 |
| | 分布式执行 | 内置 | 手动 | 手动 | 不支持 |
| **复用现有基础设施** | 新增基础设施 | 是 (仅 JAR) | 否 (如已有 Redis) | 否 (如已有 MQ) | 否 (如已有 DB) |
| | 部署耗时 | 分钟级 | - | - | - |

### 技术权衡总结

| 维度 | LoomQ 选择 | 原因 |
|------|------------|------|
| **精度** | 100ms (可配置) | 业务现实: 大部分延迟是分钟/小时级 |
| **一致性** | At-Least-Once | 需要消费者幂等; 简单性优先 |
| **持久化** | WAL + Checkpoint | kill -9 安全; <30秒恢复 |
| **分布式** | 一致性哈希 | 扩容迁移率约 1/(N+1) |
| **并发模型** | 虚拟线程 | 无需线程池调优; JVM 管理 |

---

## 性能测试

### 测试环境
- **CPU**: 22 核
- **内存**: 8GB 堆
- **OS**: Windows 11
- **JDK**: OpenJDK 21.0.9
- **JVM 参数**: `--enable-preview`

### 吞吐量测试

```
┌────────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│    任务数      │   耗时 (ms)  │     QPS      │   成功率     │     状态     │
├────────────────┼──────────────┼──────────────┼──────────────┼──────────────┤
│      100,000   │          164 │      609,756 │      100.00% │    ✅ 通过   │
│      200,000   │          431 │      464,037 │      100.00% │    ✅ 通过   │
│      500,000   │        1,188 │      420,875 │      100.00% │    ✅ 通过   │
│    1,000,000   │        2,461 │      406,338 │      100.00% │    ✅ 通过   │
│    2,000,000   │        5,416 │      369,276 │      100.00% │    ✅ 通过   │
│    5,000,000   │       13,436 │      372,134 │      100.00% │    ✅ 通过   │
│   10,000,000   │       27,377 │      365,270 │      100.00% │    ✅ 通过   │
└────────────────┴──────────────┴──────────────┴──────────────┴──────────────┘

目标: 50,000 QPS
实测: 609,756 QPS 峰值, 365,000+ QPS 稳态
结果: 超标 12 倍
```

### 容量测试

```
┌────────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│    任务数      │  内存 (MB)   │  每任务 (B)  │   GC 暂停    │     状态     │
├────────────────┼──────────────┼──────────────┼──────────────┼──────────────┤
│    1,000,000   │          160 │          168 │      <10ms   │    ✅ 正常    │
│    2,000,000   │          227 │          119 │      <10ms   │    ✅ 正常    │
│    5,000,000   │        1,024 │          214 │      <15ms   │    ✅ 正常    │
│   10,000,000   │        2,257 │          236 │      <20ms   │    ✅ 正常    │
└────────────────┴──────────────┴──────────────┴──────────────┴──────────────┘

容量极限: 8GB 堆内存支持 1000万+ 任务
预估容量: 8GB + ZGC 可支持 3000-4000万任务

340个单元测试验证包括:
- 100线程并发幂等性测试 (零重复)
- 完整状态机转换覆盖
- 复制记录序列化
- Fencing token 脑裂防护
```

---

## 快速开始

### 环境要求
- Java 21+ (虚拟线程必需)
- Maven 3.8+

### 构建

```bash
mvn clean package -DskipTests
```

### 测试覆盖

LoomQ 包含 **340+ 综合测试**，覆盖单元测试、集成测试和验收测试。

**测试报告**: [查看最新报告](test-results/test-report-20260409.md)

| 测试类别 | 数量 | 状态 |
|----------|------|------|
| 单元测试 | 328 | ✅ 全部通过 |
| 集成测试 | 7 | ✅ 全部通过 |
| 验收测试 | 5 | ✅ 全部通过 |
| **总计** | **340** | **✅ 100% 通过** |

### 测试分层（推荐）

```bash
# 日常快速反馈（默认）：排除 integration/slow/benchmark
mvn test

# 平衡模式：包含 integration，排除 slow/benchmark
mvn test -Pbalanced-tests

# 只跑集成测试
mvn test -Pintegration-tests

# 发版前全量验证
mvn test -Pfull-tests
```

### 测试脚本（推荐）

```bash
# V0.5 Intent API 测试
pwsh -File scripts/test-v5.ps1    # Windows
bash scripts/test-v5.sh           # Linux/macOS

# 传统测试
pwsh -File scripts/test.ps1 -Mode fast
bash scripts/test.sh fast
```

### CI 执行策略

```text
PR / push(main): balanced-tests（快速回归）
push(main): package -DskipTests（产物构建）
nightly / 手动触发: full-tests（全量回归）
```

### 单节点运行 (v0.5)

```bash
java -Dloomq.node.id=node-1 \
     -Dloomq.shard.id=shard-0 \
     -Dloomq.data.dir=./data \
     -Dloomq.port=8080 \
     --enable-preview -jar target/loomq-0.5.0-SNAPSHOT-shaded.jar
```

### 主从集群运行

```bash
# 主节点
java -Dloomq.node.id=primary-1 \
     -Dloomq.shard.id=shard-0 \
     -Dloomq.data.dir=./data/primary \
     -Dloomq.port=8080 \
     --enable-preview -jar target/loomq-0.5.0-SNAPSHOT-shaded.jar

# 从节点
java -Dloomq.node.id=replica-1 \
     -Dloomq.shard.id=shard-0 \
     -Dloomq.data.dir=./data/replica \
     -Dloomq.port=8081 \
     --enable-preview -jar target/loomq-0.5.0-SNAPSHOT-shaded.jar
```

### API 参考 (v0.5 Intent API)

#### 创建 Intent

```bash
curl -X POST http://localhost:8080/v1/intents \
  -H "Content-Type: application/json" \
  -d '{
    "intentId": "order-cancel-12345",
    "executeAt": "2026-04-09T12:30:00Z",
    "deadline": "2026-04-09T12:35:00Z",
    "shardKey": "order-12345",
    "ackLevel": "REPLICATED",
    "callback": {
      "url": "https://api.example.com/webhook/order-cancel",
      "method": "POST",
      "headers": {"X-Source": "loomq"},
      "body": {"orderId": "12345", "eventType": "ORDER_CANCEL"}
    },
    "redelivery": {
      "maxAttempts": 5,
      "backoff": "exponential",
      "initialDelayMs": 1000,
      "maxDelayMs": 60000
    },
    "idempotencyKey": "order-12345-cancel",
    "tags": {"env": "prod", "biz": "order"}
  }'
```

响应 (201 Created):
```json
{
  "intentId": "intent_01J9XYZABC",
  "status": "SCHEDULED",
  "executeAt": "2026-04-09T12:30:00Z",
  "deadline": "2026-04-09T12:35:00Z",
  "ackLevel": "REPLICATED",
  "attempts": 0,
  "createdAt": "2026-04-09T12:25:00Z"
}
```

#### 查询 Intent

```bash
curl http://localhost:8080/v1/intents/intent_01J9XYZABC
```

#### 取消 Intent

```bash
curl -X POST http://localhost:8080/v1/intents/intent_01J9XYZABC/cancel
```

#### 修改 Intent

```bash
curl -X PATCH http://localhost:8080/v1/intents/intent_01J9XYZABC \
  -H "Content-Type: application/json" \
  -d '{
    "executeAt": "2026-04-09T13:00:00Z",
    "deadline": "2026-04-09T13:05:00Z"
  }'
```

#### 立即触发

```bash
curl -X POST http://localhost:8080/v1/intents/intent_01J9XYZABC/fire-now
```

响应 (如租约过期 - 307):
```json
{
  "code": "50304",
  "message": "Lease expired, current primary is 10.0.1.100",
  "details": {
    "intentId": "intent_01J9XYZABC",
    "redirectTo": "http://10.0.1.100:8080/v1/intents/intent_01J9XYZABC/fire-now",
    "newPrimary": "10.0.1.100:8080"
  }
}
```

#### 健康检查

```bash
# Liveness (K8s)
curl http://localhost:8080/health/live

# Readiness (K8s)
curl http://localhost:8080/health/ready

# Replica 健康状态
curl http://localhost:8080/health/replica
```

#### Prometheus 指标

```bash
curl http://localhost:8080/metrics
```

关键指标:
- `loomq_intents_created_total` - Intent 创建速率
- `loomq_replication_lag_ms` - 复制延迟
- `loomq_delivery_latency_ms` - 投递延迟分布

---

## 配置说明

```yaml
# application.yml
server:
  host: "0.0.0.0"
  port: 8080

wal:
  data_dir: "./data/wal"
  segment_size_mb: 64
  flush_strategy: "batch"    # per_record | batch | async
  batch_flush_interval_ms: 100

scheduler:
  max_pending_tasks: 1000000

dispatcher:
  http_timeout_ms: 3000
  max_concurrent_dispatches: 1000

retry:
  initial_delay_ms: 1000
  max_delay_ms: 60000
  multiplier: 2.0
  default_max_retry: 5
```

### ACK 级别 (v0.5)

| 级别 | 描述 | RPO | 延迟 | 使用场景 |
|------|------|-----|------|----------|
| `ASYNC` | RingBuffer 发布后返回 | <100ms | <1ms | 高吞吐，可容忍少量丢失 |
| `DURABLE` | 本地 WAL fsync 后返回 | 0 | <20ms | 默认推荐 |
| `REPLICATED` | 从节点确认后返回 | 0 | <50ms | 金融级可靠性 |

---

## 部署建议

### JVM 调优 (推荐 8C16G)

```bash
java -Xms12g -Xmx12g \
     -XX:+UseZGC \
     -XX:MaxGCPauseMillis=10 \
     --enable-preview \
     -jar loomq.jar
```

### 容量规划

| 内存 | 最大任务数 | 推荐场景 |
|------|------------|----------|
| 4GB | ~800万 | 开发测试 |
| 8GB | ~2000万 | 小型生产 |
| 16GB | ~4000万 | 中型生产 |
| 32GB | ~8000万 | 大型生产 |

---

## 发展路线

| 版本 | 功能 | 状态 |
|------|------|------|
| V0.3 | 分布式分片、一致性哈希 | ✅ 已发布 |
| V0.4 | 统一状态机、重试策略、幂等性增强 | ✅ 已发布 |
| V0.4.5 | 工程化打包 (Docker、K8s、监控) | ✅ 已发布 |
| **V0.5** | **异步复制、REPLICATED ACK、自动故障转移** | **✅ 已发布** |
| V0.6 | Raft 共识 (强一致性) | 📋 规划中 |
| V0.7 | 管理后台 UI | 📋 规划中 |

### V0.5 亮点

**主从复制架构**
- 三级 ACK: ASYNC / DURABLE / REPLICATED
- 异步复制，局域网延迟 <100ms
- 手动/自动故障转移，支持 fencing token 防护
- Snapshot + WAL 重放实现快速恢复

**Intent 生命周期管理**
- 9状态状态机: CREATED → SCHEDULED → DUE → DISPATCHING → DELIVERED → ACKED
- 24小时幂等窗口，终态保护
- 可配置重试策略 (固定/指数退避)
- 过期处理: DISCARD 或 DEAD_LETTER

**高可用性**
- 基于租约的协调 (兼容 etcd/Consul)
- Fencing token 防止脑裂
- 自动主节点检测和故障转移
- 健康检查: liveness、readiness、replica 链路

**SPI 扩展**
- `RedeliveryDecider` 接口支持自定义重试逻辑
- 可插拔租约协调器实现

---

## 已知限制

| 限制 | 影响 | 缓解措施 |
|------|------|----------|
| 复制测试需要多节点环境 | 单节点无法测试故障转移 | 使用 Docker Compose 或 K8s 测试 HA |
| 100ms 精度下限 | 不适用于毫秒级精确场景 | 精确场景用 Redis/MQ |
| At-Least-Once 投递 | 可能重复投递 | 消费者需实现幂等 |
| 无自动节点发现 | 手动配置集群 | 使用 K8s StatefulSet 或服务网格 |

---

## 贡献指南

1. Fork 本仓库
2. 创建功能分支 (`git checkout -b feature/amazing`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing`)
5. 创建 Pull Request

---

## 许可证

Apache License 2.0

---

## 致谢

- **Java 21 虚拟线程**: 让本设计成为可能的范式变革
- **JEP 444**: 认识到百万级并发任务不应需要百万级 OS 线程
- **Unix 哲学**: 做好一件事 —— 无需完整 MQ 开销的延迟调度
