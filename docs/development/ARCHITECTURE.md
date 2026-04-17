# LoomQ 架构文档

## 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│                        API Layer                             │
│                    (TaskControllerV3)                        │
├─────────────────────────────────────────────────────────────┤
│                      Domain Layer                            │
│           (TaskV3, TaskLifecycleV3, TaskStatusV3)           │
├─────────────────────────────────────────────────────────────┤
│                     Scheduler Layer                          │
│              (TimeBucketScheduler, PrecisionScheduler)       │
├─────────────────────────────────────────────────────────────┤
│                     Execution Layer                          │
│              (WebhookExecutor, DispatchLimiter)              │
├─────────────────────────────────────────────────────────────┤
│                       WAL Layer                              │
│          (AsyncWalWriter, CheckpointManager)                 │
├─────────────────────────────────────────────────────────────┤
│                     Recovery Layer                           │
│                   (RecoveryServiceV3)                        │
├─────────────────────────────────────────────────────────────┤
│                     Cluster Layer                            │
│         (ClusterManager, ShardRouter, ShardMigrator)         │
├─────────────────────────────────────────────────────────────┤
│                     Storage Layer                            │
│                    (TaskStoreV3)                             │
└─────────────────────────────────────────────────────────────┘
```

## 模块职责

### API Layer

- **TaskControllerV3**: HTTP API 控制器
  - 任务创建、查询、取消、修改
  - 参数校验、幂等处理
  - 审计日志记录

### Domain Layer

- **TaskV3**: 任务实体，包含完整任务数据
- **TaskStatusV3**: 状态枚举，定义 10 种状态
- **TaskLifecycleV3**: 生命周期管理器，原子状态转换

### Scheduler Layer

- **TimeBucketScheduler**: 时间桶调度器
  - 100ms 粒度时间桶
  - ConcurrentSkipListMap 有序索引
  - 批量任务推进
- **PrecisionScheduler**: 高精度调度器（备选）

### Execution Layer

- **WebhookExecutor**: Webhook 执行器
- **DispatchLimiter**: 分发限流器
  - 信号量限流
  - 背压控制

### WAL Layer

- **AsyncWalWriter**: 异步 WAL 写入器
  - RingBuffer 缓冲
  - Group Commit
- **CheckpointManager**: 检查点管理器
  - 定期创建检查点
  - 加速恢复

### Recovery Layer

- **RecoveryServiceV3**: 恢复服务
  - WAL 重放
  - 状态重建
  - In-flight 任务重新执行

### Cluster Layer

- **ClusterManager**: 集群管理器
- **ShardRouter**: 一致性哈希路由
- **ShardMigrator**: 分片迁移

### Storage Layer

- **TaskStoreV3**: 内存任务存储
  - 多维度索引
  - 原子操作

## 数据流

### 任务创建流程

```
Client → API Layer
       → 参数校验
       → 幂等检查 (TaskStore)
       → WAL 写入 (AsyncWalWriter)
       → 存储任务 (TaskStore)
       → 调度任务 (Scheduler)
       → 返回响应
```

### 任务执行流程

```
Scheduler (时间桶到期)
       → READY 状态
       → Execution Layer
       → RUNNING 状态
       → Webhook 调用
       → SUCCESS / RETRY_WAIT / FAILED
       → WAL 写入
       → 状态更新
```

### 恢复流程

```
启动 → 加载 Checkpoint
     → 重放 WAL
     → 重建 TaskStore
     → 重新调度未完成任务
     → 重新执行 RUNNING 任务
```

## 状态机

```
                    ┌─────────────────────────────────────────┐
                    │                                         │
                    ▼                                         │
              ┌─────────┐                                     │
              │ PENDING │──────────────────────────────┐      │
              └────┬────┘                              │      │
                   │                                   │      │
                   ▼                                   ▼      │
              ┌──────────┐                       ┌──────────┐ │
              │SCHEDULED │───────────────────────▶│CANCELLED │ │
              └────┬─────┘                       └──────────┘ │
                   │                                            │
                   ▼                                            │
              ┌──────────┐                                     │
              │  READY   │──────────────────────────┐          │
              └────┬─────┘                          │          │
                   │                                ▼          │
                   ▼                          ┌──────────┐     │
              ┌──────────┐                     │CANCELLED │     │
              │ RUNNING  │──┬──┬──┐            └──────────┘     │
              └──────────┘  │  │  │                             │
                   │        │  │  │                             │
                   │        │  │  ▼                             │
                   │        │  │ ┌────────┐                     │
                   │        │  └─│ FAILED │                     │
                   │        │    └────────┘                     │
                   │        │                                   │
                   ▼        ▼                                   │
              ┌─────────┐ ┌───────────┐                         │
              │ SUCCESS │ │RETRY_WAIT │─────────────────────────┤
              └─────────┘ └─────┬─────┘                         │
                                │                               │
                                ▼                               │
                          ┌───────────┐                         │
                          │DEAD_LETTER│                         │
                          └───────────┘                         │
                                                                │
              ┌──────────┐                                     │
              │ EXPIRED  │◀────────────────────────────────────┘
              └──────────┘
```

## 关键设计决策

### 1. 时间桶调度

- **权衡**: 100ms 精度误差换取高吞吐
- **适用场景**: "订单30分钟后取消"等业务场景
- **可配置**: bucket-granularity-ms 可调整

### 2. WAL ACK 级别

- **ASYNC**: 立即返回，RPO < 100ms
- **DURABLE**: fsync 后返回，RPO = 0

### 3. 幂等策略

- **idempotencyKey**: 请求级幂等
- **bizKey**: 业务级幂等
- **终态处理**: 已完成任务拒绝重复创建

### 4. 虚拟线程

- 无线程池调优
- 百万级并发等待任务
- 自动调度

## 性能特性

| 指标 | 目标 |
|------|------|
| 任务创建 TPS | ≥ 10000 |
| 调度延迟 p99 | ≤ 200ms |
| 恢复时间(10万任务) | ≤ 30s |
| 内存占用(10万任务) | ≤ 1GB |
