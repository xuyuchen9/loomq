# LoomQ v0.5 Async Replication 实现计划

**版本目标**: 从"可部署的延迟调度引擎"升级为"可恢复的分片调度引擎"
**核心目标**: 每个 shard 支持 1 primary + 1 replica，主节点故障后可自动切换

---

## 设计决策（已定稿）

| 决策 | 结论 | 核心原则 |
|------|------|----------|
| **传输层** | **Netty 长连接** | 内部控制平面，追求稳定、低延迟、可控，不选 HTTP/2 |
| **一致性** | **异步复制 + 三档 ACK** | ASYNC / DURABLE / REPLICATED，用户显式选择可靠性级别 |
| **Failover** | **Coordinator + lease + fencing** | v0.5 不引入 Raft，但必须满足：租约、版本号、fencing token、心跳超时 |
| **Catch-up** | **Snapshot + WAL replay** | 定义快照边界和 WAL 截点，支持断点续传与幂等恢复 |
| **复制对象** | **任务全生命周期事件** | 状态迁移、调度索引、取消/修改/触发、重试计数、DLQ 进入记录 |

### 硬约束（必须满足，否则 v0.5 不能发布）

1. **Coordinator 必须是"租约 + 版本号"仲裁，不是简单投票**
2. **Catch-up 必须先定义快照边界和 WAL 截点，否则 snapshot + replay 会对不上**
3. **复制必须包含调度索引，不能只复制任务元数据**
4. **Replay 必须幂等，同一条 record 重放两次不能破坏状态**

---

## 目录

1. [架构概览](#1-架构概览)
2. [组件清单](#2-组件清单)
3. [实现阶段](#3-实现阶段)
4. [详细设计](#4-详细设计)
5. [测试策略](#5-测试策略)
6. [风险与应对](#6-风险与应对)

---

## 1. 架构概览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           LoomQ Cluster v0.5                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────────────────┐    ┌─────────────────────────────────┐ │
│  │         Shard-0 (Primary)        │    │         Shard-0 (Replica)        │ │
│  │  ┌─────────────────────────┐    │    │  ┌─────────────────────────┐    │ │
│  │  │    ReplicationManager    │◄───┼────┼──►│    ReplicationManager    │    │ │
│  │  │  (primary/replica logic) │    │    │  │  (replica/standby logic)│    │ │
│  │  └─────────────────────────┘    │    │  └─────────────────────────┘    │ │
│  │           │                      │    │           │                      │ │
│  │           ▼                      │    │           ▼                      │ │
│  │  ┌─────────────────────────┐    │    │  ┌─────────────────────────┐    │ │
│  │  │      AsyncWalWriter      │────┼────┼──►│      AsyncWalWriter      │    │ │
│  │  │   (WAL + Replication)    │    │    │  │   (apply + persist)      │    │ │
│  │  └─────────────────────────┘    │    │  └─────────────────────────┘    │ │
│  │           │                      │    │           │                      │ │
│  │           ▼                      │    │           ▼                      │ │
│  │  ┌─────────────────────────┐    │    │  ┌─────────────────────────┐    │ │
│  │  │    TimeBucketScheduler   │    │    │  │    TimeBucketScheduler   │    │ │
│  │  │   (只读 / 待命模式)       │    │    │  │   (激活后接管调度)        │    │ │
│  │  └─────────────────────────┘    │    │  └─────────────────────────┘    │ │
│  └─────────────────────────────────┘    └─────────────────────────────────┘ │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                     ClusterCoordinator                               │    │
│  │              (心跳检测、failover、路由表管理)                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.1 核心流程

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Client      │────►│  Primary     │────►│  Local WAL   │────►│  Replica     │
│  (REPLICATED)│     │  Engine      │     │  + Async Send│     │  (fsync)     │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                    │                   │                    │
       │                    │                   │                    │
       │                    ▼                   ▼                    ▼
       │              ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
       │              │  Ack Primary │     │  Record      │     │  Ack Replica │
       │              │  (DURABLE)   │     │  Offset      │     │  (REPLICATED)│
       │              └──────────────┘     └──────────────┘     └──────────────┘
       │                                                           │
       │◄──────────────────────────────────────────────────────────┘
       │                     Combined Ack
       ▼
┌──────────────┐
│  Response    │
│  (confirmed) │
└──────────────┘
```

---

## 2. 组件清单

### 2.1 新增组件 (11个)

| 组件 | 包路径 | 职责 |
|------|--------|------|
| **ReplicationManager** | `replication/` | 复制逻辑主控，管理 primary/replica 角色 |
| **ReplicationRecord** | `replication/` | 复制记录格式定义（含 offset、payload、checksum） |
| **ReplicationStream** | `replication/` | 复制流传输抽象（双向通信） |
| **ReplicaClient** | `replication/` | primary 侧的 replica 连接客户端 |
| **ReplicaServer** | `replication/` | replica 侧的复制服务接收端 |
| **HeartbeatManager** | `cluster/` | 心跳检测与故障发现 |
| **FailoverController** | `cluster/` | 故障转移控制器 |
| **ShardStateMachine** | `cluster/` | 分片状态机（REPLICA_INIT → SYNCED → PRIMARY） |
| **CatchUpService** | `recovery/` | 副本追赶服务（全量+增量同步） |
| **ReplicationMetrics** | `metrics/` | 复制链路指标收集 |
| **ReplicationConfig** | `config/` | 复制相关配置 |

### 2.2 修改组件 (6个)

| 组件 | 修改内容 |
|------|----------|
| **AsyncWalWriter** | 添加复制钩子，写入后触发异步复制 |
| **ShardRouter** | 支持路由表版本号，failover 后版本递增 |
| **ClusterCoordinator** | 集成心跳检测、failover 决策 |
| **TaskStore** | 添加幂等性校验，防止重复应用 |
| **TaskStatus** | 添加复制相关状态标记 |
| **application.yml** | 新增 replication 配置段 |

### 2.3 依赖新增

```xml
<!-- 网络传输 -->
<dependency>
    <groupId>io.netty</groupId>
    <artifactId>netty-all</artifactId>
    <version>4.1.107.Final</version>
</dependency>

<!-- 或者使用现有 HTTP client 实现 -->
```

---

## 3. 实现阶段

### 阶段 1: 基础复制协议 (Week 1)
**目标**: 定义复制格式，建立 primary-replica 通信通道

#### 任务清单

- [ ] **T1.1** 定义 `ReplicationRecord` 格式
  - `long offset` - 全局偏移量
  - `byte[] payload` - WAL 记录内容
  - `long checksum` - CRC32C 校验
  - `long timestamp` - 时间戳
  - `RecordType type` - 记录类型（CREATE/CANCEL/UPDATE/DELETE）

- [ ] **T1.2** 实现 `ReplicationStream` 抽象
  - 基于 Netty 或 HTTP/2 长连接
  - 支持双向流（primary→replica 数据，replica→primary ACK）
  - 连接保活与重连机制

- [ ] **T1.3** 实现 `ReplicaServer` (replica 侧)
  - 监听复制端口
  - 接收 replication records
  - 写入本地 WAL
  - 返回 ACK 确认

- [ ] **T1.4** 实现 `ReplicaClient` (primary 侧)
  - 维护到 replica 的连接
  - 异步发送 replication records
  - 管理发送缓冲区与重试
  - 接收 ACK 并更新 `lastReplicatedOffset`

#### 验收标准
```java
// Primary 能发送，Replica 能接收并持久化
ReplicationClient client = new ReplicaClient("replica-host", 9090);
client.connect();

ReplicationRecord record = ReplicationRecord.builder()
    .offset(100L)
    .payload(walData)
    .type(RecordType.CREATE)
    .build();

CompletableFuture<Ack> future = client.send(record);
Ack ack = future.get(5, TimeUnit.SECONDS);
assert ack.getOffset() == 100L;
assert ack.getStatus() == AckStatus.PERSISTED;
```

---

### 阶段 2: 集成 WAL 与复制 (Week 2)
**目标**: Primary 写入 WAL 后自动触发复制

#### 任务清单

- [ ] **T2.1** 扩展 `AsyncWalWriter`
  ```java
  public class AsyncWalWriter {
      private final ReplicationManager replicationManager;
      
      public CompletableFuture<WalWriteResult> append(
            byte[] data, 
            AckLevel ackLevel) {
          // 1. 写入本地 WAL
          // 2. 如果 ackLevel >= DURABLE，fsync
          // 3. 如果 replication 启用，发送给 replica
          // 4. 如果 ackLevel == REPLICATED，等待 replica ACK
      }
  }
  ```

- [ ] **T2.2** 实现 `ReplicationManager`
  - 管理 primary/replica 角色切换
  - 协调 WAL 写入与复制
  - 维护复制状态（lag、offset）

- [ ] **T2.3** 添加 `REPLICATED` ACK 级别
  ```java
  public enum AckLevel {
      ASYNC,      // 进入内存队列即返回
      DURABLE,    // 本地 WAL fsync 后返回
      REPLICATED  // primary fsync + replica fsync 后返回
  }
  ```

- [ ] **T2.4** 实现复制链路指标
  - `replication.lag.ms` - 复制延迟
  - `replication.ack.latency.ms` - replica ACK 延迟
  - `replication.sent.bytes` - 发送字节数
  - `replication.acked.offset` - 已确认 offset

#### 验收标准
```java
// 性能测试：开启复制后吞吐不低于基线 70%
BenchmarkResult result = runBenchmark(
    tasks = 100_000,
    ackLevel = AckLevel.REPLICATED,
    duration = Duration.ofMinutes(5)
);

assert result.getThroughputTps() >= BASELINE_TPS * 0.70;
assert result.getP99LatencyMs() < 500;
```

---

### 阶段 3: 心跳与故障检测 (Week 3)
**目标**: 实现自动故障发现与切换

#### 任务清单

- [ ] **T3.1** 实现 `HeartbeatManager`
  ```java
  @Scheduled(fixedDelay = 1000)
  public void sendHeartbeat() {
      Heartbeat heartbeat = Heartbeat.builder()
          .nodeId(shardId)
          .role(currentRole)
          .lastAppliedOffset(getLastAppliedOffset())
          .timestamp(Instant.now())
          .build();
      
      clusterCoordinator.broadcast(heartbeat);
  }
  ```

- [ ] **T3.2** 实现故障检测算法
  - 心跳超时判定（默认 5s）
  - 网络探测双重确认
  - 避免脑裂：必须收到 coordinator 确认才能切换

- [ ] **T3.3** 实现 `ShardStateMachine`
  ```
  REPLICA_INIT
       │
       ▼ (启动，加载本地 WAL)
  REPLICA_CATCHING_UP ◄──────┐
       │                     │
       ▼ (追平 offset)       │ (primary 有新数据)
  REPLICA_SYNCED ────────────┤
       │                     │
       ▼ (primary 故障，promotion)
  PROMOTING
       │
       ▼ (路由表更新完成)
  PRIMARY
       │
       ▼ (收到新 primary 心跳)
  DEGRADED (转为新 replica)
  ```

- [ ] **T3.4** 实现 `FailoverController`
  - 接收故障通知
  - 执行 promotion 流程
  - 更新路由表版本
  - 通知所有客户端

#### 验收标准
```java
// Failover 测试：primary 宕机，5s 内完成切换
@Test
public void testFailoverWithin5Seconds() {
    // 1. 正常写入任务
    String taskId = client.createTask("test");
    
    // 2. Kill primary
    primaryProcess.destroyForcibly();
    
    // 3. 计时
    long start = System.currentTimeMillis();
    
    // 4. 等待新 primary 接管
    await().atMost(5, TimeUnit.SECONDS)
           .until(() -> getPrimaryShard().isAvailable());
    
    // 5. 验证任务仍在
    Task task = client.getTask(taskId);
    assert task != null;
}
```

---

### 阶段 4: Catch-Up 与恢复 (Week 4)
**目标**: Replica 能追赶缺失数据，旧 primary 恢复后能重新加入

#### 任务清单

- [ ] **T4.1** 实现 `CatchUpService`
  - 全量同步：snapshot + WAL replay
  - 增量同步：从指定 offset 开始复制
  - 进度报告与可观测性

- [ ] **T4.2** 实现 offset 管理与校验
  ```java
  public class ReplicationOffset {
      private final long lastReplicatedOffset;  // primary 已发送
      private final long lastAckedOffset;       // replica 已确认
      private final long lastAppliedOffset;     // replica 已应用
  }
  ```

- [ ] **T4.3** 旧 primary 恢复流程
  - 检测到有新 primary
  - 自动转为 replica 角色
  - 执行 catch-up
  - 进入 SYNCED 状态待命

- [ ] **T4.4** 实现幂等性保证
  - TaskStore 添加 `processedOffsets` 集合
  - 重复 offset 直接返回成功，不重复应用

#### 验收标准
```java
// Catch-up 测试：replica 落后 10k 条记录，30s 内追平
@Test
public void testCatchUpWithin30Seconds() {
    // 1. 停止 replica
    replica.stopReplication();
    
    // 2. primary 写入 10k 任务
    for (int i = 0; i < 10000; i++) {
        primary.createTask("task-" + i);
    }
    
    // 3. 恢复 replica
    replica.startReplication();
    
    // 4. 验证追平
    await().atMost(30, TimeUnit.SECONDS)
           .until(() -> replica.getLastAppliedOffset() 
                   == primary.getLastReplicatedOffset());
}
```

---

### 阶段 5: 测试与验证 (Week 5)
**目标**: 全面测试覆盖，性能达标

#### 任务清单

- [ ] **T5.1** 单元测试
  - ReplicationRecord 编码/解码
  - Offset 递增与幂等性
  - 状态机转换
  - 角色切换逻辑

- [ ] **T5.2** 集成测试
  - 正常复制流程
  - Primary 宕机 failover
  - 网络抖动恢复
  - Replica 追赶
  - 双节点同时故障（降级模式）

- [ ] **T5.3** 故障演练测试
  ```bash
  # 脚本化故障注入
  ./chaos-test.sh \
    --scenario primary-death \
    --duration 300s \
    --verify-zero-data-loss
  ```

- [ ] **T5.4** 性能基准测试
  - 10k/100k/1M 任务压测
  - 对比 v0.4.5 基线
  - Failover 恢复时间测量

- [ ] **T5.5** 可观测性验证
  - Grafana 面板配置
  - 告警规则定义
  - 日志查询模板

---

## 4. 详细设计

### 4.1 ReplicationRecord 格式

```java
public class ReplicationRecord {
    // Header (固定 32 字节)
    private final long magicNumber = 0x4C51434F5059L; // "LQCOPY"
    private final short version = 1;
    private final short headerLength = 32;
    
    // Offset (8 字节)
    private final long offset;           // 全局单调递增 offset
    
    // Timestamp (8 字节)
    private final long timestamp;        // 写入时间戳
    
    // Record Type (1 字节)
    private final RecordType type;       // CREATE/CANCEL/UPDATE/DELETE/CHECKPOINT
    
    // Payload (变长)
    private final byte[] payload;        // WAL 记录内容
    
    // Checksum (8 字节，覆盖 header + payload)
    private final long checksum;         // CRC32C
}
```

### 4.2 状态机设计

```java
public enum ReplicaState {
    // Replica 侧状态
    REPLICA_INIT("初始化中"),
    REPLICA_CATCHING_UP("追赶中"),
    REPLICA_SYNCED("已同步，待命"),
    
    // Primary 侧状态
    PRIMARY_ACTIVE("主节点运行中"),
    PRIMARY_DEGRADED("主节点降级（replica 失联）"),
    
    // 切换中状态
    PROMOTING("正在提升为主节点"),
    DEMOTING("正在降级为副本"),
    
    // 异常状态
    OFFLINE("离线"),
    ERROR("错误状态");
    
    public boolean canAcceptWrite() {
        return this == PRIMARY_ACTIVE || this == PRIMARY_DEGRADED;
    }
    
    public boolean canAcceptRead() {
        return this == PRIMARY_ACTIVE || this == PRIMARY_DEGRADED 
            || this == REPLICA_SYNCED;
    }
}
```

### 4.3 路由表版本管理

```java
public class RoutingTable {
    private final long version;          // 单调递增版本号
    private final Map<String, ShardInfo> shards;  // shardId -> info
    
    public static class ShardInfo {
        private final String shardId;
        private final NodeInfo primary;
        private final NodeInfo replica;  // v0.5 新增
        private final ShardState state;
    }
    
    // 客户端缓存路由表，版本号旧则刷新
    public boolean isStale(long clientVersion) {
        return version > clientVersion;
    }
}
```

### 4.4 Coordinator 租约与仲裁（硬约束 #1）

```java
public class CoordinatorLease {
    // 租约 ID：唯一标识本次 primary 任期
    private final String leaseId;           // UUID
    
    // 持有者
    private final String holderNodeId;      // 当前 primary node ID
    
    // 时间边界
    private final Instant issuedAt;         // 发放时间
    private final Instant expiresAt;        // 过期时间（issuedAt + leaseDuration）
    
    // 版本信息
    private final long routingVersion;      // 关联的路由表版本
    
    // 续约机制：primary 必须定期续约，否则租约过期
    public boolean isValid() {
        return Instant.now().isBefore(expiresAt);
    }
    
    public boolean canRenew() {
        // 过期前 30% 时间窗口内可以续约
        long totalDuration = expiresAt.toEpochMilli() - issuedAt.toEpochMilli();
        long remaining = expiresAt.toEpochMilli() - Instant.now().toEpochMilli();
        return remaining < totalDuration * 0.3;
    }
}

public class ClusterCoordinator {
    // 关键：不是投票，是租约仲裁
    public Optional<CoordinatorLease> grantLease(String nodeId, long routingVersion) {
        // 1. 检查是否有有效租约
        if (currentLease != null && currentLease.isValid()) {
            // 只能由持有者续约
            if (!currentLease.getHolderNodeId().equals(nodeId)) {
                return Optional.empty();  // 拒绝，已有有效租约
            }
            // 续约
            return renewLease(nodeId);
        }
        
        // 2. 发放新租约
        return issueNewLease(nodeId, routingVersion + 1);
    }
    
    // 撤销租约（用于 failover）
    public void revokeLease(String leaseId, String reason) {
        // 只有 coordinator 能撤销，防止 split-brain
        currentLease = null;
        routingVersion++;
        notifyAllNodes(new LeaseRevokedEvent(leaseId, reason));
    }
}
```

### 4.5 Fencing Token（硬约束 #1 续）

```java
public class FencingToken {
    // 单调递增的 fencing 序列号
    private final long sequence;
    
    // 关联的租约 ID
    private final String leaseId;
    
    // 生成时间
    private final Instant timestamp;
    
    public boolean isValidFor(CoordinatorLease lease) {
        // token 必须与当前有效租约匹配
        return this.leaseId.equals(lease.getLeaseId()) 
            && lease.isValid();
    }
}

// 在 AsyncWalWriter 中使用
public class AsyncWalWriter {
    private final AtomicReference<FencingToken> currentToken;
    
    public CompletableFuture<WalWriteResult> append(byte[] data, AckLevel ackLevel) {
        // 每次写入前检查 fencing token
        FencingToken token = currentToken.get();
        if (token == null || !token.isValidFor(coordinator.getCurrentLease())) {
            throw new FencingTokenExpiredException(
                "This node is no longer the primary. Failover may have occurred.");
        }
        
        // 写入时携带 token，用于 replica 验证
        return doAppend(data, ackLevel, token);
    }
}
```

### 4.6 Snapshot 边界与 WAL 截点（硬约束 #2）

```java
public class SnapshotMetadata {
    // 快照唯一标识
    private final String snapshotId;        // UUID
    
    // 关键：快照包含的数据范围
    private final long startOffset;         // 包含
    private final long endOffset;           // 不包含，这是快照截点
    
    // 时间戳
    private final Instant createdAt;
    
    // 包含的任务统计
    private final int taskCount;
    private final Map<String, Long> stateCounts;  // PENDING->100, RUNNING->50...
    
    /**
     * 恢复时的规则：
     * 1. 加载 snapshot（包含 startOffset 到 endOffset-1 的状态）
     * 2. 从 endOffset 开始 replay WAL
     * 3. 任何 offset < endOffset 的 record 都幂等跳过
     */
    public long getReplayStartOffset() {
        return endOffset;
    }
}

public class CatchUpBoundary {
    // Catch-up 的三阶段边界
    
    // Stage 1: Snapshot 阶段
    private final SnapshotMetadata snapshot;
    
    // Stage 2: 追赶阶段（从 snapshot.endOffset 到 currentPrimaryOffset）
    private final long targetOffset;        // primary 当前最新 offset
    
    // Stage 3: 实时复制阶段（追平后切换）
    public boolean isCaughtUp(long appliedOffset) {
        return appliedOffset >= targetOffset;
    }
}

// 在 ReplicaServer 中的应用
public class ReplicaServer {
    public void applyReplicationRecord(ReplicationRecord record) {
        // 幂等检查：如果 offset 小于等于已应用的 snapshot 边界，跳过
        if (record.getOffset() < snapshotMetadata.getEndOffset()) {
            logger.debug("Skipping record {} - before snapshot boundary", 
                record.getOffset());
            return;  // 幂等跳过
        }
        
        // 已应用检查
        if (record.getOffset() <= lastAppliedOffset.get()) {
            logger.debug("Skipping record {} - already applied", 
                record.getOffset());
            return;  // 幂等跳过
        }
        
        // 顺序检查（确保不丢不乱）
        if (record.getOffset() != lastAppliedOffset.get() + 1) {
            throw new OutOfOrderException(
                "Expected offset " + (lastAppliedOffset.get() + 1) 
                + " but got " + record.getOffset());
        }
        
        // 应用记录
        applyToLocalState(record);
        lastAppliedOffset.set(record.getOffset());
    }
}
```

### 4.7 幂等性保证（硬约束 #4）

```java
public class IdempotentApplier {
    // 已处理 offset 集合（使用 RoaringBitmap 或类似压缩结构）
    private final RoaringBitmap processedOffsets;
    
    // 已确认 offset（用于 replica ACK）
    private final AtomicLong lastAckedOffset;
    
    /**
     * 幂等应用的核心逻辑
     * 
     * 规则：
     * 1. 如果 offset 已处理 -> 直接返回成功（不重复执行）
     * 2. 如果 offset 正好下一个 -> 执行并记录
     * 3. 如果 offset 乱序或空缺 -> 报错（上游重试）
     */
    public ApplyResult apply(ReplicationRecord record) {
        long offset = record.getOffset();
        
        // 规则 1：已处理
        if (processedOffsets.contains((int) offset)) {
            return ApplyResult.alreadyProcessed(offset);
        }
        
        // 规则 2：正好下一个
        if (offset == lastAckedOffset.get() + 1) {
            // 执行业务逻辑
            doApply(record);
            
            // 记录
            processedOffsets.add((int) offset);
            lastAckedOffset.set(offset);
            
            return ApplyResult.success(offset);
        }
        
        // 规则 3：乱序/空缺
        return ApplyResult.outOfOrder(offset, lastAckedOffset.get() + 1);
    }
}

// 在 TaskStore 中的应用
public class TaskStore {
    /**
     * 处理 CREATE 记录的幂等性
     * 场景：同一个任务创建记录被 replay 两次
     */
    public void createTask(Task task, long offset) {
        // 先检查是否已存在
        Task existing = tasks.get(task.getTaskId());
        if (existing != null) {
            // 检查来源 offset
            if (existing.getSourceOffset() == offset) {
                // 同一条记录，幂等返回
                logger.debug("Task {} already created from offset {}, ignoring", 
                    task.getTaskId(), offset);
                return;
            } else {
                // 不同 offset，但相同 taskId -> 严重错误（UUID 冲突）
                throw new DuplicateTaskException(
                    "Task " + task.getTaskId() + " already exists from offset " 
                    + existing.getSourceOffset() + ", cannot apply offset " + offset);
            }
        }
        
        // 新任务
        task.setSourceOffset(offset);
        tasks.put(task.getTaskId(), task);
    }
}
```

### 4.8 ACK 等待机制

```java
public class ReplicatedAckFuture {
    private final long offset;
    private final CompletableFuture<Ack> primaryAck;
    private final CompletableFuture<Ack> replicaAck;
    
    public CompletableFuture<Ack> combine() {
        return CompletableFuture.allOf(primaryAck, replicaAck)
            .thenApply(v -> {
                // 两者都成功才返回
                Ack p = primaryAck.join();
                Ack r = replicaAck.join();
                return new Ack(offset, AckStatus.REPLICATED);
            })
            .orTimeout(30, TimeUnit.SECONDS);  // 超时保护
    }
}
```

### 4.9 复制对象范围（决策 #5）

**原则**：复制必须包含任务全生命周期，不能只复制任务元数据。

```java
public enum ReplicationRecordType {
    // ========== 任务生命周期 ==========
    TASK_CREATE,           // 任务创建（含完整 Task 对象）
    TASK_CANCEL,           // 任务取消
    TASK_MODIFY_DELAY,     // 延迟修改
    TASK_TRIGGER_NOW,      // 立即触发
    
    // ========== 状态迁移 ==========
    STATE_TRANSITION,      // 状态变更：PENDING -> RUNNING -> SUCCESS/FAILURE
    STATE_TIMEOUT,         // 超时处理
    STATE_RETRY,           // 重试计数增加
    
    // ========== 调度索引 ==========
    INDEX_INSERT,          // 插入时间桶索引
    INDEX_REMOVE,          // 从时间桶移除
    INDEX_UPDATE,          // 更新调度时间
    
    // ========== 死信队列 ==========
    DLQ_ENTER,             // 进入死信队列
    DLQ_EXIT,              // 从死信队列恢复
    
    // ========== 检查点 ==========
    CHECKPOINT,            // 手动检查点（用于 snapshot 对齐）
    
    // ========== 系统事件 ==========
    NODE_PROMOTION,        // 节点提升为 primary
    NODE_DEMOTION,         // 节点降级为 replica
    LEASE_RENEWAL          // 租约续约
}

/**
 * 复制记录的完整结构
 */
public class ReplicationRecord {
    // 基础头部
    private final long offset;
    private final long timestamp;
    private final ReplicationRecordType type;
    private final String sourceNodeId;
    private final FencingToken fencingToken;
    
    // 类型特定 payload
    private final byte[] payload;
    
    // 为什么必须包含调度索引？
    // 场景：primary 把任务放入时间桶 "bucket-100ms-12345"
    // 如果 replica 没有这个索引，failover 后任务永远不会被调度
    // 所以 INDEX_INSERT/REMOVE/UPDATE 必须复制
}

/**
 * 复制范围验证清单
 * 
 * 以下场景必须在 replica 上可恢复：
 * 
 * 1. 任务创建后 failover
 *    - replica 必须有 TASK_CREATE 记录
 *    - replica 必须有 INDEX_INSERT 记录
 *    - failover 后任务能被正常调度
 * 
 * 2. 任务取消后 failover  
 *    - replica 必须有 TASK_CANCEL 记录
 *    - replica 必须有 INDEX_REMOVE 记录
 *    - failover 后任务不会被执行
 * 
 * 3. 重试计数变化后 failover
 *    - replica 必须有 STATE_RETRY 记录
 *    - failover 后重试计数正确，不会无限重试
 * 
 * 4. 进入 DLQ 后 failover
 *    - replica 必须有 DLQ_ENTER 记录
 *    - failover 后任务在 DLQ 中，不会丢失
 */
public class ReplicationCoverageTest {
    
    @Test
    public void testTaskCreationReplication() {
        // Primary 创建任务
        String taskId = primary.createTask("test-data");
        
        // 验证 replica 收到 TASK_CREATE + INDEX_INSERT
        await().until(() -> replica.hasRecord(ReplicationRecordType.TASK_CREATE, taskId));
        await().until(() -> replica.hasRecord(ReplicationRecordType.INDEX_INSERT, taskId));
        
        // Failover
        failover(primary, replica);
        
        // 新 primary 能调度该任务
        Task task = replica.getTask(taskId);
        assertNotNull(task);
        assertTrue(replica.getScheduler().isScheduled(taskId));
    }
    
    @Test
    public void testRetryCountReplication() {
        // Primary 创建任务，重试 3 次
        String taskId = primary.createTask(TaskRequest.builder()
            .maxRetries(3)
            .build());
        
        // 模拟 2 次失败
        primary.simulateFailure(taskId);
        primary.simulateFailure(taskId);
        
        // 验证 replica 收到 2 次 STATE_RETRY
        assertEquals(2, replica.countRecords(ReplicationRecordType.STATE_RETRY, taskId));
        
        // Failover
        failover(primary, replica);
        
        // 新 primary 只再重试 1 次（不是 3 次）
        assertEquals(1, replica.getRemainingRetries(taskId));
    }
}
```

### 4.4 ACK 等待机制

```java
public class ReplicatedAckFuture {
    private final long offset;
    private final CompletableFuture<Ack> primaryAck;
    private final CompletableFuture<Ack> replicaAck;
    
    public CompletableFuture<Ack> combine() {
        return CompletableFuture.allOf(primaryAck, replicaAck)
            .thenApply(v -> {
                // 两者都成功才返回
                Ack p = primaryAck.join();
                Ack r = replicaAck.join();
                return new Ack(offset, AckStatus.REPLICATED);
            })
            .orTimeout(30, TimeUnit.SECONDS);  // 超时保护
    }
}
```

---

## 5. 测试策略

### 5.1 测试矩阵

| 场景 | 测试方法 | 验证点 |
|------|----------|--------|
| 正常复制 | 集成测试 | 1万任务，replica追平，offset一致 |
| Primary宕机 | 故障注入 | Failover < 5s，任务不丢，新primary接管 |
| Replica宕机 | 故障注入 | Primary降级但不停止服务，告警触发 |
| 网络抖动 | Toxiproxy | Lag可观测，恢复后自动catch-up |
| 双节点故障 | 故障注入 | 集群降级，可读不可写，恢复后自愈 |
| 脑裂防护 | 集成测试 | Split-brain检测，旧primary拒绝写入 |
| 幂等性 | 单元测试 | 重复offset不重复执行 |
| 性能回归 | JMH | TPS >= 基线70%，P99 < 500ms |

### 5.2 关键测试用例

```java
@Test
public void testZeroDataLossOnFailover() {
    // 1. 写入 1000 任务，全部 REPLICATED 确认
    List<String> taskIds = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
        String id = client.createTask(
            TaskRequest.builder()
                .payload("data-" + i)
                .ackLevel(AckLevel.REPLICATED)  // 关键
                .build()
        );
        taskIds.add(id);
    }
    
    // 2. Kill primary
    primary.destroy();
    
    // 3. 等待 failover
    awaitFailover(5, TimeUnit.SECONDS);
    
    // 4. 验证所有任务都存在
    for (String taskId : taskIds) {
        Task task = client.getTask(taskId);
        assertNotNull("Task " + taskId + " lost after failover!", task);
    }
}
```

---

## 6. 风险与应对

| 风险 | 影响 | 可能性 | 应对措施 |
|------|------|--------|----------|
| **复制延迟过大** | REPLICATED 模式性能下降 | 中 | 1) 支持批量复制<br>2) 动态降级到 DURABLE<br>3) 监控告警 |
| **脑裂** | 数据不一致 | 低 | 1) Coordinator 仲裁<br>2) 路由表版本号<br>3) Fencing Token |
| **Catch-up 太慢** | Replica 长期不可用 | 中 | 1) 快照+增量<br>2) 可配置并发度<br>3) 超时降级策略 |
| **网络分区** | 误判故障 | 中 | 1) 多路径探测<br>2) 阈值判定<br>3) 人工介入接口 |
| **WAL 膨胀** | 磁盘空间不足 | 低 | 1) 已实现的 compaction<br>2) 复制进度检查点<br>3) 磁盘监控告警 |

---

## 附录 A: 配置文件示例

```yaml
loomq:
  replication:
    enabled: true
    mode: async

    # 网络配置
    bind_host: 0.0.0.0
    bind_port: 9090

    # 心跳与故障检测
    heartbeat_interval_ms: 1000
    heartbeat_timeout_ms: 5000

    # 复制性能
    batch_size: 100
    max_lag_records: 10000
    max_replication_lag_ms: 2000

    # Catch-up
    catchup_batch_size: 1000
    catchup_timeout_ms: 30000

    # 安全
    require_replicated_ack: false  # 是否强制所有写入用 REPLICATED

  cluster:
    coordinator:
      enabled: true

      # ========== 租约配置（硬约束 #1）==========
      lease:
        enabled: true
        duration_ms: 10000          # 租约有效期 10s
        renewal_window_ratio: 0.3   # 过期前 30% 可续约
        strict_mode: true           # 无有效租约拒绝写入

      # 路由表版本
      routing:
        version_initial: 1
        version_increment_on_failover: true

      # Fencing Token
      fencing:
        enabled: true
        token_ttl_ms: 30000         # token 最长有效期

      # ========== 复制对象配置（决策 #5）==========
      replication_scope:
        include_task_metadata: true
        include_state_transitions: true
        include_scheduler_index: true    # 硬约束 #3：必须包含调度索引
        include_retry_count: true
        include_dlq_records: true

      # 幂等性配置（硬约束 #4）
      idempotency:
        enabled: true
        processed_offset_window: 1000000  # 保留最近 100 万条处理记录
        check_before_apply: true
```

---

## 附录 B: API 扩展

```java
// 新增端点

// 1. 复制状态查询
GET /cluster/replication
Response: {
    "role": "PRIMARY",
    "state": "ACTIVE",
    "replica": {
        "nodeId": "shard-0-replica",
        "address": "10.0.0.2:9090",
        "connected": true,
        "lastAckedOffset": 12345,
        "lagMs": 50
    },
    "lastReplicatedOffset": 12346,
    "replicationLagMs": 50
}

// 2. 手动 failover (运维用)
POST /admin/failover/{shardId}
Request: { "force": false }

// 3. Replica 健康检查
GET /health/replica
Response: {
    "status": "UP",
    "state": "REPLICA_SYNCED",
    "lastAppliedOffset": 12345,
    "lagBehindPrimary": 1
}
```

---

**文档版本**: v0.5-draft  
**最后更新**: 2026-04-08  
**作者**: Claude + LoomQ Team
