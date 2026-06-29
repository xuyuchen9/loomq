# 设计文档：K8s Lease 选主 + Epoch 日志复制

> **状态：** 设计完成，待实现
> **日期：** 2026-06-01
> **作者：** xuyuchen
> **分支：** dev-xuyuchen

---

## 1. 背景与动机

LoomQ 当前使用自研 Raft 实现（`loomq-raft` 模块）提供分布式一致性。经过生产实践，发现两个核心问题：

1. **维护成本高**：自研 Raft 涉及选举、Term 管理、心跳、日志冲突检测等复杂逻辑，bug 风险大
2. **可用性不够高**：自研实现的 edge case 处理不如成熟方案完善

**决策：** 拆解 Raft，用 Kubernetes Lease API 替代选主，保留日志复制核心机制。

**核心价值：** 用运维复杂度换代码复杂度——K8s 已经是用户的基础设施，不是 LoomQ 额外引入的依赖。

---

## 2. 设计目标

| 目标 | 描述 |
|------|------|
| 降低维护成本 | 删除自研选主代码（~380 行），用 K8s Lease 替代 |
| 提高可用性 | K8s Lease 经过大规模验证，edge case 处理成熟 |
| 保留数据可靠性 | 核心日志复制机制（AppendEntries + InstallSnapshot）保留 |
| 异步复制优先 | 默认异步复制（写 WAL 即返回），同步模式可选 |

---

## 3. 关键决策

| 决策项 | 选择 | 理由 |
|--------|------|------|
| 选主机制 | K8s Lease API | 成熟、无需自研、K8s 原生支持 |
| Term 替代 | Epoch（`leaseTransitions`） | K8s Lease 内置计数器，单调递增，天然 fencing token |
| 复制模式 | 异步默认 + 同步可选 | 延时任务场景可接受异步，性能优先 |
| 通信协议 | gRPC | 生态好，双向流支持分块传输 |
| 部署环境 | 仅 K8s | 分布式部署统一使用 K8s |

---

## 4. 架构总览

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Pod A      │  │   Pod B      │  │   Pod C      │      │
│  │  (Leader)    │  │ (Follower)   │  │ (Follower)   │      │
│  │              │  │              │  │              │      │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │      │
│  │ │K8s Lease │ │  │ │K8s Lease │ │  │ │K8s Lease │ │      │
│  │ │ Election │ │  │ │ Watcher  │ │  │ │ Watcher  │ │      │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │      │
│  │      │       │  │      │       │  │      │       │      │
│  │      ▼       │  │      ▼       │  │      ▼       │      │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │      │
│  │ │  Epoch   │ │  │ │  Epoch   │ │  │ │  Epoch   │ │      │
│  │ │ Manager  │ │  │ │ Manager  │ │  │ │ Manager  │ │      │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │      │
│  │      │       │  │      │       │  │      │       │      │
│  │      ▼       │  │      ▼       │  │      ▼       │      │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │      │
│  │ │   Log    │ │  │ │   Log    │ │  │ │   Log    │ │      │
│  │ │Replicat. │◄├──►│ │Replicat. │◄├──►│ │Replicat. │ │      │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │      │
│  │      │       │  │      │       │  │      │       │      │
│  │      ▼       │  │      ▼       │  │      ▼       │      │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │      │
│  │ │ Intent   │ │  │ │ Intent   │ │  │ │ Intent   │ │      │
│  │ │  Store   │ │  │ │  Store   │ │  │ │  Store   │ │      │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │                  Lease Resource                       │  │
│  │  holder: pod-a                                       │  │
│  │  leaseTransitions: 3  ←── 这就是 epoch               │  │
│  │  acquireTime: 2026-06-01T10:00:00Z                   │  │
│  │  renewTime: 2026-06-01T10:00:05Z                     │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. 组件变更清单

### 5.1 删除的组件

| 文件 | 原因 |
|------|------|
| `LeaderElection.java` | 选举逻辑交给 K8s Lease |
| `RaftTransport.java` 中的 RequestVote RPC | 不再需要投票机制 |
| `RaftNode.java` 中的选举相关代码 | 选举回调、term 管理 |

### 5.2 保留并适配的组件

| 文件 | 变更 |
|------|------|
| `LogReplication.java` | `term` → `epoch`，移除选举依赖 |
| `RaftLog.java` | 不变 |
| `RaftWriteCoordinator.java` | `isLeader()` 改为检查 K8s Lease holder + 有效期 |
| `RaftTransport.java` | 改用 gRPC，移除 RequestVote，保留 AppendEntries + InstallSnapshot |

### 5.3 新增的组件

| 文件 | 职责 |
|------|------|
| `K8sLeaseElection.java` | 封装 K8s Lease API，提供 `isLeader()`、`getEpoch()`、`getCurrentLeader()` |
| `LeaderElection.java` (接口) | 抽象选主接口，支持 K8s 和未来其他实现 |
| gRPC proto 定义 | AppendEntries、InstallSnapshot 的 gRPC 服务定义 |

---

## 6. 核心流程

### 6.1 Leader 选举流程

```
Pod A 启动
    │
    ▼
尝试创建/更新 Lease
    │
    ├─ 成功 → 成为 Leader，epoch = leaseTransitions
    │
    └─ 失败（已有 holder）→ 成为 Follower，watch Lease
                              │
                              ▼
                    Lease holder 变更？
                        │
                        ├─ 是 → epoch++，尝试抢 Lease
                        │
                        └─ 否 → 继续 watch
```

### 6.2 写入流程（Leader）

```
客户端请求 → IntentHandler
    │
    ▼
RaftWriteCoordinator.submitWrite()
    │
    ├─ 检查 isLeader()（K8s Lease holder + 有效期）
    │
    ├─ 检查 epoch（fencing token）
    │
    ├─ propose 到本地 RaftLog
    │
    ├─ 异步模式（默认）：立即返回成功，后台异步复制
    │
    ├─ 同步模式：等待多数确认后返回
    │
    └─ 返回结果
```

### 6.3 读取流程

```
客户端请求 → IntentHandler
    │
    ├─ 强一致读 → 只从 Leader 读（在 lease 有效期内）
    │
    └─ 最终一致读 → 从任意节点读
```

### 6.4 Follower 转正流程

```
1. K8s Lease 过期，触发新选主
2. Follower 获取 Lease，epoch 递增
3. Follower 变为新 Leader，将自己的最后一条日志 index 设为 commitIndex
4. 新 Leader 开始接收写入请求
5. 新 Leader 向其他 Follower 发送 AppendEntries（带新 epoch）
   → Follower 发现 epoch 变更，接受新 Leader
   → 可能截断旧 epoch 未提交的日志
```

---

## 7. 关键设计细节

### 7.1 Epoch 管理

```java
public interface LeaderElection {
    boolean isLeader();
    long currentEpoch();           // 对应 K8s Lease.leaseTransitions
    String currentLeader();        // 对应 K8s Lease.holderIdentity
    void addLeadershipListener(LeadershipListener listener);
}

public class K8sLeaseElection implements LeaderElection {
    private final String podName;
    private final String leaseName;
    private final String namespace;
    private final KubernetesClient client;
    private volatile long currentEpoch = 0;
    private volatile boolean isLeader = false;

    // 通过 watch Lease 资源感知 leadership 变更
    // 当 holder 变更为自己时，epoch = leaseTransitions
}
```

### 7.2 Epoch 持久化

epoch 必须持久化——节点重启后如果忘记了自己的 epoch，可能接受旧 Leader 的过期日志。

```java
// 在 K8sLeaseElection 初始化时
long persistedEpoch = walAccessor.readEpoch();  // 从 WAL meta 恢复
if (persistedEpoch > 0) {
    this.currentEpoch = persistedEpoch;
}

// 每次 epoch 变更时持久化
walAccessor.writeEpoch(newEpoch);
```

复用现有 WAL 的 `raft_meta` 文件，将 `term` 替换为 `epoch`。

### 7.3 isLeader() 检查

```java
public boolean isLeader() {
    // 不仅检查 Lease holder，还要检查 Lease 是否仍然有效
    return leaseHolder.equals(podName) && lease.isValid();
}

// Lease 有效性检查
private boolean isLeaseValid() {
    if (renewTime == null) return false;
    long elapsedMs = System.currentTimeMillis() - renewTime.toEpochMilli();
    return elapsedMs < leaseDurationSeconds * 1000L;
}
```

### 7.4 日志条目格式变更

```
旧格式: [8 bytes term][payload]
新格式: [8 bytes epoch][payload]
```

### 7.5 AppendEntries 协议变更

```
旧: AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
新: AppendEntries(epoch, leaderId, prevLogIndex, prevLogEpoch, entries, leaderCommit)
```

### 7.6 Fencing 机制

```java
// Follower 收到 AppendEntries 时
if (msg.epoch < currentEpoch) {
    // 拒绝：这是旧 Leader 的请求
    return AppendEntriesResult.fail(currentEpoch);
}

// Leader 发送 AppendEntries 时
if (!isLeader()) {
    // 已经不是 Leader 了，停止发送
    return;
}
```

### 7.7 复制模式

```java
public enum ReplicationMode {
    ASYNC,   // Leader 写 WAL 即返回，后台异步复制（默认）
    SYNC     // 等待多数确认后返回
}

// RaftWriteCoordinator 中
if (replicationMode == ReplicationMode.ASYNC) {
    // 写入本地 WAL 后立即返回
    long index = raftLog.appendEntry(epoch, payload);
    triggerAsyncReplication();  // 后台异步复制
    return WriteResult.success(index);
} else {
    // 同步模式：等待多数确认
    long index = raftLog.appendEntry(epoch, payload);
    awaitQuorumAck(index, timeoutMs);
    return WriteResult.success(index);
}
```

### 7.8 gRPC InstallSnapshot 分块传输

```protobuf
service RaftService {
    rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
    rpc InstallSnapshot(stream SnapshotChunk) returns (SnapshotResponse);
}

message SnapshotChunk {
    int64 epoch = 1;
    string leader_id = 2;
    int64 last_included_index = 3;
    int64 last_included_epoch = 4;
    int32 chunk_index = 5;
    int32 total_chunks = 6;
    bytes data = 7;
}
```

---

## 8. Lease 参数配置

```yaml
# 初始推荐值
loomq:
  raft:
    lease:
      duration-seconds: 10      # 过期时间
      renew-interval-seconds: 3  # 每 3 秒续约一次
```

如果续约因为 GC 或网络抖动偶尔失败，3 秒的间隔给了足够的重试窗口（可以续约 3 次才过期）。10 秒的过期时间意味着 Leader 宕机后最坏 10 秒恢复。对于延时任务场景，这个 RTO 完全可以接受。

---

## 9. 风险分析

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| K8s Lease 超时时间长（默认 10-30s） | 故障切换慢 | 设置较短的 leaseDuration（10s），renewInterval（3s） |
| 网络分区时可能双 Leader | 数据不一致 | 使用 epoch 做 fencing，旧 Leader 的写入会被拒绝 |
| K8s API Server 不可用 | 无法选主 | 这是 K8s 的固有限制，需要保证 API Server 高可用 |
| gRPC 性能低于自定义 TCP | 复制延迟增加 | 使用 streaming、pipeline、批量发送优化 |
| 旧 Leader GC 停顿后自以为是 | 写入过期日志 | isLeader() 同时检查 Lease 有效期，Follower 端 epoch fencing |

---

## 10. 代码量预估

| 变更类型 | 预估代码量 |
|---------|-----------|
| 删除 `LeaderElection.java` | -180 行 |
| 删除选举相关代码 | -200 行 |
| 新增 `K8sLeaseElection.java` | +150 行 |
| 新增 gRPC 定义 + 实现 | +300 行 |
| 适配 `LogReplication.java` | ~50 行变更 |
| 适配 `RaftNode.java` | ~100 行变更 |

**净变化：** 删除约 380 行，新增约 600 行，但核心复杂度从"自研选举 + 自研复制"降为"K8s Lease + 自研复制"。

---

## 11. 实施路径

### Phase 1：接口抽象
- 定义 `LeaderElection` 接口
- 将 `RaftNode`、`LogReplication`、`RaftWriteCoordinator` 中的选举依赖改为接口依赖
- 确保现有功能不受影响（用现有 `LeaderElection` 实现适配接口）

### Phase 2：Epoch 替代 Term
- 将所有 `term` 引用替换为 `epoch`
- 修改 WAL meta 持久化格式
- 修改 AppendEntries 协议格式

### Phase 3：K8s Lease 实现
- 实现 `K8sLeaseElection`
- 集成 Kubernetes Java Client
- 实现 Lease watch 和 leadership 监听

### Phase 4：gRPC 迁移
- 定义 gRPC proto 文件
- 实现 gRPC transport
- 保留分块 InstallSnapshot 逻辑

### Phase 5：复制模式
- 实现异步/同步复制模式切换
- 配置化复制模式选择

### Phase 6：清理
- 删除 `LeaderElection.java`（旧实现）
- 删除 RequestVote 相关代码
- 更新测试

---

## 12. 总结

这个设计砍掉了分布式系统中最容易出 Bug 的部分（选主、Term 管理、心跳），用 K8s 的成熟原语替代，同时保留了日志复制的核心可靠性。

**最大的架构价值：** 用运维复杂度换代码复杂度——而运维复杂度由 K8s 承担。K8s 已经是用户的基础设施，不是 LoomQ 额外引入的依赖。
