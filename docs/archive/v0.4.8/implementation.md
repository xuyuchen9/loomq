# LoomQ v0.4.8 实现文档

> 版本: v0.4.8
> 主题: 主从复制与高可用基础

---

## 1. Replication Framework

### 1.1 核心组件

#### ReplicationManager
- 中央复制协调器
- 管理所有复制连接
- 协调 ACK 级别

#### ReplicatingWalWriter
- 带复制支持的 WAL 写入器
- 在本地 WAL 写入后发起复制
- 等待副本确认（根据 ACK 级别）

#### ReplicationRecord
- 任务变更传播格式
- 序列化/反序列化
- 支持幂等重放

### 1.2 Netty 复制协议

#### ReplicaClient
- 基于 Netty 的副本客户端
- 维护与主节点的长连接
- 处理 ACK 响应

#### ReplicaServer
- 基于 Netty 的副本服务端
- 接收复制记录
- 写入本地 WAL

### 1.3 追平机制

#### WalCatchUpManager
- 副本重连后的追平
- 基于快照 + WAL 重放
- 断点续传支持

---

## 2. ACK 机制

### 2.1 ACK 级别

| 级别 | RPO | 延迟 | 使用场景 |
|------|-----|------|----------|
| `ASYNC` | <100ms | <1ms | 高吞吐，可容忍丢失 |
| `DURABLE` | 0 | <20ms | 默认推荐 |
| `REPLICATED` | 0 | <50ms | 金融级可靠性 |

### 2.2 ACK 状态
- `PENDING` - 等待确认
- `ACKED` - 已确认
- `TIMEOUT` - 超时
- `FAILED` - 失败

---

## 3. Lease-based Cluster Coordination

### 3.1 租约机制

#### CoordinatorLease
- 基于 epoch 的租约
- 租约续期机制
- 租约过期检测

#### FencingToken
- 单调递增的防护令牌
- 防止脑裂
- 拒绝过期租约的请求

### 3.2 心跳管理

#### HeartbeatManager
- 双向健康监测
- 可配置的超时时间
- 节点抖动检测

### 3.3 状态机

#### ShardStateMachine
- 分片状态转换
- PRIMARY / REPLICA / UNKNOWN
- 带 fencing 的状态变更

### 3.4 故障转移

#### FailoverController
- 自动化故障转移
- 带追平支持
- 手动/自动模式

---

## 4. 设计原则

### 4.1 传输层选择
- **Netty 长连接** - 内部控制平面，追求稳定、低延迟
- 不选 HTTP/2 - 对于内部控制平面过于复杂

### 4.2 一致性模型
- **异步复制 + 三档 ACK** - 用户显式选择可靠性级别
- v0.4.8 不引入 Raft - 保持简单性

### 4.3 硬约束

1. Coordinator 必须是"租约 + 版本号"仲裁，不是简单投票
2. Catch-up 必须先定义快照边界和 WAL 截点
3. 复制必须包含调度索引，不能只复制任务元数据
4. Replay 必须幂等，同一条 record 重放两次不能破坏状态

---

## 5. 代码位置

```
src/main/java/com/loomq/
├── replication/
│   ├── ReplicationManager.java
│   ├── ReplicatingWalWriter.java
│   ├── ReplicationRecord.java
│   ├── Ack.java
│   ├── AckLevel.java
│   ├── AckStatus.java
│   ├── client/ReplicaClient.java
│   ├── server/ReplicaServer.java
│   └── WalCatchUpManager.java
└── cluster/
    ├── CoordinatorLease.java
    ├── FencingToken.java
    ├── HeartbeatManager.java
    ├── ShardStateMachine.java
    └── FailoverController.java
```

---

## 6. 测试覆盖

| 测试类 | 描述 |
|--------|------|
| ReplicationManagerTest | 复制管理器测试 |
| ReplicationRecordTest | 复制记录序列化测试 |
| WalCatchUpManagerTest | 追平机制测试 |
| CoordinatorLeaseTest | 租约测试 |
| FencingTokenTest | 防护令牌测试 |
| FailoverControllerCatchUpTest | 故障转移追平测试 |
