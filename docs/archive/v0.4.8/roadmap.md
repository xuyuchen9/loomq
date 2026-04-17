# LoomQ v0.4.8 路线图

> 版本: v0.4.8
> 目标: 主从复制与高可用基础
> 状态: ✅ 已发布

---

## 版本目标

实现 **Primary-Replica 复制框架**，为 v0.5 的高可用功能奠定基础。

---

## 核心功能

| 功能 | 状态 | 说明 |
|------|------|------|
| Replication Framework | ✅ | 主从复制协调器 |
| ACK 机制 | ✅ | 三档确认级别 (ASYNC/DURABLE/REPLICATED) |
| ReplicatingWalWriter | ✅ | 支持复制的 WAL 写入器 |
| Netty 传输 | ✅ | 基于 Netty 的复制协议 |
| Catch-up | ✅ | 断线重连后的追平机制 |
| Lease 协调 | ✅ | 基于租约的集群协调 |
| Fencing Token | ✅ | 脑裂防护机制 |
| 自动故障转移 | ✅ | 带追平的故障转移 |

---

## 关键组件

### Replication 模块
- `ReplicationManager` - 复制协调器
- `ReplicatingWalWriter` - 复制感知 WAL
- `ReplicationRecord` - 复制记录格式
- `ReplicaClient` / `ReplicaServer` - Netty 客户端/服务端
- `WalCatchUpManager` - WAL 追平管理

### Cluster 模块
- `CoordinatorLease` - 租约管理
- `FencingToken` - 防护令牌
- `HeartbeatManager` - 心跳管理
- `ShardStateMachine` - 分片状态机
- `FailoverController` - 故障转移控制器

### ACK 语义
- `Ack` - 确认对象
- `AckLevel` - 确认级别枚举
- `AckStatus` - 确认状态

---

## 交付物

- [implementation.md](implementation.md) - 详细实现文档
- 完整 Replication 测试套件
- Cluster HA 测试套件

---

## 版本关系

v0.4.8 是 v0.5 的基础版本，实现了核心的复制和 HA 机制。
实际发布的 v0.5 版本在此基础上增加了 Intent API 和完整的生命周期管理。
