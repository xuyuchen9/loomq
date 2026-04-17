# LoomQ v0.5 路线图

> 版本: v0.5
> 目标: 生产级高可用延迟调度引擎
> 状态: ✅ 已发布

---

## 版本目标

从"可恢复的分片调度引擎"升级为**生产级高可用延迟调度引擎**

- 支持每个 shard 1 primary + 1 replica
- 主节点故障后自动切换
- 金融级可靠性 (REPLICATED ACK)

---

## 核心功能

| 功能 | 状态 | 说明 |
|------|------|------|
| 异步复制 | ✅ | ASYNC / DURABLE / REPLICATED 三档 ACK |
| Netty 传输层 | ✅ | 长连接，低延迟 |
| 租约协调 | ✅ | Coordinator + lease + fencing |
| 故障转移 | ✅ | 自动/手动 failover |
| 快照恢复 | ✅ | Snapshot + WAL replay |
| 脑裂防护 | ✅ | Fencing token 保护 |

---

## Intent 状态机

```
CREATED → SCHEDULED → DUE → DISPATCHING → DELIVERED → ACKED
   ↓          ↓         ↓          ↓            ↓
CANCELED   (可修改)   EXPIRED   FAILED      DEAD_LETTER
```

---

## 交付物

- [implementation.md](implementation.md) - 详细实现计划
- 340+ 单元测试
- 完整 API 文档 (v1/intents)

---

## 里程碑

| 日期 | 里程碑 |
|------|--------|
| 2026-04-01 | v0.5 设计定稿 |
| 2026-04-05 | 核心复制逻辑完成 |
| 2026-04-09 | 全部测试通过，版本发布 |
