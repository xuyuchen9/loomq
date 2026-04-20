# v0.6.1 第一性原理设计文档

## 1. 设计背景

v0.6.0 的 WAL 实现存在过度工程化问题：
- 59字节结构化头部，WAL 层被迫理解业务 Schema
- JSON 序列化产生大量临时对象，GC 压力高
- 异步投递状态机复杂，难以维护

本次重构基于第一性原理，追问每个组件的**本质需求**。

---

## 2. WAL 简化分析

### 2.1 当前假设 vs 本质需求

| 当前假设 | 本质需求 |
|----------|----------|
| WAL 需要结构化存储 Intent 的所有字段 | WAL 只是顺序追加的原始字节流，用于崩溃恢复 |
| 固定59字节头部 + 字段编码 + Payload | 记录边界 + 字节内容 |
| WAL 层需要理解 taskId、bizKey 等字段 | 编码/解码应发生在应用层，而非 WAL 层 |

### 2.2 简化方案

**格式定义（8字节长度前缀）**

```
+------------+----------------+----------------+
|  Length    |    Payload     |   Checksum     |
|  (4 bytes) |   (Length)     |   (4 bytes)    |
+------------+----------------+----------------+
```

- Length: 大端序 uint32，最大 16MB
- Payload: 应用层原始字节（由应用层决定格式）
- Checksum: CRC32，覆盖 Length + Payload

**总开销**: 8 字节/记录（对比原设计 59+ 字节头部）

### 2.3 为什么用二进制而非 JSON？

| 因素 | JSON | 二进制 |
|------|------|--------|
| 性能 | ~2 µs | ~100 ns |
| 内存分配 | 多（临时对象） | 极少 |
| 可读性 | 好 | 差（需工具） |
| 代码量 | 50 行 | 200 行 |
| 风险 | 低 | 一次性编码复杂度 |

**决策**: 在 200K QPS 目标下，二进制是必须的。一次性编写 200 行编码器，换取永久零 GC 压力。

### 2.4 设计原则

1. **WAL 层无感知业务 Schema** - 只做字节搬运
2. **MemorySegment 零拷贝** - 保留高性能写入
3. **批量刷盘摊平 fsync** - 写入与刷盘解耦

---

## 3. 过期检查分析

### 3.1 当前假设 vs 本质需求

| 当前假设 | 本质需求 |
|----------|----------|
| 需要维护按 deadline 排序的全局索引 | 只需要快速回答"哪些任务已过期" |
| ConcurrentSkipListSet O(log n) | 按时间分桶，只检查当前桶 O(1) |

### 3.2 为什么保留 BucketGroup？

当前 `BucketGroup` 已经是分桶设计：

```java
// 按精度窗口取整作为 key
long bucketKey = floorToBucket(executeAt.toEpochMilli());  // O(1)
buckets.compute(bucketKey, ...);  // ConcurrentHashMap 的 O(1)
```

**实际情况**：
- 插入是 O(1) 分桶
- `ConcurrentSkipListMap` 的 O(log n) 仅影响范围查询
- 桶数量 = 时间窗口/精度（如 1小时/100ms = 36000 个），实际开销可忽略

### 3.3 时间轮 vs SkipList 对比

| 方面 | 时间轮 | SkipList (当前) |
|------|--------|-----------------|
| 复杂度 | O(1) 插入/取出 | O(log n) 节点查找 |
| 并发 | 每个桶独立队列 | ConcurrentHashMap 无竞争 |
| 内存 | 固定 3600 个桶 | 动态桶 |
| 代码量 | ~50 行 | 已有 ~200 行 |
| 收益 | 无可测量提升 | - |

**决策**: 时间轮在这里是 over-engineering，保留现有 BucketGroup。

---

## 4. 投递模式分析

### 4.1 当前假设 vs 本质需求

| 当前假设 | 本质需求 |
|----------|----------|
| HTTP 投递需要异步非阻塞以提升吞吐 | 投递是副作用，可以用背压 + 重调度替代复杂性 |
| sendAsync() + 回调管理状态 | 成功通知下游，失败重试即可 |

### 4.2 为什么用同步批量？

**第一性原理**：虚拟线程使同步调用成本极低，批量发送摊平连接开销。

```java
// 批量取出
dispatchQueue.drainTo(batch, 100);

// 并行发送（虚拟线程）
batch.stream()
    .map(i -> CompletableFuture.runAsync(() -> sendSync(i), virtualThreadExecutor))
    .toList();

// 等待批量完成
CompletableFuture.allOf(futures).join();
```

### 4.3 背压机制

```java
public void dispatch(Intent intent) {
    // 队列满时直接拒绝，由上游处理背压
    if (!dispatchQueue.offer(intent, 100, TimeUnit.MILLISECONDS)) {
        throw new BackPressureException("Dispatcher overloaded");
    }
}
```

### 4.4 对比

| 方面 | 异步回调 | 同步批量 |
|------|----------|----------|
| 代码复杂度 | 高（状态机） | 低（顺序逻辑） |
| 可靠性 | 需要管理回调状态 | 简单重试 |
| 吞吐 | 依赖异步框架 | 批量摊平 |
| 调试 | 困难 | 容易 |

**决策**: 同步批量 + 虚拟线程更简单可靠。

---

## 5. 设计决策汇总

| 决策点 | 原方案 | 最终决策 | 理由 |
|--------|--------|----------|------|
| WAL 格式 | 8字节头 + JSON | 8字节头 + **二进制** | 一次性编码复杂度，永久零 GC |
| 过期检查 | 时间轮 O(1) | **保留 BucketGroup** | 收益为负，避免过度设计 |
| 投递模式 | 异步回调 | **同步批量 + 背压** | 更简单可靠 |

---

## 6. 关键洞察

1. **WAL 层应该是无感知的** - 只做字节搬运，Schema 由应用层决定
2. **时间轮不是万能药** - 当前 BucketGroup 已足够高效
3. **虚拟线程改变设计权衡** - 同步调用成本极低，批量才是关键
4. **简化不是目的，消除不必要的抽象才是**

---

## 7. 参考

- [第一性原理设计提案](../../../CHANGELOG.md) - 原始设计讨论
- [BucketGroup 源码](../../src/main/java/com/loomq/scheduler/v5/BucketGroup.java)
- [SimpleWalWriter 源码](../../src/main/java/com/loomq/wal/SimpleWalWriter.java)
