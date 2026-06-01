# LoomQ 全链路地图 — 第一性原理拆分

## 第一性原理：一个持久化时间内核的本质

LoomQ 本质上只做一件事：**在正确的时刻，可靠地触发事件**。

从这个公理出发，系统的所有功能可以分解为 9 条独立链路：

---

## 链路总览

| # | 链路名称 | 方向 | 核心问题 |
|---|---------|------|---------|
| 1 | 写入链路 (Ingress) | 入 | 如何接收并持久化一个 Intent？ |
| 2 | 调度链路 (Scheduling) | 内 | 如何组织大量不同时间点的 Intent？ |
| 3 | 触发链路 (Dispatch) | 内→出 | 如何在正确的时刻触发 Intent？ |
| 4 | 投递链路 (Delivery) | 出 | 如何将 Intent 可靠地送达用户回调？ |
| 5 | 重试链路 (Retry) | 内 | 投递失败后如何重试？死信如何处理？ |
| 6 | 查询链路 (Query) | 入 | 如何读取 Intent 状态？ |
| 7 | 持久化链路 (Persistence) | 内 | 如何保证数据不丢失？ |
| 8 | 恢复链路 (Recovery) | 内 | 崩溃后如何恢复状态？ |
| 9 | 共识链路 (Consensus) | 内↔内 | 多节点间如何保持一致？ |

---

## 链路 1：写入链路 (Ingress)

**核心问题：** 如何接收并持久化一个 Intent？

### 入口

有两个协议入口，共享同一个引擎：

```
HTTP (Netty:7928)                    gRPC (:8928)
POST /v1/intents                     CreateIntent RPC
       │                                    │
       ▼                                    ▼
NettyRequestHandler                  LoomqGrpcService
  ├─ Route match (RadixRouter)         ├─ ProtoConverter.toDomain()
  ├─ Auth (SecurityConfig)             ├─ IntentValidator
  ├─ Semaphore backpressure            ├─ Idempotency check
  ├─ Body copy to heap                 ├─ TierAdvisor (SLO)
  └─ Dispatch to virtual thread        └─ WriteCoordinator / Engine
```

### 非 Raft 模式写入管线

```
IntentHandler.createIntent()
  │
  ├─ 1. 反序列化 JSON → CreateIntentRequest
  ├─ 2. 校验 (IntentValidator: executeAt, deadline, shardKey, callback URL)
  ├─ 3. 幂等检查 (engine.checkIdempotency → ConcurrentHashMap, 24h 窗口)
  ├─ 4. ID 生成 (SHA-256 确定性 / UUID 随机)
  ├─ 5. 构建 Intent draft (apply tier defaults, TierAdvisor.recommend)
  │
  └─ 6. engine.createIntent(intent, ackMode)  ← 进入 Core
         │
         ├─ a. IntentCommandService.createIntent() [virtual thread]
         │     ├─ sequenceNumber++
         │     ├─ apply engine default tier
         │     ├─ CREATED → SCHEDULED, revision++
         │     ├─ IntentBinaryCodec.encode() [CPU overlap]
         │     ├─ WalMode 解析: explicit > intent > tier default
         │     ├─ SimpleWalWriter.writeAsync/writeDurable() [WAL I/O]
         │     ├─ ConcurrentIntentStore.save() [memory]
         │     ├─ if DURABLE: awaitFlush(position)
         │     │
         │     ├─ [if delay > 1h] ColdIntentSwapper.swapOut()
         │     │     ├─ 创建 ColdIntentEntry (~80B)
         │     │     ├─ intentStore.delete(intentId)
         │     │     └─ coldIndex.put(executeAtMs, entry)
         │     │
         │     └─ [if delay <= 1h] scheduler.schedule(intent)
         │           ├─ [delay > precisionWindow] → CohortManager.register()
         │           └─ [delay <= precisionWindow] → BucketGroupManager.add()
         │
         └─ CompletableFuture<Long> (WAL sequence number)
```

### Raft 模式写入管线

```
IntentHandler.createIntent()
  │
  ├─ 1-5. 同上
  │
  └─ 6. raftWriteCoordinator.commitSnapshot(snapshot, "create", requestKey)
         │
         ├─ a. Semaphore.tryAcquire(500ms) → backpressure 检查
         ├─ b. ReentrantLock.lock() → 序列化写入
         ├─ c. 检查 isLeader()
         ├─ d. IntentBinaryCodec.encode(snapshot)
         ├─ e. raftNode.propose(bytes)
         │     └─ raftLog.appendEntry(term, encoded) → WAL.writeEntry()
         ├─ f. logReplication.awaitApplied(index, 5s)
         │     └─ [等待 heartbeat 复制到 majority]
         │           └─ advanceCommitIndex → applyCommitted()
         │                 ├─ IntentStore.upsert()
         │                 └─ RaftRuntimeListener.onCommittedIntent()
         ├─ g. 从 store 读取已提交的 intent
         └─ h. 返回 committed intent
```

### 关键文件

| 组件 | 文件 |
|------|------|
| HTTP 入口 | `loomq-server/.../http/netty/NettyRequestHandler.java` |
| 路由 | `loomq-server/.../http/netty/RadixRouter.java` |
| Intent 处理器 | `loomq-server/.../http/netty/IntentHandler.java` |
| gRPC 服务 | `loomq-channel/loomq-channel-grpc/.../LoomqGrpcService.java` |
| 命令服务 | `loomq-core/.../application/command/IntentCommandService.java` |
| 引擎入口 | `loomq-core/.../LoomqEngine.java` |

---

## 链路 2：调度链路 (Scheduling)

**核心问题：** 如何组织大量不同时间点的 Intent？

### 数据结构层次

```
PrecisionScheduler
  │
  ├─ CohortManager (长延迟队列)
  │     ConcurrentSkipListMap<Long/*cohortKey*/, ConcurrentLinkedDeque<Intent>>
  │     └─ 唤醒线程: platform daemon "cohort-waker"
  │
  ├─ BucketGroupManager (短延迟队列)
  │     EnumMap<PrecisionTier, BucketGroup>
  │     └─ BucketGroup
  │           ├─ ConcurrentSkipListMap<Long/*bucketKey*/, ConcurrentHashMap<id, Intent>>
  │           ├─ ConcurrentHashMap<String/*intentId*/, Long/*bucketKey*/> (反向索引)
  │           └─ AtomicLong pendingCount
  │
  ├─ tierDispatchQueues (就绪队列)
  │     EnumMap<PrecisionTier, ConcurrentLinkedDeque<Intent>>
  │
  └─ ResizableSemaphore (并发控制)
        EnumMap<PrecisionTier, ResizableSemaphore>
        └─ 支持跨 tier 借用 (Arrow-inspired, max 50%)
```

### 调度决策树

```
scheduler.schedule(intent)
  │
  ├─ delayMs <= 0 → addToBucketAndDispatch(intent) → 直接进就绪队列
  │
  ├─ delayMs > precisionWindowMs → CohortManager.register(intent)
  │     └─ 按 (executeAt - precisionWindow) 分组
  │         └─ wakeLoop: park until earliest → bucketGroupManager.addAll()
  │
  └─ 0 < delayMs <= precisionWindowMs → addToBucketAndDispatch(intent)
        └─ 进入时间桶，等待 scan 线程扫描
```

### 五级精度分层

| Tier | 精度窗口 | 最大并发 | 批大小 | 消费者数 | WAL 模式 |
|------|---------|---------|--------|---------|---------|
| ULTRA | 10ms | 200 | 1 | 16 | ASYNC |
| FAST | 50ms | 150 | 1 | 12 | ASYNC |
| HIGH | 100ms | 50 | 5 | 4 | BATCH_DEFERRED |
| STANDARD | 500ms | 50 | 20 | 3 | DURABLE |
| ECONOMY | 1000ms | 50 | 25 | 2 | DURABLE |

### 线程模型

```
Per-tier scan threads (5 platform daemon threads)
  └─ ScheduledExecutorService, 每个 tier 一个
      └─ scanAndDispatch(tier) @ precisionWindowMs 间隔

Per-tier batch consumers (N virtual threads per tier)
  └─ VirtualThreadPerTaskExecutor (共享)
      └─ runBatchConsumer(tier): acquire → poll → deliver → finalize

Cohort wake thread (1 platform daemon)
  └─ park until earliest cohort → flush to buckets → trigger scan

Cold swap-in thread (1 platform daemon)
  └─ every 30s: headMap(now+60s) → readRecord → decode → restore
```

### 关键文件

| 组件 | 文件 |
|------|------|
| 调度器 | `loomq-core/.../application/scheduler/PrecisionScheduler.java` |
| 队列管理 | `loomq-core/.../application/scheduler/CohortManager.java` |
| 时间桶 | `loomq-core/.../application/scheduler/BucketGroupManager.java` |
| 时间桶组 | `loomq-core/.../application/scheduler/BucketGroup.java` |
| 并发信号量 | `loomq-core/.../application/scheduler/ResizableSemaphore.java` |
| 冷热交换 | `loomq-core/.../application/swap/ColdIntentSwapper.java` |
| Tier 配置 | `loomq-core/.../domain/intent/PrecisionTierCatalog.java` |

---

## 链路 3：触发链路 (Dispatch)

**核心问题：** 如何在正确的时刻触发 Intent？

### 扫描-触发管线

```
[Per-tier scan thread] @ precisionWindowMs 间隔
  │
  ├─ BucketGroup.scanDue(now)
  │     ├─ currentBucketKey = floor(nowMs / precisionWindow) * precisionWindow
  │     ├─ buckets.headMap(currentBucketKey, true) → 所有到期桶
  │     ├─ 原子移除桶，更新 pendingCount
  │     └─ 过滤: intent.executeAt > now → 重新放入 (精度窗口内)
  │
  ├─ 结果按 (executeAt, createdAt, intentId) 排序
  │
  └─ 逐个 enqueue 到 tierDispatchQueues.get(tier)
        └─ offer 失败 (背压) → 回退到 SCHEDULED，下轮重试
```

### 消费-投递管线

```
[Per-tier batch consumer] virtual thread
  │
  ├─ acquireWithBorrow(tier)
  │     ├─ tryAcquire(1) on own tier → 成功则返回
  │     ├─ 遍历低优先级 tier → tryAcquire(1) (受 MAX_LEND_RATIO=0.5 约束)
  │     └─ 回退: blocking acquire on own tier
  │
  ├─ poll from tierDispatchQueues.get(tier)
  │
  ├─ deliveryHandler.deliverAsync(intent) ← SPI 调用
  │     └─ CompletableFuture<DeliveryResult>, 30s timeout
  │
  └─ .whenComplete() callback
        ├─ release semaphore
        └─ finalizeIntent() on sharedExecutor [virtual thread]
              ├─ SUCCESS → DUE → DISPATCHING → DELIVERED → ACKED
              ├─ RETRY → calculateDelay(attempts) → SCHEDULED → schedule()
              ├─ DEAD_LETTER → DEAD_LETTERED
              └─ EXPIRED → EXPIRED
```

### 过期检查 (频率分频)

```
ULTRA/FAST: 每轮扫描检查
HIGH:       每 3 轮检查
STANDARD:   每 5 轮检查
ECONOMY:    每 10 轮检查
```

### 关键文件

| 组件 | 文件 |
|------|------|
| 扫描逻辑 | `loomq-core/.../application/scheduler/PrecisionScheduler.java` (scanAndDispatch) |
| 消费循环 | `loomq-core/.../application/scheduler/PrecisionScheduler.java` (runBatchConsumer) |
| 跨 tier 借用 | `loomq-core/.../application/scheduler/PrecisionScheduler.java` (acquireWithBorrow) |
| 投递 SPI | `loomq-core/.../spi/DeliveryHandler.java` |
| 投递结果 | `loomq-core/.../spi/DeliveryResult.java` (推测) |

---

## 链路 4：投递链路 (Delivery)

**核心问题：** 如何将 Intent 可靠地送达用户回调？

### HTTP Webhook 投递 (主路径)

```
PrecisionScheduler 触发
  │
  └─ NettyHttpDeliveryHandler.deliverAsync(intent)  [SPI 实现]
        │
        ├─ Reactor Netty HttpClient (连接池, keep-alive, 30s timeout)
        │
        ├─ POST intent.callback.url
        │     Headers: Content-Type, X-LoomQ-Intent-Id, + callback.headers
        │     Body: {"intentId":"...", "precisionTier":"..."}
        │
        └─ CompletableFuture<DeliveryResult>
              ├─ 2xx → SUCCESS
              ├─ RedeliveryDecider.shouldRedeliver(context) == true → RETRY
              └─ otherwise → DEAD_LETTER
```

### 事件通知投递 (CallbackHandler)

```
IntentCommandService.cancelIntent() / delivery failure
  │
  └─ dispatchCallback(intent, CANCELLED/FAILED, error)
        │
        └─ HttpCallbackHandler.onIntentEvent(intent, type, error)  [async]
              │
              ├─ java.net.http.HttpClient (virtual thread executor)
              ├─ POST/PUT/PATCH intent.callback.url
              │     Body: {intentId, eventType, status, executeAt, payload, error}
              │     Headers: Content-Type, X-LoomQ-Intent-Id, X-LoomQ-Event-Type, + callback.headers
              └─ Retry: max 3 次, exponential backoff (100ms → 5s max)
```

### 关键文件

| 组件 | 文件 |
|------|------|
| 投递处理器 | `loomq-channel/loomq-channel-http/.../NettyHttpDeliveryHandler.java` |
| 回调处理器 | `loomq-channel/loomq-channel-http/.../HttpCallbackHandler.java` |
| 回调客户端 | `loomq-channel/loomq-channel-http/.../HttpCallbackClient.java` |
| 重试决策 SPI | `loomq-core/.../spi/RedeliveryDecider.java` |
| 默认重试策略 | `loomq-core/.../spi/DefaultRedeliveryDecider.java` |

---

## 链路 5：重试链路 (Retry)

**核心问题：** 投递失败后如何重试？死信如何处理？

### 重试决策树

```
deliveryHandler.deliverAsync(intent) 返回结果
  │
  ├─ SUCCESS (2xx)
  │     └─ DUE → DISPATCHING → DELIVERED → ACKED
  │
  ├─ 非 2xx / 异常
  │     ├─ RedeliveryDecider.shouldRedeliver(context)
  │     │     └─ DefaultRedeliveryDecider:
  │     │           retry on: timeout, connection-error, 5xx
  │     │           don't retry on: 4xx, success
  │     │
  │     ├─ [should retry]
  │     │     ├─ attempts < maxAttempts
  │     │     ├─ delay = RedeliveryPolicy.calculateDelay(attempts)
  │     │     │     └─ backoff * 2^attempts + jitter
  │     │     ├─ intent.executeAt = now + delay
  │     │     ├─ SCHEDULED (重新进入调度)
  │     │     └─ scheduler.schedule(intent)
  │     │
  │     └─ [should not retry / max attempts exceeded]
  │           └─ DEAD_LETTERED
  │
  └─ EXPIRED (deadline exceeded)
        └─ expiredAction 决定:
              ├─ DISCARD → EXPIRED
              └─ DEAD_LETTER → DEAD_LETTERED
```

### 死信复活

```
POST /v1/intents/{id}/revive
  │
  ├─ 仅 DEAD_LETTERED 状态有效
  ├─ 可选修改: executeAt, deadline, callback, redelivery
  ├─ attempts 重置为 0
  ├─ if executeAt == null || executeAt < now → executeAt = now + precisionWindow
  └─ engine.createIntent(intent, ackMode) → 重新进入写入链路
```

### 关键文件

| 组件 | 文件 |
|------|------|
| 重试决策 | `loomq-core/.../spi/RedeliveryDecider.java` |
| 默认策略 | `loomq-core/.../spi/DefaultRedeliveryDecider.java` |
| 投递上下文 | `loomq-core/.../spi/DeliveryContext.java` |
| 重试调度 | `loomq-core/.../application/scheduler/PrecisionScheduler.java` (finalizeIntent) |

---

## 链路 6：查询链路 (Query)

**核心问题：** 如何读取 Intent 状态？

### 查询端点

```
GET /v1/intents/{id}                 → intentStore.findById()
GET /v1/intents?status=X&limit=N     → intentStore.findByStatus()
GET /v1/intents/{id}/trace           → IntentTraceStore.get()
GET /health                          → WAL + Raft 状态
GET /health/live                     → 静态 {"status":"ALIVE"}
GET /health/ready                    → WAL + Raft leader + quorum + lease
GET /health/deep                     → + tier 背压详情
GET /metrics                         → LoomQMetrics.MetricsSnapshot (40+ 指标)
GET /v1/system/chronoscope           → 调度器内部状态
GET /v1/system/timeline?from=&to=    → 时间线查询
GET /v1/system/wal/segments          → WAL 段列表
GET /v1/system/wal/replay?id=        → WAL 重放
```

### Raft 读取路径

```
[非 Raft 模式]
  GET → engine.getIntent() → intentStore.findById() → 直接返回

[Raft 模式]
  GET → isRaftFollowerReadBlocked() 检查
    ├─ Leader + readLease 有效 → 直接读取 (线性一致)
    ├─ Leader + lease 过期 → 503 (RAFT_READ_LEASE_UNAVAILABLE)
    └─ Follower → 503 (RAFT_NOT_LEADER)
```

### 响应序列化 (零拷贝快速路径)

```
NettyRequestHandler 响应写回:
  ├─ byte[] → 直接写入
  ├─ MetricsSnapshot → MetricsResponseSerializer (直接写 ByteBuf)
  ├─ IntentResponse / HttpErrorResponse → DirectSerializedResponse (手写 JSON)
  └─ 其他 → Jackson fallback (JsonCodec)
```

### 关键文件

| 组件 | 文件 |
|------|------|
| Intent 处理器 | `loomq-server/.../http/netty/IntentHandler.java` |
| 指标序列化 | `loomq-server/.../http/netty/MetricsResponseSerializer.java` (推测) |
| 健康检查 | `loomq-server/.../server/LoomqServerApplication.java` (registerSystemRoutes) |
| 存储接口 | `loomq-core/.../store/IntentStore.java` |

---

## 链路 7：持久化链路 (Persistence)

**核心问题：** 如何保证数据不丢失？

### WAL 写入管线

```
IntentCommandService.createIntent()
  │
  ├─ IntentBinaryCodec.encode(intent) [CPU, 与 IO 重叠]
  │     └─ 格式: fieldCount(1B) + [fieldType(1B) + fieldLen(4B) + fieldValue]*
  │
  ├─ SimpleWalWriter.writeAsync/writeDurable(payload)
  │     │
  │     ├─ ensureCapacity() → 可能触发 segment rotation
  │     ├─ writePosition.getAndAdd(recordSize) [lock-free 原子递增]
  │     ├─ 写入 mmap region: header(4B) + payload + CRC32(4B)
  │     │
  │     ├─ [ASYNC] 返回 position 立即 (不等待 fsync)
  │     └─ [DURABLE] awaitFlush(position) → 阻塞直到 flush 线程 fsync
  │
  └─ ConcurrentIntentStore.save() [memory]
```

### WAL 段管理

```
SimpleWalWriter
  │
  ├─ Segment 文件: wal-000001.bin, wal-000002.bin, ...
  │     ├─ 每段 = segmentSize bytes (预分配 via channel.truncate)
  │     ├─ 内存映射 via FileChannel.map() + Arena
  │     └─ 全局偏移: segment.startGlobalOffset = (idx-1) * segmentSize
  │
  ├─ Flush 线程 (platform daemon "wal-flusher")
  │     ├─ 条件: pendingBytes >= flushThreshold(64KB) || flushRequested
  │     ├─ mappedRegion.force() (msync)
  │     ├─ 更新 flushedPosition
  │     └─ StripedCondition.signalRange() → 唤醒等待线程
  │
  └─ Segment recovery (startup)
        └─ 扫描 wal-*.bin, 读取记录, 验证 CRC, 恢复 writePosition/flushedPosition
```

### 快照管线

```
RecoveryPipeline.startSnapshots() [每 5 分钟]
  │
  ├─ SnapshotManager.createSnapshot(store, walOffset)
  │     ├─ 遍历 store.getAllIntents()
  │     ├─ IntentBinaryCodec.encode() 每个 intent
  │     ├─ GZIP 压缩
  │     ├─ 格式: magic(4B) + version(4B) + createdAt(8B) + walOffset(8B) + count(4B) + [len+bytes]*
  │     ├─ 原子写入 .snap.gz (temp + rename)
  │     └─ 保留最多 3 个快照
  │
  └─ walAccessor.truncateBefore(snapshotInfo.walOffset)
        └─ 删除 endGlobalOffset <= snapshotOffset 的段文件
```

### Raft 元数据持久化

```
SimpleWalWriter (WalAccessor 实现)
  │
  ├─ raft_meta 文件 (text format)
  │     ├─ term, votedFor, lastLogEntryTerm
  │     └─ snapshotIndex, snapshotTerm, snapshotOffset
  │
  └─ 持久化方式: 原子 temp-file + rename
```

### 关键文件

| 组件 | 文件 |
|------|------|
| WAL 写入 | `loomq-core/.../infrastructure/wal/SimpleWalWriter.java` |
| 二进制编解码 | `loomq-core/.../infrastructure/wal/IntentBinaryCodec.java` |
| 条件变量 | `loomq-core/.../infrastructure/wal/StripedCondition.java` |
| 快照管理 | `loomq-core/.../snapshot/SnapshotManager.java` |
| 恢复管线 | `loomq-core/.../recovery/RecoveryPipeline.java` |

---

## 链路 8：恢复链路 (Recovery)

**核心问题：** 崩溃后如何恢复状态？

### 启动恢复流程

```
LoomqEngine.start()
  │
  └─ recoveryPipeline.recover(intentStore, scheduler, walWriter)
        │
        ├─ 1. 清空 IntentStore 和 BucketGroupManager
        │
        ├─ 2. SnapshotManager.restoreFromSnapshot()
        │     ├─ 找到最新 .snap.gz
        │     ├─ GZIP 解压
        │     ├─ 逐条 IntentBinaryCodec.decode()
        │     └─ store.upsert() + scheduler.restore() 每个 intent
        │           └─ restore: bucketGroupManager.add() + indexIntent()
        │               (仅非终态 + executeAt != null 的 intent)
        │
        └─ 3. WalReplayManager.replay(walAccessor, snapshotOffset)
              ├─ 列出所有 WAL 段
              ├─ 跳过 snapshotOffset 之前的段
              ├─ 顺序读取记录, 验证 CRC
              ├─ IntentBinaryCodec.decode() 每条
              └─ store.upsert() + scheduler.restore()
```

### 冷 Intent 换入

```
ColdIntentSwapper.swapInLoop() [每 30s]
  │
  ├─ coldIndex.headMap(nowMs + 60_000, true)
  │     └─ 找到 60 秒内到期的冷 Intent
  │
  └─ 对每个 ColdIntentEntry:
        ├─ walWriter.readRecord(walPosition, recordLength) → 原始字节
        ├─ IntentBinaryCodec.decode(payload)
        ├─ 验证 intentId 匹配
        ├─ intentStore.upsert(intent)
        ├─ scheduler.restore(intent) → BucketGroupManager.add()
        └─ coldIndex.remove(entry)
```

### 关键文件

| 组件 | 文件 |
|------|------|
| 恢复管线 | `loomq-core/.../recovery/RecoveryPipeline.java` |
| WAL 重放 | `loomq-core/.../recovery/WalReplayManager.java` |
| 快照恢复 | `loomq-core/.../snapshot/SnapshotManager.java` |
| 冷换入 | `loomq-core/.../application/swap/ColdIntentSwapper.java` |

---

## 链路 9：共识链路 (Consensus)

**核心问题：** 多节点间如何保持一致？

### Leader 选举

```
选举超时触发 (random 150-300ms)
  │
  └─ LeaderElection.startElection()
        ├─ term++, voteFor = self, 持久化 (wal.setTermAndVotedFor + persistRaftMeta)
        ├─ role = CANDIDATE
        │
        ├─ [单节点] becomeLeader(term) → 立即成为 Leader
        │
        └─ [多节点] onElectionStarted 回调
              └─ RaftNode.onElectionStarted()
                    ├─ 对每个 peer: transport.sendRequestVote(peerId, term, nodeId, lastIdx, lastTerm)
                    │     ├─ RaftTransport 编码 RequestVote payload
                    │     ├─ ReplicaClient.send(ReplicationRecord(type=RAFT_REQUEST_VOTE))
                    │     ├─ ReplicaServer → ackHandler → LeaderElection.handleRequestVote()
                    │     │     ├─ candidateTerm > term → stepDown
                    │     │     ├─ 未投票 || 已投给此 candidate && isUpToDate → grant
                    │     │     └─ 持久化 term+votedFor
                    │     └─ 返回 vote granted/rejected in Ack.raftResponse
                    │
                    └─ votesGranted >= majority → becomeLeader(term)
                          ├─ 取消选举定时器
                          ├─ role = LEADER
                          ├─ 启动 heartbeat 定时器
                          ├─ 续约 readLease
                          └─ 通知 RaftRuntimeListener
```

### 日志复制 (Heartbeat 驱动)

```
Leader heartbeat 定时器 @ heartbeatMs (50ms)
  │
  └─ 对每个 peer:
        │
        ├─ [follower 太落后] nextIndex < firstIndex()
        │     └─ sendInstallSnapshot(peerId) → 走快照链路
        │
        └─ [正常复制]
              ├─ 读取 entries [nextIndex..lastIndex] from RaftLog (batch max 1000)
              ├─ transport.sendAppendEntries(peerId, term, leaderId, prevLogIndex, prevLogTerm, entries, commitIdx)
              │     ├─ RaftTransport 编码 AppendEntries payload
              │     ├─ ReplicaClient.send(ReplicationRecord(type=RAFT_APPEND_ENTRIES))
              │     └─ ReplicaServer → ackHandler → LogReplication.handleAppendEntries()
              │           ├─ 验证 prevLogIndex/prevLogTerm
              │           ├─ 截断冲突条目 (Raft section 5.3)
              │           ├─ 追加新条目
              │           ├─ advance commitIndex = min(leaderCommit, lastIdx)
              │           ├─ applyCommitted() → IntentStore.upsert() + RaftRuntimeListener
              │           └─ 返回 (success, matchIndex)
              │
              ├─ [成功] 更新 peer.matchIndex/nextIndex, 统计 quorum acks
              ├─ [quorum 达成] renewReadLease(), advanceCommitIndexFromAllPeers()
              ├─ [更高 term] election.stepDown()
              └─ [拒绝] 使用 conflictIndex 快速回溯
```

### 快照传输 (InstallSnapshot)

```
Leader 检测 follower.nextIndex < raftLog.firstIndex() (日志已截断)
  │
  └─ RaftNode.sendInstallSnapshot(peerId)
        ├─ encodeStoreSnapshot(): 序列化所有 intents via IntentBinaryCodec
        ├─ raftLog.compactThrough(snapshotIndex, snapshotTerm)
        ├─ transport.sendInstallSnapshot(peerId, term, nodeId, index, term, data)
        │     ├─ RaftTransport 编码 InstallSnapshot payload
        │     ├─ ReplicaClient.send(ReplicationRecord(type=RAFT_INSTALL_SNAPSHOT))
        │     └─ ReplicaServer → ackHandler → RaftNode.handleInstallSnapshot()
        │           ├─ decodeStoreSnapshot(data) → List<Intent>
        │           ├─ store.clear()
        │           ├─ 每个 intent: store.upsert()
        │           ├─ raftLog.compactThrough(lastIncludedIndex, lastIncludedTerm)
        │           ├─ replication.resetToSnapshot(lastIncludedIndex)
        │           └─ 返回 lastIncludedIndex
        └─ 更新 peer.nextIndex/matchIndex
```

### Raft 写入协调

```
RaftWriteCoordinator
  │
  ├─ 写入序列化: ReentrantLock (一次一个写入)
  ├─ 背压: Semaphore(maxPendingWrites=64), 500ms timeout
  ├─ 请求去重: ConcurrentHashMap<String, CachedWrite> (15min TTL)
  │
  ├─ commitSnapshot(): create/revive → 全量快照提议
  ├─ commitMutation(): patch/cancel/fire-now → 读取当前状态 + 校验 revision + 应用 mutator
  │
  └─ 核心 commitWrite():
        acquire permit → acquire lock → verify leader →
        propose → awaitApplied(5s) → read from store
```

### Replication 传输层

```
ReplicaClient (Netty TCP client)
  ├─ Pipeline: IdleStateHandler(write 2s) → ReplicationProtocol → ClientHandler
  ├─ send(ReplicationRecord) → CompletableFuture<Ack>
  ├─ pendingAcks map + 30s timeout
  └─ Writer idle → heartbeat

ReplicaServer (Netty TCP server)
  ├─ Pipeline: IdleStateHandler(read idle) → ReplicationProtocol → ReplicaHandler
  ├─ setAckHandler (Raft mode): handler 返回自定义 Ack
  ├─ setRecordHandler (legacy): auto Ack.success()
  └─ Read idle → close (heartbeat timeout)

ReplicationProtocol (Netty codec)
  └─ 格式: length(4) + type(1) + payload
        类型: RECORD(0x01), ACK(0x02), HEARTBEAT(0x03), CATCHUP_REQUEST(0x04), CATCHUP_RESPONSE(0x05)
```

### 关键文件

| 组件 | 文件 |
|------|------|
| Raft 节点 | `loomq-raft/.../raft/RaftNode.java` |
| Leader 选举 | `loomq-raft/.../raft/LeaderElection.java` |
| 日志复制 | `loomq-raft/.../raft/LogReplication.java` |
| Raft 日志 | `loomq-raft/.../raft/RaftLog.java` |
| Raft 传输 | `loomq-raft/.../raft/RaftTransport.java` |
| 写入协调 | `loomq-raft/.../raft/RaftWriteCoordinator.java` |
| 复制客户端 | `loomq-raft/.../replication/client/ReplicaClient.java` |
| 复制服务端 | `loomq-raft/.../replication/server/ReplicaServer.java` |
| 复制协议 | `loomq-raft/.../replication/protocol/ReplicationProtocol.java` |

---

## 补充：实时流式链路 (WatchIntent)

**核心问题：** 如何将 Intent 生命周期事件实时推送给客户端？

```
Client → gRPC WatchIntent(WatchIntentRequest)
  │
  └─ GlobalIntentObserver.register(request, serverObserver)
        ├─ 指定 intentIds → 注册 per-intent watchers
        ├─ 未指定 → 注册 wildcard watcher
        └─ 设置 status filter

[当调度器触发生命周期事件时]
  Scheduler thread → IntentObserver.onScheduled/onDelivered/onDeadLettered/onExpired/onDeliveryFailed
    │
    └─ GlobalIntentObserver.dispatch(intentId, traceId, fromStatus, toStatus)
          ├─ 构建 IntentEvent proto
          └─ dispatchExecutor.submit(async) [virtual thread]
                ├─ 通知 per-intent watchers (status filter 匹配)
                ├─ 通知 wildcard watchers (status filter 匹配)
                └─ WatchEntry.deliver(event)
                      ├─ observer.isReady() → onNext()
                      └─ 不 ready → enqueue (max 256, drop oldest)
```

### 关键文件

| 组件 | 文件 |
|------|------|
| 全局观察者 | `loomq-channel/loomq-channel-grpc/.../GlobalIntentObserver.java` |
| gRPC 服务 | `loomq-channel/loomq-channel-grpc/.../LoomqGrpcService.java` |
| 观察者 SPI | `loomq-core/.../spi/IntentObserver.java` |

---

## 补充：配置链路

```
优先级 (高→低): JVM -D 系统属性 → 外部 ./config/application.yml → classpath application.yml → @DefaultValue

LoomqConfig (Singleton)
  ├─ SimpleYamlConfigLoader → YAML → Properties
  ├─ ServerConfig (23 字段: host, port, maxConnections, maxConcurrentBusinessRequests, ...)
  ├─ WalConfig (段大小, flush 阈值, flush 间隔, ...)
  ├─ SchedulerConfig (冷阈值, 默认 tier, ...)
  ├─ DispatcherConfig (投递超时, ...)
  ├─ RetryConfig (最大重试, backoff, ...)
  ├─ RecoveryConfig (快照间隔, ...)
  ├─ SecurityConfig (token auth, 恒定时间比较)
  └─ GrpcConfig (enabled, port, maxMessageSize, ...)
```

---

## 补充：Intent 生命周期状态机

```
                    ┌──────────┐
                    │ CREATED  │
                    └────┬─────┘
                         │ createIntent()
                         ▼
                    ┌──────────┐
              ┌─────│SCHEDULED │◄────┐
              │     └────┬─────┘     │
              │          │ schedule() │ retry (calculateDelay)
              │          ▼            │
         cancel()   ┌─────────┐      │
              │      │  DUE    │      │
              ▼      └────┬────┘      │
         ┌──────────┐     │ scan     │
         │CANCELED  │     ▼          │
         └──────────┘┌────────────┐  │
                     │DISPATCHING │──┘
                     └────┬───────┘  │
                          │ deliver  │
                          ▼          │
                     ┌──────────┐    │
                     │DELIVERED │    │
                     └────┬─────┘    │
                          │ ack      │
                          ▼          │
                     ┌──────────┐    │
                     │  ACKED   │    │
                     └──────────┘    │
                                     │
  ┌──────────────┐    max attempts   │
  │DEAD_LETTERED │◄──────────────────┘
  └──────┬───────┘
         │ revive
         ▼
     SCHEDULED (重新进入)

  ┌──────────┐
  │ EXPIRED  │  (deadline exceeded)
  └──────────┘
```

---

## 链路依赖关系图

```
                    ┌─────────────┐
                    │  链路1:写入  │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
      ┌──────────┐  ┌──────────┐  ┌──────────┐
      │链路7:持久化│  │链路2:调度 │  │链路9:共识 │
      └──────┬───┘  └──────┬───┘  └──────┬───┘
             │             │             │
             │             ▼             │
             │      ┌──────────┐         │
             │      │链路3:触发 │         │
             │      └──────┬───┘         │
             │             │             │
             │             ▼             │
             │      ┌──────────┐         │
             │      │链路4:投递 │         │
             │      └──────┬───┘         │
             │             │             │
             │             ▼             │
             │      ┌──────────┐         │
             │      │链路5:重试 │         │
             │      └──────────┘         │
             │                           │
             ▼                           ▼
      ┌──────────┐              ┌──────────┐
      │链路8:恢复 │              │链路6:查询 │
      └──────────┘              └──────────┘
```

---

## 模块与链路映射

| 模块 | 包含的链路 |
|------|-----------|
| `loomq-core` | 链路 1 (IntentCommandService), 2 (Scheduler), 3 (Dispatch), 5 (Retry 决策), 7 (WAL), 8 (Recovery), + SPI 定义 |
| `loomq-server` | 链路 1 (HTTP 入口), 6 (Query/Health/Metrics), 7 (RocksDB 存储), + 配置加载 |
| `loomq-channel-http` | 链路 4 (Delivery + Callback), 5 (RedeliveryDecider SPI 实现) |
| `loomq-channel-grpc` | 链路 1 (gRPC 入口), 6 (gRPC 查询), 补充 (WatchIntent 流式) |
| `loomq-raft` | 链路 9 (Consensus: 选举 + 复制 + 快照 + 写入协调) |

---

## 线程清单

| 线程名 | 类型 | 数量 | 职责 | 所属链路 |
|--------|------|------|------|---------|
| Netty boss | platform | 1 | 接受 TCP 连接 | 链路 1 |
| Netty worker | platform | 2*CPU | I/O 读写 | 链路 1, 6 |
| businessExecutor | virtual | 无限制 | HTTP 请求处理 | 链路 1, 6 |
| operationExecutor | virtual | 无限制 | Engine 操作 | 链路 1 |
| callbackExecutor | virtual | 无限制 | 回调通知 | 链路 4 |
| tier-scan-{tier} | platform daemon | 5 | 扫描到期桶 | 链路 3 |
| tier-consumer-{tier} | virtual | 3-16/tier | 消费并投递 | 链路 3, 4 |
| cohort-waker | platform daemon | 1 | 唤醒长延迟 cohort | 链路 2 |
| cold-swap-in | platform daemon | 1 | 冷 Intent 换入 | 链路 8 |
| wal-flusher | platform daemon | 1 | WAL fsync | 链路 7 |
| recovery-snapshot | daemon | 1 | 周期快照 | 链路 7, 8 |
| idempotency-cleanup | daemon | 1 | 清理过期幂等记录 | 链路 1 |
| election-timer | daemon | 1 | 选举超时 | 链路 9 |
| heartbeat-{peerId} | scheduled | 每 peer | Raft heartbeat | 链路 9 |
| gRPC boss | platform | 1 | gRPC 接受连接 | 链路 1 |
| gRPC worker | platform | 2*CPU | gRPC I/O | 链路 1, 6 |
| dispatchExecutor | virtual | 无限制 | WatchIntent 事件分发 | 补充 |
