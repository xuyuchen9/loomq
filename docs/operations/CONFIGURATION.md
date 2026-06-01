# LoomQ 配置说明

LoomQ 当前使用两层配置来源：

1. `classpath:application.yml`
2. `file:./config/application.yml`

同时，少量环境变量或启动参数也会覆盖配置，例如 `LOOMQ_NODE_ID`、`LOOMQ_DATA_DIR`、`LOOMQ_RAFT_ENABLED`、`LOOMQ_RAFT_NODE_ID`、`LOOMQ_RAFT_PEERS`、`LOOMQ_RAFT_PORT`、`loomq.node.id`、`loomq.data.dir` 和 `loomq.raft.*`。

`LOOMQ_CLUSTER_ENABLED` 已退役；如果显式设置为 `true`，启动会直接失败。

## 配置优先级

从高到低：

1. JVM 系统属性
2. 外部 `./config/application.yml`
3. classpath `application.yml`
4. `@DefaultValue`

## 当前有效配置项

### Server

| Key | 默认值 | 作用 | 生产建议 |
|-----|--------|------|----------|
| `server.host` | `0.0.0.0` | HTTP 绑定地址 | 生产环境绑定到明确网卡或容器入口 |
| `server.port` | `7928` | HTTP 监听端口 | 与负载均衡和探针端口保持一致 |
| `server.backlog` | `1024` | 连接队列长度 | 高并发入口可适度调大 |
| `server.virtual_threads` | `true` | 是否启用虚拟线程处理请求 | 默认开启 |
| `server.max_request_size` | `10485760` | 请求体大小上限，字节 | 按回调体积上限设置 |
| `server.thread_pool_size` | `200` | 非虚拟线程模式下的线程池大小 | 仅在关闭虚拟线程时使用 |

### Identity / Raft

| Key | 默认值 | 作用 | 生产建议 |
|-----|--------|------|----------|
| `nodeId` / `loomq.node.id` | `node-1` | 本地节点标识 | 与部署环境中的实例名保持稳定映射 |
| `raft.enabled` / `loomq.raft.enabled` | `false` | 是否启用 Raft 模式 | Raft 部署应显式开启 |
| `raft.nodeId` / `loomq.raft.nodeId` | `node-1` | Raft 本地节点 ID | 与 `nodeId` 保持一致更简单 |
| `raft.peers` / `loomq.raft.peers` | 本机节点 ID | Raft peer 列表，单节点可只写本机节点 ID；多节点支持 `peerId@host:port` 或 `peerId=host:port` | 多节点部署必须为每个远端 peer 配置可连接端点 |
| `raft.port` / `loomq.raft.port` | `9928` | Raft RPC 监听端口 | 与防火墙和 peer 连接规划一致 |

#### Raft 启动校验

Raft 启动时会直接 fail fast，常见约束包括：

- `raft.nodeId` 不能为空
- `raft.peers` 不能为空，且必须包含本地 node id
- 重复的 peer id 会被拒绝
- 远端 peer 必须显式给出可连接端点
- `raft.port` 必须在 `[1, 65535]`
- `raft.port` 不能与 HTTP `server.port` 或 `netty.port` 相同

### Netty

| Key | 默认值 | 作用 | 生产建议 |
|-----|--------|------|----------|
| `netty.host` | `0.0.0.0` | 预留/兼容项；当前 HTTP 绑定以 `server.host` 为准 | 与 `server.host` 保持一致，避免配置摘要误导 |
| `netty.port` | `7928` | 预留/兼容项；当前 HTTP 监听以 `server.port` 为准 | 与 `server.port` 保持一致，避免配置摘要误导 |
| `netty.bossThreads` | `1` | boss 线程数 | 一般保持 1 |
| `netty.workerThreads` | `0` | worker 线程数 | `0` 表示使用 Netty 默认值 |
| `netty.maxContentLength` | `10485760` | HTTP body 上限，字节 | 与 `server.max_request_size` 对齐 |
| `netty.useEpoll` | `true` | Linux epoll 优化 | Linux 上保持开启 |
| `netty.pooledAllocator` | `true` | 是否使用池化分配器 | 生产建议开启 |
| `netty.soBacklog` | `1024` | socket backlog | 高峰连接场景可调大 |
| `netty.tcpNoDelay` | `true` | 关闭 Nagle | 默认开启 |
| `netty.connectionTimeoutMs` | `30000` | 连接超时，毫秒 | 视网络环境调整 |
| `netty.idleTimeoutSeconds` | `60` | 空闲超时，秒 | 视负载均衡器策略调整 |
| `netty.maxConnections` | `10000` | 最大连接数 | 按机器规格设置 |
| `netty.writeBufferHighWaterMark` | `1048576` | 写缓冲高水位，字节 | 视响应体大小调整 |
| `netty.writeBufferLowWaterMark` | `524288` | 写缓冲低水位，字节 | 应小于 high water mark |
| `netty.maxConcurrentBusinessRequests` | `2000` | 业务并发上限 | 保护下游 callback 路径 |
| `netty.httpSemaphoreTimeoutMs` | `500` | 获取业务信号量超时，毫秒 | 超时返回 429 |
| `netty.gracefulShutdownTimeoutMs` | `30000` | 优雅停机等待时间，毫秒 | 与运维停机窗口对齐 |

### WAL / 持久化

| Key | 默认值 | 作用 | 生产建议 |
|-----|--------|------|----------|
| `wal.data_dir` | `./data/wal` | WAL 根目录 | 放在独立磁盘或卷上 |
| `wal.segment_size_mb` | `64` | 段文件大小 | 维持默认起步即可 |
| `wal.flush_strategy` | `batch` | 刷盘策略 | 生产通常优先 `batch` |
| `wal.batch_flush_interval_ms` | `100` | 批量刷盘间隔，毫秒 | 结合 RPO 目标调整 |
| `wal.sync_on_write` | `false` | 写时同步刷盘 | 仅在极端可靠性要求下开启 |
| `wal.engine` | `memory_segment` | WAL 引擎 | 当前主路径 |
| `wal.memory_segment.initial_size_mb` | `64` | 初始映射大小，MB | 与预计负载匹配 |
| `wal.memory_segment.max_size_mb` | `1024` | 最大映射大小，MB | 与磁盘容量匹配 |
| `wal.memory_segment.flush_threshold_kb` | `64` | 刷盘阈值，KB | 影响吞吐与延迟平衡 |
| `wal.memory_segment.flush_interval_ms` | `10` | 刷盘间隔，毫秒 | 低延迟场景可保持较低值 |
| `wal.memory_segment.stripe_count` | `16` | 条带数量 | 影响等待和唤醒分布 |
| `wal.memory_segment.min_batch_size` | `100` | 最小批量 | 与吞吐目标相关 |
| `wal.memory_segment.adaptive_flush_enabled` | `true` | 自适应刷盘 | 建议保持开启 |

### Scheduler

| Key | 默认值 | 作用 | 生产建议 |
|-----|--------|------|----------|
| `scheduler.max_pending_intents` | `1000000` | 最大待处理 Intent 数 | 按 heap 和恢复时长评估 |

精度档位默认参数（在 `PrecisionTierCatalog` 中硬编码，暂不支持外部配置）：

| 档位 | 窗口 | 最大并发 | 批量 | 消费者数 | WAL 模式 |
|------|------|---------|------|---------|----------|
| ULTRA | 10ms | 200 | 1×5ms | 16 | ASYNC |
| FAST | 50ms | 150 | 1×10ms | 12 | ASYNC |
| HIGH | 100ms | 50 | 5×50ms | 4 | BATCH_DEFERRED |
| STANDARD | 500ms | 50 | 20×100ms | 3 | DURABLE |
| ECONOMY | 1000ms | 50 | 25×300ms | 2 | DURABLE |

AdapTBF 跨层借用约束（当前硬编码）：

| 参数 | 值 | 说明 |
|------|-----|------|
| `MAX_LEND_RATIO` | 0.5 | 每档位最多借出 50% slot |
| 借用超时 | 100ms | `tryAcquire` 超时，超时后尝试下一档位 |
| 回退策略 | `acquire()` 阻塞 | 所有档位均无法借用时阻塞在自身信号量上 |

### Dispatcher / Retry / Recovery

| Key | 默认值 | 作用 |
|-----|--------|------|
| `dispatcher.http_timeout_ms` | `3000` | HTTP 超时 |
| `dispatcher.max_concurrent_dispatches` | `1000` | 并发投递上限 |
| `dispatcher.connect_timeout_ms` | `5000` | 连接超时 |
| `dispatcher.read_timeout_ms` | `3000` | 读取超时 |
| `dispatcher.follow_redirects` | `true` | 是否跟随重定向 |
| `dispatcher.retry_on_timeout` | `true` | 超时是否重试 |
| `dispatcher.backoff_strategy` | `exponential` | 退避策略 |
| `retry.initial_delay_ms` | `1000` | 初始退避 |
| `retry.max_delay_ms` | `60000` | 最大退避 |
| `retry.multiplier` | `2.0` | 指数退避倍率 |
| `retry.default_max_retry` | `5` | 默认最大重试次数 |
| `recovery.batch_size` | `1000` | 恢复批次大小 |
| `recovery.sleep_ms` | `10` | 恢复轮询间隔 |
| `recovery.concurrency_limit` | `100` | 恢复并发上限 |
| `recovery.safe_mode` | `false` | 安全模式 |
| `recovery.auto_start` | `true` | 启动时是否自动恢复 |
| `recovery.checkpoint_interval_sec` | `60` | 检查点间隔，秒 |

### Security

| Key | 默认值 | 作用 | 生产建议 |
|-----|--------|------|----------|
| `security.enabled` | `false` | 是否启用 HTTP token 认证 | 生产环境建议开启 |
| `security.token_header` | `X-Loomq-Token` | 认证 token 所在请求头 | 与网关或 sidecar 配置一致 |
| `security.tokens` | 空 | 允许的 token 列表 | 使用环境变量或外部配置注入，不要提交明文生产 token |

开启 `security.enabled=true` 时，`/v1/**` 和 `/metrics` / `/api/v1/metrics` 需要携带 token；`/health`、
`/health/live`、`/health/ready`、`/health/deep` 保持开放，便于负载均衡器和 Kubernetes 探针工作。token
既可以直接放在 `X-Loomq-Token`，也可以使用 `Bearer <token>` 形式。

如果开启 security 但没有配置任何非空 token，进程会在启动阶段 fail fast，避免进入“看似开启、实际裸奔”的状态。

### Metrics / Health / Logging

这些项当前都在配置文件里支持：

- `metrics.*`（包含 Raft 的 role、leader、term、commit lag、peer reachability 等运行态字段）
- `health.*`（`/health` 和 `/health/deep` 会包含 WAL 与 Raft 安全信号；`/health/ready` 在 Raft 模式下只允许当前安全 leader 接客户端流量）
- `logging.*`

## 启动摘要

当前 `loomq-server` 启动时会打印关键运行摘要，包括：

- `nodeId`
- `dataDir`
- `server` 和 `netty` 绑定参数
- `wal` 的引擎与刷盘配置
- `scheduler`、`retry`、`recovery` 的核心阈值

这能帮助排查“配置写了但没有生效”或“节点之间配置不一致”的问题。

## 一点约定

- 新增配置项时，优先补默认值和单位
- 公共文档里优先使用 `Intent`，不要再把新文档写成旧语义体系
- 如果某个配置尚未真正进入执行路径，要在文档里明确标成“预留”或“实验”
- `loomq.data.dir` 当前作为 WAL 根目录覆盖项使用，优先级高于 `wal.data_dir`
