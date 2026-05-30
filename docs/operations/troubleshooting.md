# Troubleshooting

This guide focuses on the issues most likely to show up in the current codebase.

## Server Does Not Start

Check:

- the configured port is free
- `server.host` and `server.port` are valid
- the WAL directory is writable
- the startup log shows the runtime summary

## `/health` Is Unhealthy

Check:

- WAL flush activity
- `walHealthy`
- recent errors in the server log
- disk space and file permissions

## `/health/ready` Returns 503

Readiness is stricter than liveness. `GET /health/live` only proves the process is up; `GET /health/ready` means the node is safe to receive client traffic.

Check `error.details.reason`:

| Reason | Meaning | First checks |
|--------|---------|--------------|
| `WAL_UNHEALTHY` | Persistence is not safe | disk space, permissions, WAL flush errors |
| `RAFT_NOT_LEADER` | This node is healthy but should not receive client API traffic | retry on `leaderId`, check load-balancer routing |
| `RAFT_QUORUM_UNREACHABLE` | The leader cannot see enough peers | peer endpoints, firewall, Raft port, pod DNS |
| `RAFT_READ_LEASE_UNAVAILABLE` | Leader freshness lease is unavailable | peer connectivity and election churn |
| `RAFT_COMMIT_LAG` | Committed entries are not fully applied locally | disk pressure, apply loop errors, log volume |
| `RAFT_PENDING_WRITES` | Writes are in flight and readiness is failing closed | sustained write load, proposal latency, client retry behavior |

Use `/health` for a full Raft snapshot and `/metrics` for trend data such as `raftCommitLag`, `raftPendingWrites`, `raftWriteTimeouts`, and `raftWriteStepDownAborts`.

## `/metrics` Looks Flat

Check:

- whether there was any traffic
- whether the path was hit through the standalone server
- whether the metrics snapshot is being scraped from the right instance

## Requests Are Rejected

If you see service-unavailable responses or rising rejects:

- inspect `loomq_http_concurrency_limit_exceeded_total`
- inspect `raftWriteBackpressureRejects`, `raftWriteTimeouts`, and `raftWriteRevisionConflicts`
- check `netty.maxConcurrentBusinessRequests`
- check `activeRequests`
- check `/health/ready` for `leaderId`, `acceptingReads`, and `acceptingWrites`
- verify the downstream callback target is healthy

## Recovery Is Slow

Check:

- WAL directory size
- snapshot cadence
- restart disk throughput
- the `recovery.batch_size` and `recovery.concurrency_limit` settings

## Intent Behavior Looks Wrong

Check the lifecycle first:

- `executeAt` must be in the future on create
- `deadline` must be after `executeAt`
- only cancellable states can be cancelled
- only modifiable states can be patched

If the behavior still looks inconsistent, compare the API response with the lifecycle defined in `IntentStatus`.

## Intent Lost After Crash

If Intents are missing after a process restart:

1. **检查 `ackLevel`**：使用 `ASYNC` 模式创建的 Intent 在崩溃窗口（API 返回到 WAL 落盘之间）内可能丢失。这是设计行为，不是 bug。详见[持久性语义](../architecture/core-model.md#durability-semantics)。

2. **检查 WAL 目录**：确认 WAL 段文件未被意外删除。

3. **检查快照**：`GET /health` 查看最近快照时间。如果快照间隔过长，WAL 截断后旧数据不可恢复。

4. **确认恢复日志**：启动日志应包含 `Recovery completed: snapshot=X, walReplay=Y`。如果 Y=0 且预期有 WAL 数据，检查 WAL 目录权限。

**预防措施**：

- 对可靠性要求高的场景，使用 `ackLevel: DURABLE` 或 `ackLevel: REPLICATED`
- 监控 WAL 健康状态：`walHealthy`、`walFlushDurationMs`
- 配置合理的快照间隔（默认 5 分钟）
