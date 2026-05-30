# LoomQ API

The current standalone API is intent-based.

- Base path: `/v1`
- Resource: `intents`

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/intents` | Create a new intent |
| GET | `/v1/intents/{intentId}` | Get intent by ID |
| PATCH | `/v1/intents/{intentId}` | Update an existing intent |
| POST | `/v1/intents/{intentId}/cancel` | Cancel an intent |
| POST | `/v1/intents/{intentId}/fire-now` | Immediately trigger an intent |
| GET | `/health` | Health check |
| GET | `/health/live` | Liveness probe |
| GET | `/health/ready` | Readiness probe (WAL plus Raft client-traffic safety) |
| GET | `/health/deep` | Deep health probe (includes Raft and tier backpressure) |
| GET | `/metrics` | JSON metrics snapshot |
| GET | `/api/v1/metrics` | Same as `/metrics` |

---

## Authentication

By default local development runs without HTTP token authentication. When `security.enabled=true`, all `/v1/**`,
`/metrics`, and `/api/v1/metrics` requests must include the configured token header:

```http
X-Loomq-Token: <token>
```

`Bearer <token>` is also accepted. Health endpoints stay unauthenticated so liveness and readiness probes can keep
working without sharing application credentials.

---

## Create Intent

`POST /v1/intents`

### Request

```json
{
  "intentId": "intent_abc123def456",
  "executeAt": "2026-05-01T10:00:00Z",
  "deadline": "2026-05-01T10:05:00Z",
  "expiredAction": "DISCARD",
  "precisionTier": "STANDARD",
  "shardKey": "order-123",
  "ackLevel": "DURABLE",
  "callback": {
    "url": "https://example.com/webhook",
    "method": "POST",
    "headers": {"X-Request-Id": "abc-123"},
    "body": "{\"type\": \"timeout\"}"
  },
  "redelivery": {
    "maxAttempts": 5,
    "backoff": "exponential",
    "initialDelayMs": 1000,
    "maxDelayMs": 60000,
    "multiplier": 2.0,
    "jitter": true
  },
  "idempotencyKey": "req-abc-123",
  "tags": {"tenant": "demo"}
}
```

### Validation Rules

| Field | Rule |
|-------|------|
| `executeAt` | **Required.** Must be in the future |
| `deadline` | Optional. If set, must be after `executeAt` |
| `shardKey` | Optional. Used for shard routing in distributed deployments |
| `callback.url` | Required when `callback` is provided |
| `precisionTier` | Optional. Defaults to `STANDARD`. One of: ULTRA, FAST, HIGH, STANDARD, ECONOMY |
| `ackLevel` | Optional. Defaults to `ASYNC`. One of: ASYNC, DURABLE, REPLICATED. Controls API 返回前的持久化级别 — 见[持久性语义](../architecture/core-model.md#durability-semantics) |
| `expiredAction` | Optional. Defaults to `DISCARD`. One of: DISCARD, DEAD_LETTER |

### Response (201)

```json
{
  "intentId": "intent_abc123def456",
  "traceId": "a1b2c3d4",
  "status": "SCHEDULED",
  "executeAt": "2026-05-01T10:00:00Z",
  "deadline": "2026-05-01T10:05:00Z",
  "expiredAction": "DISCARD",
  "precisionTier": "STANDARD",
  "ackLevel": "DURABLE",
  "attempts": 0,
  "lastDeliveryId": null,
  "revision": 1,
  "createdAt": "2026-05-01T09:59:00Z",
  "updatedAt": "2026-05-01T09:59:00Z"
}
```

### Error Responses

| Code | Condition |
|------|-----------|
| **409** | `idempotencyKey` conflict — duplicate request with terminal state |
| **422** | Validation error — invalid `executeAt`, `deadline`, `precisionTier`, or `callback.url` |
| **429** | Backpressure — server overloaded. Response body includes `retryAfterMs` |
| **500** | Internal error — failed to persist or schedule |

---

## Get Intent

`GET /v1/intents/{intentId}`

Returns the current intent snapshot.

When Raft is enabled, this endpoint is leader-authoritative:

- the leader serves the read normally
- followers return `503 Service Unavailable`
- the error payload uses code `50301`
- the `details` map includes `retryable=true`, the node role, and the current leader id when known
- the leader only serves the read while its quorum freshness lease is valid, so a partitioned leader fails closed instead of serving stale data

### Error Responses

| Code | Condition |
|------|-----------|
| **404** | Intent not found |
| **503** | Raft follower rejected the read; retry on the leader |

---

## Update Intent

`PATCH /v1/intents/{intentId}`

Partial update. Only provided fields are changed.

### Request

```json
{
  "executeAt": "2026-05-01T10:30:00Z",
  "deadline": "2026-05-01T10:35:00Z",
  "expiredAction": "DEAD_LETTER",
  "redelivery": {"maxAttempts": 3},
  "tags": {"priority": "high"}
}
```

Updatable fields: `executeAt`, `deadline`, `expiredAction`, `redelivery`, `tags`.

When Raft is enabled, mutating calls require `X-LoomQ-Expected-Revision` so stale writers fail fast instead of overwriting newer state.

### Error Responses

| Code | Condition |
|------|-----------|
| **404** | Intent not found |
| **422** | Intent in a non-modifiable terminal state; or `executeAt` in the past |
| **428** | Missing `X-LoomQ-Expected-Revision` header in Raft mode |

---

## Cancel Intent

`POST /v1/intents/{intentId}/cancel`

Cancels an intent in `SCHEDULED` or `DUE` state.

Raft mode requires `X-LoomQ-Expected-Revision` and rejects stale revisions with a retryable `40902` conflict.

### Response (200)

```json
{
  "intentId": "intent_abc123def456",
  "status": "CANCELED"
}
```

### Error Responses

| Code | Condition |
|------|-----------|
| **404** | Intent not found |
| **422** | Intent in a non-cancellable state (already terminal) |
| **428** | Missing `X-LoomQ-Expected-Revision` header in Raft mode |
| **500** | Failed to cancel |

---

## Fire Now

`POST /v1/intents/{intentId}/fire-now`

Immediately triggers an intent regardless of its scheduled `executeAt`.

Raft mode requires `X-LoomQ-Expected-Revision` and rejects stale revisions with a retryable `40902` conflict.

### Response (200)

```json
{
  "intentId": "intent_abc123def456",
  "status": "DUE"
}
```

### Error Responses

| Code | Condition |
|------|-----------|
| **404** | Intent not found |
| **428** | Missing `X-LoomQ-Expected-Revision` header in Raft mode |
| **500** | Failed to fire |

---

## Health Check

`GET /health`

```json
{
  "status": "OK",
  "timestamp": "2026-05-14T00:00:00Z",
  "wal": {
    "status": "HEALTHY",
    "unflushedBytes": 0,
    "lastFsyncMsAgo": 12,
    "writePosition": 1024,
    "flushedPosition": 1024
  },
  "raft": {
    "enabled": true,
    "nodeId": "node-1",
    "role": "LEADER",
    "leaderId": "node-1",
    "term": 3,
    "commitIndex": 42,
    "lastApplied": 42,
    "commitLag": 0,
    "replicationLag": 0,
    "connectedPeers": 2,
    "totalPeers": 2,
    "quorumReachable": true,
    "peerReachability": {
      "node-2": true,
      "node-3": true
    },
    "acceptingReads": true,
    "acceptingWrites": true
  }
}
```

The top-level `status` mirrors the WAL health state and is typically `OK`, `WARNING`, or `CRITICAL`.

## Deep Health Probe

`GET /health/deep`

Returns the base health payload plus tier-level backpressure details under `tiers`.

## Liveness Probe

`GET /health/live`

```json
{"status": "ALIVE"}
```

## Readiness Probe

`GET /health/ready`

```json
{
  "status": "UP",
  "ready": true,
  "mode": "raft",
  "raftEnabled": true,
  "nodeId": "node-1",
  "role": "LEADER",
  "leaderId": "node-1",
  "term": 3,
  "commitIndex": 42,
  "lastApplied": 42,
  "commitLag": 0,
  "connectedPeers": 2,
  "totalPeers": 2,
  "quorumReachable": true,
  "acceptingReads": true,
  "acceptingWrites": true,
  "writePending": 0,
  "reason": "READY"
}
```

In embedded mode readiness reflects WAL health. In Raft mode, readiness is intentionally stricter and only returns HTTP 200 when the node is safe for client traffic:

- WAL is healthy
- the node is the current leader
- quorum is reachable
- the leader read freshness lease is valid
- `commitLag == 0`
- `writePending == 0`

Unready nodes return HTTP 503 with error code `50304` and a `details.reason` such as `WAL_UNHEALTHY`, `RAFT_NOT_LEADER`, `RAFT_QUORUM_UNREACHABLE`, `RAFT_READ_LEASE_UNAVAILABLE`, `RAFT_COMMIT_LAG`, or `RAFT_PENDING_WRITES`.

## Metrics

`GET /metrics`

Returns a JSON snapshot of all kernel and server metrics. See [metrics.md](../operations/metrics.md) for field descriptions.

The Raft section includes:

- `raftRole`
- `raftLeaderId`
- `raftTerm`
- `raftCommitIndex`
- `raftLastApplied`
- `raftCommitLag`
- `raftReplicationLag`
- `raftConnectedPeers`
- `raftTotalPeers`
