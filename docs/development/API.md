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
| GET | `/health/ready` | Readiness probe (includes WAL health) |
| GET | `/metrics` | JSON metrics snapshot |
| GET | `/api/v1/metrics` | Same as `/metrics` |

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
| `shardKey` | Optional. Used for shard routing in cluster mode |
| `callback.url` | Required when `callback` is provided |
| `precisionTier` | Optional. Defaults to `STANDARD`. One of: ULTRA, FAST, HIGH, STANDARD, ECONOMY |
| `ackLevel` | Optional. Defaults to `ASYNC`. One of: ASYNC, DURABLE, REPLICATED |
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

### Error Responses

| Code | Condition |
|------|-----------|
| **404** | Intent not found |

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

### Error Responses

| Code | Condition |
|------|-----------|
| **404** | Intent not found |
| **422** | Intent in a non-modifiable terminal state; or `executeAt` in the past |

---

## Cancel Intent

`POST /v1/intents/{intentId}/cancel`

Cancels an intent in `SCHEDULED` or `DUE` state.

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
| **500** | Failed to cancel |

---

## Fire Now

`POST /v1/intents/{intentId}/fire-now`

Immediately triggers an intent regardless of its scheduled `executeAt`.

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
| **500** | Failed to fire |

---

## Health Check

`GET /health`

```json
{"status": "UP"}
```

## Liveness Probe

`GET /health/live`

```json
{"status": "ALIVE"}
```

## Readiness Probe

`GET /health/ready`

```json
{"status": "UP"}
```

Returns `{"status": "DOWN"}` if WAL is unhealthy.

## Metrics

`GET /metrics`

Returns a JSON snapshot of all kernel and server metrics. See [metrics.md](../operations/metrics.md) for field descriptions.
