# LoomQ API

The current standalone API is intent-based.

- Base path: `/v1`
- Resource: `intents`

## Create Intent

`POST /v1/intents`

### Request

```json
{
  "intentId": "intent_123",
  "executeAt": "2026-04-20T10:00:00Z",
  "deadline": "2026-04-20T10:05:00Z",
  "expiredAction": "DISCARD",
  "precisionTier": "STANDARD",
  "shardKey": "order-123",
  "ackLevel": "DURABLE",
  "callback": {
    "url": "https://example.com/webhook",
    "method": "POST",
    "headers": {
      "X-Request-Id": "abc-123"
    },
    "body": {
      "type": "timeout"
    }
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
  "tags": {
    "tenant": "demo"
  }
}
```

### Rules

- `executeAt` is required
- `deadline` is required
- `shardKey` is required
- `deadline` must be after `executeAt`
- `executeAt` must be in the future
- `callback.url` is required when `callback` is provided

### Response

```json
{
  "intentId": "intent_123",
  "status": "SCHEDULED",
  "executeAt": "2026-04-20T10:00:00Z",
  "deadline": "2026-04-20T10:05:00Z",
  "expiredAction": "DISCARD",
  "precisionTier": "STANDARD",
  "ackLevel": "DURABLE",
  "attempts": 0,
  "lastDeliveryId": null,
  "createdAt": "2026-04-20T09:59:00Z",
  "updatedAt": "2026-04-20T09:59:00Z"
}
```

## Get Intent

`GET /v1/intents/{intentId}`

Returns the current intent snapshot or `404` if the intent is missing.

## Patch Intent

`PATCH /v1/intents/{intentId}`

### Request

```json
{
  "executeAt": "2026-04-20T10:10:00Z",
  "deadline": "2026-04-20T10:15:00Z",
  "expiredAction": "DEAD_LETTER",
  "redelivery": {
    "maxAttempts": 8,
    "backoff": "fixed",
    "initialDelayMs": 2000,
    "maxDelayMs": 60000,
    "multiplier": 2.0,
    "jitter": false
  },
  "tags": {
    "tenant": "demo",
    "priority": "high"
  }
}
```

Only the non-null fields are applied.

## Cancel Intent

`POST /v1/intents/{intentId}/cancel`

The intent must be in a cancellable state.

## Fire Now

`POST /v1/intents/{intentId}/fire-now`

Triggers the intent immediately and returns a small action response:

```json
{
  "intentId": "intent_123",
  "status": "DISPATCHING"
}
```

## Error Handling

Validation errors use a `422` response with an application error code. Typical cases include:

- missing `executeAt`
- missing `deadline`
- missing `shardKey`
- invalid time ordering
- invalid callback URL

Lifecycle errors return `404` or `422` depending on whether the intent is missing or simply not in a valid state for the requested operation.

