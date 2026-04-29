# Metrics

LoomQ exposes metrics through two layers:

- `MetricsCollector` in `loomq-core` — operational counters, per-tier histograms, latency tracking
- `LoomQMetrics` / `HttpMetrics` in `loomq-server` — composite snapshots + HTTP traffic

## Access

| Endpoint | Format | Description |
|----------|--------|-------------|
| `GET /metrics` | JSON | Full metrics snapshot (kernel + server) |
| `GET /api/v1/metrics` | JSON | Same as `/metrics` |
| Prometheus export | Text | Via `MetricsCollector.exportPrometheusMetrics()` |

---

## Intent Lifecycle Counters

| Metric | Description |
|--------|-------------|
| `intentsCreated` | Total intents created |
| `intentsCompleted` | Intents reaching ACKED state |
| `intentsCancelled` | Intents explicitly cancelled |
| `intentsExpired` | Intents that expired past deadline |
| `intentsDeadLettered` | Intents moved to dead letter queue |
| `pendingIntents` | Current count of non-terminal intents |
| `activeDispatches` | Intents currently in DISPATCHING state |

---

## Delivery Metrics

| Metric | Description |
|--------|-------------|
| `deliveriesTotal` | Total delivery attempts |
| `deliveriesSuccess` | Successful deliveries (ACKED) |
| `deliveriesFailed` | Failed deliveries |
| `deliveriesRetried` | Deliveries scheduled for retry |

---

## Per-Tier Metrics

Each precision tier (ULTRA, FAST, HIGH, STANDARD, ECONOMY) reports:

### Counters

| Metric | Description |
|--------|-------------|
| `intent_total{precision_tier}` | Intents created per tier |
| `intent_due_total{precision_tier}` | Intents that became due per tier |
| `backpressure_events{precision_tier}` | Queue-full events per tier |
| `dispatch_queue_offer_failed{precision_tier}` | Failed queue offers per tier |

### Gauges

| Metric | Description |
|--------|-------------|
| `bucket_size{precision_tier}` | Current items in time-bucket |
| `dispatch_queue_size{precision_tier}` | Current dispatch queue depth |

### Latency Histograms (P50/P75/P90/P95/P99/P99.9)

| Metric | Description |
|--------|-------------|
| `scan_duration_ms{precision_tier}` | Scan-and-dispatch cycle time |
| `wakeup_latency_ms{precision_tier}` | executeAt → actual wake-up time |
| `dispatch_queue_lag_ms{precision_tier}` | Queue entry → dequeued by consumer |

---

## RTT Metrics (Delivery Latency)

Independent of scheduling precision — measures pure delivery time:

| Tier | dequeue→webhook received |
|------|--------------------------|
| ULTRA | p50/p95/p99 in ms |
| FAST | p50/p95/p99 in ms |
| HIGH | p50/p95/p99 in ms |
| STANDARD | p50/p95/p99 in ms |
| ECONOMY | p50/p95/p99 in ms |

Reported via `RESULT_RTT` marker in benchmark output. Parsed by `run-all.ps1` into JSON.

---

## Arrow Borrowing Metrics

| Metric | Description |
|--------|-------------|
| `own_direct` | Successful immediate acquires on own tier |
| `own_blocking` | Blocking acquires (no slots available, no borrow possible) |
| `borrowed` | Successful cross-tier borrows |
| `borrow_timeouts` | Failed borrow attempts (all tiers at lend limit or no idle slots) |
| `borrow_rate` | `borrowed / total_acquires * 100` |

---

## Cohort Metrics

| Metric | Description |
|--------|-------------|
| `cohort_count` | Active cohort groups |
| `pending_intent_count` | Intents waiting in cohorts |
| `total_registered` | Total intents registered with CohortManager |
| `total_flushed` | Total intents flushed to bucket groups |
| `wake_event_count` | Total cohort wake events |

---

## WAL Health

| Metric | Description |
|--------|-------------|
| `walHealthy` | `true` if WAL is operational |
| `walIdleTimeMs` | Time since last flush |
| `walFlushErrorCount` | Cumulative flush errors |
| `walWritePosition` | Current write offset |
| `walFlushedPosition` | Last flushed offset |
| `walUnflushedBytes` | Bytes written but not flushed |

---

## HTTP Traffic (Netty Layer)

| Metric | Type | Description |
|--------|------|-------------|
| `loomq_http_requests_total` | Counter | Total HTTP requests |
| `loomq_http_request_duration_seconds` | Histogram | Request duration |
| `loomq_http_active_requests` | Gauge | In-flight requests |
| `loomq_http_concurrency_limit_exceeded_total` | Counter | Requests rejected by semaphore |
| `loomq_netty_active_connections` | Gauge | Open connections |
| `loomq_netty_connection_errors_total` | Counter | Connection-level errors |

---

## What To Watch

### Kernel Health

- `walHealthy` must stay `true`
- `walIdleTimeMs` should be bounded during active traffic
- `pendingIntents` should not grow without workload increase

### Backpressure

- Non-zero `loomq_http_concurrency_limit_exceeded_total` → server shedding load
- Non-zero `backpressure_events{*}` → tier dispatch queue full
- Rising `borrow_timeouts` → AdapTBF cap may be too tight for workload

### Latency Degradation

- Rising `wakeup_latency_ms` → scheduler scan falling behind
- Rising `dispatch_queue_lag_ms` → insufficient consumers or semaphore too tight
- Rising RTT p95 → downstream webhook slow or network issue

### Borrowing Health

- `borrow_rate` > 15% → own tier concurrency may be too low
- `borrow_timeouts` >> `borrowed` → AdapTBF ratio too restrictive
- `own_blocking` growing → system approaching total saturation
