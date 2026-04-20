# Metrics

LoomQ exposes two metric layers:

- `LoomQMetrics` for kernel and intent lifecycle state
- `HttpMetrics` for Netty request traffic and backpressure

## Core JSON Snapshot

The standalone server exposes a fast JSON snapshot at `/metrics`.

Common fields include:

- `intentsCreated`
- `intentsCompleted`
- `intentsCancelled`
- `intentsExpired`
- `intentsDeadLettered`
- `deliveriesTotal`
- `deliveriesSuccess`
- `deliveriesFailed`
- `deliveriesRetried`
- `pendingIntents`
- `activeDispatches`
- `walHealthy`
- `walIdleTimeMs`
- `walFlushErrorCount`

## Prometheus HTTP Metrics

The Netty layer also registers Prometheus counters, gauges, and histograms such as:

- `loomq_http_requests_total`
- `loomq_http_request_duration_seconds`
- `loomq_http_active_requests`
- `loomq_http_concurrency_limit_exceeded_total`
- `loomq_netty_active_connections`
- `loomq_netty_connection_errors_total`

## What To Watch

### Kernel health

- `walHealthy` should remain `true`
- `walIdleTimeMs` should stay bounded during active traffic
- `pendingIntents` should not grow without a matching workload increase

### Backpressure

- `loomq_http_concurrency_limit_exceeded_total` indicates the server is shedding load
- rising `activeRequests` with flat throughput usually means the downstream path is slow

### Latency

- request duration
- delivery latency
- wakeup latency by precision tier

## Dashboard Starter

Suggested panels:

- request rate
- active requests
- pending intents
- WAL health
- delivery success vs failure
- wakeup p95/p99 by precision tier
- backpressure rejects

