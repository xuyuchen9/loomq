# LoomQ — Durable Time Kernel for Future Events

[![JDK](https://img.shields.io/badge/JDK-25%2B-green.svg)](https://openjdk.org/)
[![Maven Central](https://img.shields.io/badge/Maven%20Central-0.9.x-blue.svg)](https://central.sonatype.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Tests](https://img.shields.io/badge/Tests-622%20tests-brightgreen.svg)]()

**Making future events happen reliably, powered by Java 25 Virtual Threads.**

LoomQ is a durable time kernel for distributed systems — scheduling, persistence, recovery, retry orchestration, and deadline handling. Embed it as a library or run it as a standalone server.

## What LoomQ Is / Isn't

**LoomQ is:**

- a durable scheduling kernel for future events (called **Intents**)
- embeddable (`loomq-core`, zero HTTP/JSON deps) or standalone (`loomq-server`, Netty HTTP)
- extensible via SPI hooks for delivery, callbacks, and retry decisions

**LoomQ is not:**

- a general-purpose message queue
- a workflow engine
- a lock or lease service (those belong in layers above the kernel)

## Capability Maturity

| Category | Examples |
|----------|----------|
| **Stable** | durable delayed execution, persistence + recovery, precision-tier scheduling, retry orchestration, metrics, pluggable storage (in-memory + RocksDB), WAL segment rotation with snapshot compaction, IntentObserver lifecycle hooks, Raft leader election + log replication + snapshot catch-up, leader-authoritative reads/writes, Raft observability, token authentication, error recovery advisor, health narration, dead letter revival |
| **Beta** | dynamic Raft membership, Kubernetes operator |
| **Not yet committed** | distributed coordination primitives, lock/lease semantics |

---

## Quick Start

### Prerequisites

- JDK 25+
- Maven 3.9+

### Maven Dependency

```xml
<dependency>
    <groupId>com.loomq</groupId>
    <artifactId>loomq-core</artifactId>
    <version>0.9.2</version>
</dependency>
```

### Build from Source

```bash
git clone https://github.com/xuyuchen9/loomq.git
cd loomq
mvn clean package -DskipTests
```

### Start Server

```bash
java -jar loomq-server/target/loomq-server-0.9.2.jar
```

Server listens on `http://localhost:7928` by default.

### Create Your First Intent

```bash
curl -X POST http://localhost:7928/v1/intents \
  -H "Content-Type: application/json" \
  -d '{
    "executeAt": "'$(date -u -d "+30 seconds" +%Y-%m-%dT%H:%M:%SZ)'",
    "precisionTier": "STANDARD",
    "callback": {
      "url": "http://your-server/callback",
      "method": "POST"
    }
  }'
```

### Check Intent Status

```bash
curl http://localhost:7928/v1/intents/{intentId}
```

### Raft Mode Notes

When `LOOMQ_RAFT_ENABLED=true`, the standalone server switches to Raft mode:

- Leader-authoritative reads AND writes — `GET /v1/intents/{intentId}` and `POST/PATCH /v1/intents` require leader
- followers return `503` with error codes `50301` (read redirect), `50302` (write redirect), `50303` (write backpressure); `retryable=true` and leader id in `details` when known
- `RaftWriteCoordinator` provides bounded backpressure, request deduplication, and optimistic concurrency via `X-LoomQ-Expected-Revision` header
- `/health` and `/metrics` expose Raft signals: role, leader id, term, commit index, commit lag, replication lag, peer reachability, and leader read/write acceptance status
- `/health/deep` adds tier backpressure data on top of the Raft view
- `/health/ready` returns `503` with Raft reason codes when not in ready state

### LoomQ CLI — Interactive Temporal Explorer

```bash
# Build the CLI
mvn package -pl loomq-cli -am -DskipTests

# Run (defaults to http://localhost:8080, override with LOOMQ_URL env var)
java -jar loomq-cli/target/loomq-cli-0.9.2.jar

# Or set a custom server URL
$env:LOOMQ_URL="http://localhost:7928"; java -jar loomq-cli/target/loomq-cli-0.9.2.jar
```

Interactive commands: `schedule`, `get`, `list`, `chronoscope`, `timeline`, `dead-letters`, `revive`, `health`, `follow`, `help`, `exit`.

---

## Core Features

| Feature | Description |
|---------|-------------|
| **Five Precision Tiers** | ULTRA (10ms), FAST (50ms), HIGH (100ms), STANDARD (500ms), ECONOMY (1000ms) |
| **Durable Persistence** | Memory-mapped WAL with ASYNC / BATCH_DEFERRED / DURABLE modes; tier-differentiated write strategy |
| **Virtual Threads Native** | Zero thread-pool tuning; all delivery and scheduling runs on virtual threads |
| **Cohort-Based Wakeup** | CSA-inspired batched wakeup — 5 daemon events replace 100,000 VT sleeps (100% VT reduction) |
| **Batch Delivery** | 2,231 HTTP requests for 100,000 intents — 44.8x reduction via batch aggregation |
| **Arrow Cross-Tier Borrowing** | High-priority tiers borrow idle slots from lower tiers during bursts, with AdapTBF bounds |
| **Resizable Concurrency** | Per-tier `ResizableSemaphore` supports runtime concurrency adjustment without restart |
| **Cold Swap** | Long-delay intents (>1h) evicted from heap after DURABLE WAL persist; auto-reloaded 60s before executeAt; 72.5% memory reduction |
| **Segmented WAL** | Multi-segment WAL files with automatic rotation; auto-truncation after snapshot; 1.25M ops/sec ASYNC throughput |
| **Pluggable Storage** | `IntentStore` interface with in-memory (`ConcurrentIntentStore`) and RocksDB (`RocksDBIntentStore`) backends |
| **IntentObserver SPI** | Lifecycle hooks (onScheduled/onDelivered/onDeadLettered/onExpired/onDeliveryFailed) for service-layer integration |
| **Crash Recovery** | Snapshot + WAL replay pipeline, gzip-compressed binary snapshots every 5 minutes |
| **Observability** | Per-tier latency histograms (P50–P99.9), RTT metrics, Prometheus export, borrow stats |
| **Engine DefaultTier** | `builder.defaultTier(Tier)` sets engine-wide tier; auto-applied to all intents at creation |
| **SLO TierAdvisor** | Maps SLO declarations (maxTardinessMs + Reliability) to cost-optimal tier; 2x safety margin |
| **HealthNarrator** | Narrative health responses — upgrades binary UP/DOWN to "is it on time?" with structured vitals and anomaly detection |
| **TimelineService** | Temporal forecast — upcoming cohort wakes, scheduler activity preview for the next N minutes |
| **WalReplayService** | WAL time-travel debugging — reconstruct intent lifecycle or system state at any point in time |
| **Intent Listing** | `GET /v1/intents` with optional status filter, paginated results |
| **Dead Letter Revival** | Revive dead-lettered intents with optional reschedule time via `POST /v1/intents/{id}/revive` |
| **Error Recovery Advisor** | Actionable recovery hints per error code — tells what went wrong, next steps, and transient wait times |
| **Token Authentication** | Optional `X-LoomQ-Token` header auth with constant-time comparison; health endpoints exempt |
| **LoomQ CLI** | Interactive temporal explorer shell (`loomq-cli`) — schedule, get, list, chronoscope, timeline, dead-letters, revive, health, follow |

---

## Performance Benchmarks

**Test Environment:** JDK 25.0.2, Windows 11, 22 cores, 8GB heap, localhost, Netty mock server (tiered HTTP delays).

### Full Suite (100k intents, 20k per tier)

| Tier | QPS | E2E p50 | E2E p95 | E2E p99 | Sched p50 | Sched p95 | Delivery p50 | Delivery p95 |
|------|-----|---------|---------|---------|-----------|-----------|--------------|--------------|
| ULTRA | 13,633 | 1,058ms | 1,417ms | 1,448ms | 1,055ms | 1,416ms | 3ms | 3ms |
| FAST | 5,475 | 2,106ms | 3,490ms | 3,630ms | 2,092ms | 3,479ms | 13ms | 14ms |
| HIGH | 1,465 | 7,088ms | 12,990ms | 13,519ms | 7,076ms | 12,978ms | 12ms | 13ms |
| STANDARD | 1,131 | 9,064ms | 16,825ms | 17,511ms | 9,052ms | 16,812ms | 12ms | 13ms |
| ECONOMY | 760 | 13,693ms | 25,057ms | 26,068ms | 13,680ms | 25,044ms | 13ms | 14ms |

**System QPS:** 3,504.5 | **Pipeline:** scheduler p50=3,595ms p95=19,987ms | delivery p50=12ms p95=13ms

> E2E latency = executeAt → webhook received. The dominant factor is scheduler precision (executeAt → dequeue), while delivery (dequeue → webhook received) is consistently ~12–14ms across tiers.

### Batch Delivery & Cohort CSA Impact

| Metric | Value |
|--------|-------|
| Total batches (for 100k intents) | 2,231 |
| HTTP request reduction | 44.8x (100,000 → 2,231) |
| Cohort wake events | 5 (platform daemon) |
| Virtual threads saved | 99,995 (100% VT reduction) |
| Intents via cohort | 100,000 (was: 1 VT each) |

> **Key Insight:** CohortManager consolidates 100,000 per-intent VT sleeps into 5 daemon wake events. Combined with batch delivery, the scheduler achieves both scheduling efficiency and HTTP economy.

### Arrow Borrowing Efficiency

| Metric | Value |
|--------|-------|
| Direct acquires | 869,094 |
| Borrowed acquires | 1,067 (0.1%) |
| Blocking fallbacks | 10,389 |

> The low borrow rate in steady-state tests reflects sufficient per-tier capacity under uniform load. Arrow borrowing is designed for burst scenarios — when a tier's slots are saturated, consumers transparently borrow from lower-priority idle tiers with AdapTBF's 50% lend cap preventing starvation.

### Tier-Differentiated WAL

| Tier | WAL Mode | Trade-off |
|------|----------|-----------|
| ULTRA | ASYNC | Lowest latency; crash-lose window < flush interval |
| FAST | ASYNC | Lowest latency; crash-lose window < flush interval |
| HIGH | BATCH_DEFERRED | Balanced: periodic batch fsync (~50ms) |
| STANDARD | DURABLE | Strongest durability; fsync per write |
| ECONOMY | DURABLE | Strongest durability; fsync per write |

### Cold Swap Memory Efficiency

**Test:** 10,000 intents with 2h delay, DURABLE WAL, measured before/after swap-out.

| Metric | Value |
|--------|-------|
| Hot heap per intent | 476 bytes |
| Cold heap per intent | 131 bytes |
| Memory saved | 72.5% (345 bytes/intent) |
| Cold index entry | ~80 bytes (record) + ~50 bytes (map overhead) |

> **Key Insight:** For 1 million long-delay intents, cold swap reduces heap from ~450 MB to ~130 MB. The swap-in daemon reloads intents from WAL 60s before executeAt with sub-millisecond latency since WAL positions are known at swap-out time.

---

## Precision Tiers

| Tier | Window | Concurrency | Batch | Consumers | WAL Mode | Use Case |
|------|--------|-------------|-------|-----------|----------|----------|
| **ULTRA** | 10ms | 200 | 1×5ms | 16 | ASYNC | Heartbeats, sub-50ms deadlines |
| **FAST** | 50ms | 150 | 1×10ms | 12 | ASYNC | Message retry, backoff |
| **HIGH** | 100ms | 50 | 5×50ms | 4 | BATCH_DEFERRED | General purpose |
| **STANDARD** | 500ms | 50 | 20×100ms | 3 | DURABLE | **Recommended**, order timeouts |
| **ECONOMY** | 1000ms | 50 | 25×300ms | 2 | DURABLE | Long-delay intents, bulk scheduling |

**Selection Guide:**
- Sub-50ms deadlines: **ULTRA** or **FAST**
- Order timeouts and scheduled notifications: **STANDARD** (best balance of throughput and latency)
- Massive batch scheduling (>1h delay): **ECONOMY** (highest resource efficiency)

---

## Embedded Usage (No HTTP Required)

```java
import com.loomq.LoomqEngine;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;

LoomqEngine engine = LoomqEngine.builder()
    .walDir(Path.of("./data"))
    .defaultTier(PrecisionTier.FAST)
    .intentStore(new RocksDBIntentStore(Path.of("./db")))  // optional: pluggable storage
    .deliveryHandler(intent -> {
        System.out.println("Intent fired: " + intent.getIntentId());
        return DeliveryResult.SUCCESS;
    })
    .build();

engine.registerObserver(new IntentObserver() {
    @Override public void onDelivered(Intent i, DeliveryResult r) {
        System.out.println("Delivered: " + i.getIntentId());
    }
    // ... other callbacks
});

engine.start();

// Schedule an intent 5 seconds from now
Intent intent = new Intent();
intent.setExecuteAt(Instant.now().plusSeconds(5));
intent.setPrecisionTier(PrecisionTier.STANDARD);
engine.createIntent(intent, AckMode.ASYNC);

// ... when done:
engine.close();
```

Use cases for embedded mode: single-node apps, integration tests, resource-constrained environments, custom shells on top of LoomQ core.

---

## Architecture

```
loomq-cli (interactive temporal explorer shell)
    └── HTTP client → loomq-server REST API

loomq-server (Netty HTTP + JSON + webhook delivery + security)
    ├── IntentHandler        — REST API routing (RadixTree) + error recovery advisor
    ├── NettyHttpServer      — epoll + pooled allocator + semaphore backpressure
    ├── HttpDeliveryHandler  — Reactor Netty HTTP client
    └── SecurityConfig       — token-based authentication

loomq-core (embeddable kernel, zero HTTP/JSON deps)
    ├── LoomqEngine           — builder-pattern entry point
    ├── PrecisionScheduler    — time-wheel buckets, per-tier scan + batch consumers
    │   ├── CohortManager     — CSA-style batched wakeup (replaces per-intent VT sleep)
    │   ├── BucketGroupManager — per-tier time-bucket storage
    │   ├── ResizableSemaphore — runtime-adjustable concurrency (extends Semaphore)
    │   ├── ChronoscopeSnapshot — scheduler internal state X-ray for diagnostics
    │   └── TierAdvisor       — SLO → tier recommendation with safety margin
    ├── ColdIntentSwapper     — long-delay intent memory swap-out/in; 72.5% heap reduction
    ├── HealthNarrator        — narrative health with vitals and anomaly detection
    ├── TimelineService       — temporal forecast (upcoming wakes, scheduler preview)
    ├── WalReplayService      — WAL time-travel debugging (intent lifecycle reconstruction)
    ├── IntentStore (interface) — pluggable storage (ConcurrentIntentStore / RocksDBIntentStore)
    ├── SimpleWalWriter       — segmented WAL with FFM API, auto-truncation
    ├── RecoveryPipeline      — snapshot + WAL replay on restart
    └── SPI interfaces        — DeliveryHandler, CallbackHandler, IntentObserver, WalAccessor, RedeliveryDecider
```

### Scheduler Design

**Batch Consumers (fire-and-forget):** Each tier runs N virtual-thread consumers. A consumer acquires a permit (own tier or borrowed), polls the dispatch queue, and calls `deliveryHandler.deliverAsync()`. The permit is released in the async callback — consumer threads never block on HTTP.

**Cohort Wakeup (CSA-inspired):** Intents with delay > precision window are grouped by cohort key. A single daemon thread wakes each cohort at its due time, flushing all intents into the bucket. This replaces per-intent virtual-thread sleep — one thread handles thousands of intents.

**Arrow Borrowing (AdapTBF-bounded):** When a tier's own semaphore is exhausted, consumers attempt `tryAcquire(100ms)` on each lower-priority tier. Up to 50% of a tier's slots can be lent. Borrowed permits are returned on the lender's semaphore, with `borrowedCount` tracking.

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/intents` | Create an intent |
| GET | `/v1/intents` | List intents (optional `?status=` filter) |
| GET | `/v1/intents/{id}` | Get intent by ID |
| PATCH | `/v1/intents/{id}` | Update intent fields |
| POST | `/v1/intents/{id}/cancel` | Cancel an intent |
| POST | `/v1/intents/{id}/fire-now` | Trigger immediately |
| POST | `/v1/intents/{id}/revive` | Revive dead-lettered intent |
| GET | `/v1/admin/chronoscope` | Scheduler internal state snapshot |
| GET | `/v1/admin/timeline` | Temporal forecast (cohort wakes, activity preview) |
| GET | `/v1/admin/dead-letters` | List dead-lettered intents |
| GET | `/health` | Health check with narrative vitals |
| GET | `/health/live` | Liveness probe |
| GET | `/health/ready` | Readiness probe (WAL + Raft health) |
| GET | `/health/deep` | Deep health with tier backpressure data |
| GET | `/metrics` | JSON metrics snapshot |

### Create Intent Request

```json
{
  "executeAt": "2026-05-01T10:30:00Z",
  "deadline": "2026-05-01T11:00:00Z",
  "precisionTier": "STANDARD",
  "ackLevel": "DURABLE",
  "idempotencyKey": "req-abc-123",
  "callback": {
    "url": "https://example.com/webhook",
    "method": "POST",
    "headers": {"X-Request-Id": "123"}
  },
  "redelivery": {
    "maxAttempts": 5,
    "backoff": "exponential",
    "initialDelayMs": 1000,
    "maxDelayMs": 60000
  },
  "tags": {"tenant": "demo"}
}
```

**Required:** `executeAt` | **Optional:** `intentId`, `deadline`, `expiredAction`, `precisionTier`, `shardKey`, `ackLevel`, `callback`, `redelivery`, `idempotencyKey`, `tags`

### Error Responses

| Code | Error Code | When |
|------|------------|------|
| **400** | `40001`–`40004` | Bad request (missing params, invalid revision header) |
| **404** | `40401` | Intent not found |
| **409** | `40901` | Idempotency key conflict (duplicate with terminal state) |
| **409** | `40902` | Revision conflict — optimistic concurrency failure, retry with fresh revision |
| **422** | `42201`–`42207` | Validation error (invalid time, unmodifiable state, missing fields, etc.) |
| **429** | `42900` | Backpressure — retry after `retryAfterMs` from response body |
| **503** | `50301` | Raft: follower read rejected — redirect to leader (retryable) |
| **503** | `50302` | Raft: follower write rejected — redirect to leader (retryable) |
| **503** | `50303` | Raft: leader write backpressure — retry with delay (retryable) |
| **500** | `50001`–`50003` | Internal server error (cancel/fire-now/revive failures) |

All error responses include a structured `ErrorResponse` body with `errorCode`, `message`, `retryable` flag, and optional `recoveryHint` providing actionable next steps.

---

## SPI Extension Points

| Interface | Method | Purpose |
|-----------|--------|---------|
| `DeliveryHandler` | `deliverAsync(Intent)` → `CompletableFuture<DeliveryResult>` | How intents are dispatched (HTTP, MQ, local) |
| `IntentObserver` | `onScheduled/onDelivered/onDeadLettered/onExpired/onDeliveryFailed` | Lifecycle event observation for service layers |
| `WalAccessor` | `readRecord/listSegments/truncateBefore` | WAL read access for recovery and Raft snapshot boundary management |
| `CallbackHandler` | `onIntentEvent(Intent, EventType, Throwable)` | Lifecycle event notification |
| `RedeliveryDecider` | `shouldRedeliver(DeliveryContext)` → `boolean` | Custom retry policy |

`DeliveryResult` enum: `SUCCESS`, `RETRY`, `DEAD_LETTER`, `EXPIRED`

---

## Configuration

Configuration is loaded from (highest to lowest priority):
1. JVM system properties (`-Dloomq.xxx`)
2. External `./config/application.yml`
3. Classpath `application.yml`
4. `@DefaultValue` annotations

Key config groups: `server.*`, `netty.*`, `wal.*`, `scheduler.*`, `dispatcher.*`, `retry.*`, `recovery.*`

See [Configuration Reference](docs/operations/CONFIGURATION.md) for the complete key list.

---

## Deployment

### Diagnostics Tools

- **LoomQ CLI** — interactive terminal for scheduling, querying, and troubleshooting
- **`/v1/admin/chronoscope`** — scheduler X-ray snapshot (per-tier state, semaphore, borrow stats)
- **`/v1/admin/timeline`** — temporal forecast (cohort wakes, scheduler activity preview)
- **`/health/deep`** — deep health with tier backpressure data
- **`/v1/admin/dead-letters`** — dead letter queue inspection and triage

### Docker

```bash
# Build and run single node
make docker-build && make docker-run

# Single-node + monitoring stack (Prometheus + Grafana)
docker-compose --profile full up -d
```

| Service | Port | Dashboard |
|---------|------|-----------|
| LoomQ server | 7928 | REST API + `/metrics` |
| Prometheus | 9090 | Metrics collection (30d retention) |
| Grafana | 3000 | Pre-built dashboards (admin / loomq123) |

### Makefile Shortcuts

```bash
make build          # Full build with tests
make build-fast     # Build without tests
make format         # Apply Spotless formatting
make check-format   # Verify Spotless formatting
make test           # Run default test suite
make run            # Build and start server
make docker-build   # Build Docker image
```

---

## Roadmap

### v0.9.2 (current)
- [x] Cohort-based batched wakeup (CSA-inspired)
- [x] Arrow cross-tier slot borrowing + AdapTBF constraints
- [x] ResizableSemaphore (runtime concurrency adjustment)
- [x] Cold swap: long-delay intent memory optimization (72.5% heap reduction)
- [x] Segmented WAL with auto-truncation
- [x] Pluggable storage engine: `IntentStore` interface + `ConcurrentIntentStore` + `RocksDBIntentStore`
- [x] `IntentObserver` SPI for lifecycle hooks
- [x] `WalAccessor` SPI for WAL read/truncation
- [x] Raft leader election, log replication, snapshot catch-up, leader-authoritative reads/writes
- [x] Raft write coordination: bounded backpressure, request deduplication, optimistic concurrency
- [x] Observable backpressure (no silent drops)
- [x] Expiry index: O(log n) expired intent scanning
- [x] LoomQ CLI: interactive temporal explorer shell with 10+ commands
- [x] HealthNarrator: narrative health with vitals and anomaly detection
- [x] ErrorRecoveryAdvisor: actionable recovery hints per error code
- [x] TimelineService: temporal forecast and cohort wake preview
- [x] TierAdvisor: SLO → tier recommendation with safety margin
- [x] WalReplayService: WAL time-travel debugging
- [x] Dead letter revival: revive dead-lettered intents with optional reschedule
- [x] Intent listing: `GET /v1/intents` with status filter
- [x] Token authentication: `X-LoomQ-Token` header with constant-time comparison
- [x] ChronoscopeSnapshot: scheduler internal state X-ray for diagnostics
- [x] Benchmark suite: WAL throughput, storage comparison, observer overhead

### v0.9.2 (next)
- [ ] Dynamic Raft membership management
- [ ] Kubernetes operator
- [ ] Web-based management console
- [ ] gRPC delivery handler

---

## Development

- [Release Checklist](docs/engineering/release-checklist.md)
- [Benchmark Guide](benchmark/README.md)
- [Configuration Reference](docs/operations/CONFIGURATION.md)
- [Architecture Details](docs/development/ARCHITECTURE.md)
- [Core Model](docs/architecture/core-model.md)

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## License

```
Copyright 2026 LoomQ Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
