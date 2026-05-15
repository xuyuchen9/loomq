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
| **Stable** | durable delayed execution, persistence + recovery, precision-tier scheduling, retry orchestration, metrics, pluggable storage (in-memory + RocksDB), WAL segment rotation with snapshot compaction, IntentObserver lifecycle hooks |
| **Beta** | Raft leader election, log replication, snapshot catch-up, leader-authoritative reads, Raft observability |
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
    <version>0.9.1</version>
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
java -jar loomq-server/target/loomq-server-0.9.1.jar
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

- `GET /v1/intents/{intentId}` becomes leader-authoritative
- followers return `503` with error code `50301`, `retryable=true`, and the leader id in `details` when known
- `/health` and `/metrics` expose Raft signals such as role, leader id, term, commit index, commit lag, replication lag, peer reachability, and whether the leader is currently accepting reads / writes
- `/health/deep` adds tier backpressure data on top of the Raft view

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
loomq-server (Netty HTTP + JSON + webhook delivery)
    ├── IntentHandler        — REST API routing (RadixTree)
    ├── NettyHttpServer      — epoll + pooled allocator + semaphore backpressure
    └── HttpDeliveryHandler  — Reactor Netty HTTP client

loomq-core (embeddable kernel, zero HTTP/JSON deps)
    ├── LoomqEngine           — builder-pattern entry point
    ├── PrecisionScheduler    — time-wheel buckets, per-tier scan + batch consumers
    │   ├── CohortManager     — CSA-style batched wakeup (replaces per-intent VT sleep)
    │   ├── BucketGroupManager — per-tier time-bucket storage
    │   └── ResizableSemaphore — runtime-adjustable concurrency (extends Semaphore)
    ├── ColdIntentSwapper     — long-delay intent memory swap-out/in; 72.5% heap reduction
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
| GET | `/v1/intents/{id}` | Get intent by ID |
| PATCH | `/v1/intents/{id}` | Update intent fields |
| POST | `/v1/intents/{id}/cancel` | Cancel an intent |
| POST | `/v1/intents/{id}/fire-now` | Trigger immediately |
| GET | `/health` | Health check |
| GET | `/health/live` | Liveness probe |
| GET | `/health/ready` | Readiness probe (WAL health) |
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

| Code | When |
|------|------|
| **404** | Intent not found |
| **409** | Idempotency key conflict (duplicate with terminal state) |
| **422** | Validation error (invalid time, unmodifiable state, etc.) |
| **429** | Backpressure — retry after `retryAfterMs` from response body |

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

### v0.9.1 (current)
- [x] Cohort-based batched wakeup (CSA-inspired)
- [x] Arrow cross-tier slot borrowing + AdapTBF constraints
- [x] ResizableSemaphore (runtime concurrency adjustment)
- [x] Cold swap: long-delay intent memory optimization (72.5% heap reduction)
- [x] Segmented WAL with auto-truncation
- [x] Pluggable storage engine: `IntentStore` interface + `ConcurrentIntentStore` + `RocksDBIntentStore`
- [x] `IntentObserver` SPI for lifecycle hooks
- [x] `WalAccessor` SPI for WAL read/truncation
- [x] Raft leader election and log replication
- [x] Observable backpressure (no silent drops)
- [x] Expiry index: O(log n) expired intent scanning
- [x] Benchmark suite: WAL throughput, storage comparison, observer overhead

### Future
- Dynamic Raft membership management
- Linearizable leader-read path
- Kubernetes operator
- Web-based management console

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
