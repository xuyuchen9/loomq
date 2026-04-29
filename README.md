# LoomQ — Durable Time Kernel for Future Events

[![JDK](https://img.shields.io/badge/JDK-25%2B-green.svg)](https://openjdk.org/)
[![Maven Central](https://img.shields.io/badge/Maven%20Central-0.8.x-blue.svg)](https://central.sonatype.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Tests](https://img.shields.io/badge/Tests-471%20passed-brightgreen.svg)]()

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
| **Stable** | durable delayed execution, persistence + recovery, precision-tier scheduling, retry orchestration, metrics |
| **Beta** | cluster plumbing, replication, shard routing, failover |
| **Not yet committed** | distributed coordination primitives, lock/lease semantics, leader election |

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
    <version>0.8.0-SNAPSHOT</version>
</dependency>
```

### Build from Source

```bash
git clone https://github.com/loomq/loomq.git
cd loomq
mvn clean package -DskipTests
```

### Start Server

```bash
java -jar loomq-server/target/loomq-server-0.8.0-SNAPSHOT.jar
```

Server listens on `http://localhost:8080` by default.

### Create Your First Intent

```bash
curl -X POST http://localhost:8080/v1/intents \
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
curl http://localhost:8080/v1/intents/{intentId}
```

---

## Core Features

| Feature | Description |
|---------|-------------|
| **Five Precision Tiers** | ULTRA (10ms), FAST (50ms), HIGH (100ms), STANDARD (500ms), ECONOMY (1000ms) |
| **Durable Persistence** | Memory-mapped WAL with ASYNC/DURABLE/REPLICATED ack levels, CRC32 checksums |
| **Virtual Threads Native** | Zero thread-pool tuning; all delivery and scheduling runs on virtual threads |
| **Cohort-Based Wakeup** | CSA-inspired batched wakeup replaces per-intent VT sleep for efficient long-delay scheduling |
| **Arrow Cross-Tier Borrowing** | High-priority tiers borrow idle slots from lower tiers during bursts, with AdapTBF bounds |
| **Resizable Concurrency** | Per-tier `ResizableSemaphore` supports runtime concurrency adjustment without restart |
| **Crash Recovery** | Snapshot + WAL replay pipeline, gzip-compressed binary snapshots every 5 minutes |
| **Observability** | Per-tier latency histograms (P50–P99.9), RTT metrics, Prometheus export, borrow stats |

---

## What's New in v0.8.0

v0.8.0 builds on the v0.7.0 modular split to make concurrency control adaptive and observable.

| Component | What It Does |
|-----------|-------------|
| **CohortManager** | Batches intents with similar executeAt times — one daemon thread wakes thousands of intents, replacing per-intent virtual-thread sleep |
| **ResizableSemaphore** | `extends Semaphore` for zero-overhead acquire/tryAcquire; supports runtime `resize()` for gradual permit adjustment |
| **Arrow Borrowing** | When a tier's slots are full, consumers borrow from lower-priority idle tiers (100ms timeout) |
| **AdapTBF Constraints** | Bounds Arrow borrowing: each tier lends at most 50% of its slots, preventing starvation |
| **RTT Metrics** | Per-tier dequeue→webhook-received latency (p50/p95/p99), independent of scheduling precision |
| **BorrowStats** | `own_direct`, `own_blocking`, `borrowed`, `borrow_timeouts`, `borrow_rate` — full borrowing visibility |

---

## Precision Tiers

| Tier | Window | Concurrency | Batch | Consumers | WAL Mode | Use Case |
|------|--------|-------------|-------|-----------|----------|----------|
| **ULTRA** | 10ms | 200 | 1×5ms | 16 | ASYNC | Heartbeats, sub-50ms deadlines |
| **FAST** | 50ms | 150 | 1×10ms | 12 | ASYNC | Message retry, backoff |
| **HIGH** | 100ms | 50 | 5×50ms | 4 | BATCH | General purpose |
| **STANDARD** | 500ms | 50 | 20×100ms | 3 | DURABLE | **Recommended**, order timeouts |
| **ECONOMY** | 1000ms | 50 | 25×300ms | 2 | DURABLE | Long-delay intents, bulk scheduling |

**Selection Guide:**
- Sub-50ms deadlines: **ULTRA** or **FAST**
- Order timeouts and scheduled notifications: **STANDARD** (best balance of throughput and latency)
- Massive batch scheduling (>1h delay): **ECONOMY** (highest resource efficiency)

---

## Performance Benchmarks

**Test Environment:** JDK 25, NVMe SSD, 16 cores, localhost, Netty mock server

### Full Benchmark (100k intents, 20k per tier)

| Tier | E2E p50 | E2E p95 | E2E p99 | RTT p50 | RTT p95 |
|------|---------|---------|---------|---------|---------|
| ULTRA | 1,675ms | 2,967ms | 3,076ms | 1ms | 16ms |
| FAST | 3,606ms | 5,979ms | 6,210ms | 1ms | 16ms |
| HIGH | 11,272ms | 20,055ms | 20,800ms | 1ms | 16ms |
| STANDARD | 18,192ms | 30,894ms | 31,503ms | 1ms | 16ms |
| ECONOMY | 15,470ms | 27,987ms | 29,122ms | 15ms | 17ms |

**System QPS:** 2,476 (100k intents in ~40s)

### Arrow Borrowing Efficiency

| Metric | Value |
|--------|-------|
| Direct acquires | 164,413 |
| Borrowed acquires | 18,190 (9.6%) |
| Blocking fallbacks | 6,768 |
| Borrow timeouts | 11,047 |

> **Key Insight:** Arrow borrowing handles 9.6% of all acquires without blocking. AdapTBF's 50% lend cap prevents high-priority tiers from starving ECONOMY while still enabling significant burst absorption.

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
    .deliveryHandler(intent -> {
        System.out.println("Intent fired: " + intent.getIntentId());
        return DeliveryResult.SUCCESS;
    })
    .build();

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
    │   ├── CohortManager     — CSA-style batched wakeup
    │   ├── BucketGroupManager — per-tier time-bucket storage
    │   └── ResizableSemaphore — runtime-adjustable concurrency (extends Semaphore)
    ├── IntentStore           — in-memory ConcurrentHashMap storage
    ├── SimpleWalWriter       — memory-mapped WAL with FFM API, ~100ns/record
    ├── RecoveryPipeline      — snapshot + WAL replay on restart
    └── SPI interfaces        — DeliveryHandler, CallbackHandler, RedeliveryDecider
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

## Roadmap

### v0.8.0 (current)
- [x] Cohort-based batched wakeup (CSA-inspired)
- [x] Arrow cross-tier slot borrowing
- [x] AdapTBF lending constraints
- [x] ResizableSemaphore (runtime concurrency adjustment)
- [x] RTT per-tier metrics
- [ ] Plugin-based storage engine (RocksDB, LevelDB)

### v0.9.0
- Multi-node clustering with Raft consensus
- Web-based management console
- **Loomqex**: lock/lease semantics built on the stable kernel boundary

### Future
- Kubernetes operator
- Multi-region replication
- Schema registry for callback payloads

---

## Development

- [Release Checklist](docs/engineering/release-checklist.md)
- [Benchmark Checklist](docs/engineering/benchmark-checklist.md)
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
