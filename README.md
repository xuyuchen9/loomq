# LoomQ - Durable Time Kernel for Future Events

[![JDK](https://img.shields.io/badge/JDK-25%2B-green.svg)](https://openjdk.org/)
[![Maven Central](https://img.shields.io/badge/Maven%20Central-0.7.x-blue.svg)](https://central.sonatype.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Tests](https://img.shields.io/badge/Tests-367%20passed-brightgreen.svg)]()

**Making future events happen reliably, powered by Java 25 Virtual Threads.**

LoomQ is a durable time kernel for distributed systems. It focuses on scheduling, rescheduling, expiry, recovery, retry orchestration, and deadline handling.

> **Note:** `Intent` is the public model. Some historical docs still use `task` terminology, but new material should prefer `Intent`.

## What LoomQ Is / Isn't

**LoomQ is:**

- a durable scheduling kernel for future events
- a recovery-friendly time layer for embedded or standalone use
- a shell-friendly core with SPI hooks for delivery and callbacks

**LoomQ is not:**

- a general-purpose message queue
- a workflow engine
- a lock or lease product baked into the core

## Capability Maturity

| Category | Examples |
|----------|----------|
| **Stable** | durable delayed execution, persistence + recovery, scheduling, retry orchestration, metrics baseline |
| **Beta** | cluster plumbing, storage plugin surface, replication-related experiments |
| **Not yet committed** | distributed coordination primitives, lock / lease semantics, leader election |

---

## Core Features

| Feature | Description |
|---------|-------------|
| **Five Precision Tiers** | ULTRA (10ms), FAST (50ms), HIGH (100ms), STANDARD (500ms), ECONOMY (1000ms) — choose the right precision for your use case |
| **Durable Persistence** | ASYNC mode for <100ms RPO, DURABLE mode for zero data loss |
| **Virtual Threads Native** | Zero thread pool tuning, handle millions of concurrent intents effortlessly |
| **O(1) Expiration** | Time-wheel bucket expiration with constant time complexity |
| **Netty HTTP Layer** | High-performance HTTP server with memory-mapped zero-copy WAL |
| **Observability** | Prometheus metrics + HdrHistogram high-precision latency statistics |

---

## Performance Benchmarks

**Test Environment:** JDK 25, NVMe SSD, 16 cores, localhost

### Write Throughput

| Mode | QPS | Description |
|------|-----|-------------|
| ASYNC | **424,077** | Returns immediately after publish, RPO < 100ms |
| DURABLE | **>150,000** | Returns after WAL fsync, RPO = 0 |

### End-to-End Trigger Performance (50 concurrent threads)

| Tier | Window | QPS | Efficiency | P99 Latency | Use Case |
|------|--------|-----|------------|-------------|----------|
| ULTRA | 10ms | 45,949 | 2.5% | ≤15ms | High-frequency heartbeats / short deadlines |
| FAST | 50ms | 44,718 | 5% | ≤60ms | Message retry, backoff |
| HIGH | 100ms | 40,710 | 10% | ≤120ms | Default tier |
| STANDARD | 500ms | 39,974 | 25% | ≤550ms | **Recommended**, order timeout |
| ECONOMY | 1000ms | 42,295 | 25% | ≤1100ms | Massive long-delay intents |

**Key Insight:** ECONOMY tier achieves 10x per-thread efficiency compared to ULTRA, making it ideal for massive long-delay intents.

---

## Quick Start

### Prerequisites

- JDK 25+ (no `--enable-preview` required)
- Maven 3.9+

### Maven Dependency

```xml
<dependency>
    <groupId>com.loomq</groupId>
    <artifactId>loomq-core</artifactId>
    <version>0.7.0-SNAPSHOT</version>
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
java -jar loomq-server/target/loomq-server-0.7.0-SNAPSHOT.jar
```

Server starts at `http://localhost:8080` by default.

### Create Your First Intent

```bash
# Create an intent that triggers in 30 seconds
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

## Embedded Usage (No HTTP Required)

LoomQ core can be embedded directly in your Java application without starting an HTTP server:

```java
import com.loomq.embedded.EmbeddedDemo;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;

public class MyApp {
    public static void main(String[] args) {
        EmbeddedDemo loomq = new EmbeddedDemo();
        loomq.start();

        // Create an intent with local callback
        loomq.createIntent(5000, PrecisionTier.STANDARD, intent -> {
            System.out.println("Intent triggered: " + intent.getIntentId());
            // Your business logic here
        });

        // Keep running...
    }
}
```

See `src/test/java/com/loomq/embedded/EmbeddedDemo.java` for the full example.

**Use Cases for Embedded Mode:**
- Single-node applications with delayed event needs
- Integration tests without external dependencies
- Resource-constrained environments (no HTTP server overhead)
- Building custom shells on top of LoomQ core

---

## Precision Tiers

| Tier | Window | Max Delay | Recommended Concurrency | Batch Strategy | Use Case |
|------|--------|-----------|------------------------|----------------|----------|
| ULTRA | 10ms | <1min | 10-50 | Single intent | Heartbeat, short deadline |
| FAST | 50ms | <5min | 50-100 | Small batch | Message retry, exponential backoff |
| HIGH | 100ms | <30min | 100-500 | Medium batch | Default choice, general purpose |
| STANDARD | 500ms | <24h | 500-2000 | Large batch | **Recommended**, order timeout, scheduled notifications |
| ECONOMY | 1000ms | >24h | 2000+ | Massive batch | Long-delay intents, data retention policies |

**Selection Guide:**
- For high-frequency deadlines or heartbeats: **ULTRA** or **FAST**
- For order timeouts: **STANDARD** (best balance)
- For massive long-delay intents: **ECONOMY**

---

## Architecture Highlights

LoomQ is built on first-principles thinking for maximum performance:

### Virtual Thread Sleep Instead of Priority Queue

Traditional schedulers use priority queues (heap) with O(log n) insertion and centralized locks. LoomQ uses virtual threads that sleep until execution time—no locks, no heap operations, just pure OS-level scheduling.

```
Traditional:  insert(heap) → O(log n) + lock contention
LoomQ:        virtualThread.sleep(until) → O(1), no locks
```

### MemorySegment + StripedCondition

Zero object allocation for waiting. Uses `MemorySegment` for direct memory access and `StripedCondition` for striped condition variables—eliminating the "thundering herd" problem on wake-up.

### Batch Delivery & Resource Isolation

Low-precision tiers (STANDARD, ECONOMY) benefit from automatic batch collection and delivery, achieving 10x per-thread efficiency compared to high-precision tiers.

---

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/v1/intents` | Create an intent |
| GET | `/v1/intents/{id}` | Get intent status |
| PATCH | `/v1/intents/{id}` | Update intent |
| POST | `/v1/intents/{id}/cancel` | Cancel intent |
| POST | `/v1/intents/{id}/fire-now` | Trigger immediately |
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus metrics |

### Request Body

```json
{
  "executeAt": "2024-01-15T10:30:00Z",
  "deadline": "2024-01-15T11:00:00Z",
  "precisionTier": "STANDARD",
  "ackLevel": "DURABLE",
  "callback": {
    "url": "http://your-server/callback",
    "method": "POST",
    "headers": {"X-Request-Id": "123"},
    "body": "{\"event\": \"timeout\"}"
  }
}
```

---

## v0.6.x Standalone Performance Summary

**v0.6.x was the last single-module standalone milestone before the v0.7.x split.** It marked the end of the single-node era, after which the project moved to an embeddable core plus a standalone Netty service.

### Performance Achievements

| Metric | Value | Notes |
|--------|-------|-------|
| **ASYNC QPS** | 424,077 | Network + memory bandwidth bound |
| **DURABLE QPS** | >150,000 | NVMe SSD fsync bound |
| **Max Concurrent Intents** | 10M+ | 8GB heap memory |
| **P99 Latency (ULTRA)** | ≤15ms | 10ms precision window |
| **P99 Latency (STANDARD)** | ≤550ms | 500ms precision window, **recommended** |

### Physical Limits Reached

1. **Disk I/O**: DURABLE mode throughput is bounded by NVMe SSD random write IOPS (~500K). With group commit batching (20-50 records per fsync), theoretical max is 25K fsync/s → 500K-1.25M records/s. Achieved: >150K QPS including business logic overhead.

2. **Network Bandwidth**: ASYNC mode is network and memory bandwidth bound. Netty + Virtual Threads achieve 424K QPS on JDK 25. Further optimization would require bypassing TCP stack or RDMA (out of scope for standalone).

3. **CPU/Serialization**: JSON serialization overhead reduced to ~20ns per record (JDK 25 optimizations). No longer a bottleneck.

4. **Scheduling**: Time-wheel bucketing achieves O(1) expiration check. Virtual thread sleep eliminates centralized scheduling overhead.

### Resource Efficiency by Tier

| Tier | Efficiency | Relative to ULTRA | Use Case |
|------|-----------|-------------------|----------|
| ULTRA | 2.5% | 1x | Distributed locks, heartbeats |
| FAST | 5% | 2x | Message retry, backoff |
| HIGH | 10% | 4x | Default precision |
| **STANDARD** | **15%** | **6x** | **Recommended for production** |
| ECONOMY | 25% | 10x | Massive long-delay intents |

**Key Insight**: ECONOMY tier achieves 10x the resource efficiency of ULTRA. For intents with delays >1 hour, ECONOMY is strongly recommended.

---

## Known Limitations

LoomQ v0.6.x was the **last single-module standalone version**. The following limitations are historical notes that informed the v0.7.0 split.

| Limitation | Description | Status |
|------------|-------------|--------|
| **REPLICATED ACK Not Implemented** | Distributed replication requires consensus protocol (Raft). REPLICATED mode currently falls back to DURABLE. | Planned for v0.7.0 |
| **WAL in JSON Format** | WAL uses human-readable JSON instead of binary. Binary encoding would yield marginal gains (327ns/record already achieved). | Intentional trade-off |
| **Memory-Bound Capacity** | IntentStore is in-memory only. ~10M intents require ~8GB heap. Plugin storage engine planned for v0.8.0. | Documented constraint |
| **No Grafana Dashboard** | Observability limited to Prometheus metrics endpoint. Grafana templates are community-contributable. | Non-blocking |
| **Single-Node Only** | No clustering, failover, or partition tolerance in v0.6.x. Multi-node support is the primary v0.7.0 goal. | Architectural boundary |

### v0.6.x Completion Criteria

The following checklist defines v0.6.x completion. All items are now **✓ Done**:

- [x] Confirm core modules have no HTTP/JSON dependencies
  - `IntentStore` ✓ Pure Java collections
  - `IntentWal` ✓ Binary codec, no Jackson
  - `BucketGroupManager` ✓ Pure Java
  - `PrecisionTier` ⚠️ Has Jackson annotations (to be moved in v0.7.0)
- [x] Create embedded demo (`EmbeddedDemo.java`)
- [x] Document v0.6.x as the last single-module standalone version
- [x] Tag `v0.6.3-final` and create `release/v0.6.x` branch

**Next Phase (v0.7.0)**: Split into `loomq-core` (embeddable, HTTP-free) + `loomq-server` (Netty HTTP layer).

---

## Roadmap

### v0.7.0
- Split into `loomq-core` (embeddable) + `loomq-server` (standalone)
- Plugin-based storage engine (RocksDB, LevelDB support)
- REST API v2 with OpenAPI spec

### v0.8.0
- **Loomqex**: A future shell built on top of LoomQ for lease / lock semantics, once the kernel boundary is fully validated
- Multi-node clustering with Raft consensus
- Web-based management console

### Future
- Cloud-native deployment (Kubernetes operator)
- Multi-region replication
- Schema registry for callback payloads

---

## Development & Release

The current engineering baseline lives in the docs:

- [Release checklist](docs/engineering/release-checklist.md)
- [Benchmark checklist](docs/engineering/benchmark-checklist.md)
- [Configuration reference](docs/operations/CONFIGURATION.md)
- [Core model](docs/architecture/core-model.md)

These documents are the source of truth for how we describe the current kernel and how we publish it.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

- **Issues**: Report bugs or request features at [GitHub Issues](https://github.com/loomq/loomq/issues)
- **Pull Requests**: Fork, branch, and submit PRs against `main`

---

## License

```
Copyright 2024 LoomQ Authors

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
