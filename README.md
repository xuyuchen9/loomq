# LoomQ - High-Performance Standalone Delayed Task Scheduler

[![JDK](https://img.shields.io/badge/JDK-25%2B-green.svg)](https://openjdk.org/)
[![Maven Central](https://img.shields.io/badge/Maven%20Central-0.6.x-blue.svg)](https://central.sonatype.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Tests](https://img.shields.io/badge/Tests-367%20passed-brightgreen.svg)]()

**Making future events happen reliably, powered by Java 21+ Virtual Threads.**

LoomQ is a high-performance standalone delayed task scheduling engine. It is not a general-purpose message queue or workflow engine—its core value proposition is ensuring that future events occur reliably at the appointed time.

> **Note:** v0.6.x is the performance milestone for the standalone version. Future releases will focus on distributed deployment and an embeddable core.

---

## Core Features

| Feature | Description |
|---------|-------------|
| **Five Precision Tiers** | ULTRA (10ms), FAST (50ms), HIGH (100ms), STANDARD (500ms), ECONOMY (1000ms) — choose the right precision for your use case |
| **Durable Persistence** | ASYNC mode for <100ms RPO, DURABLE mode for zero data loss |
| **Virtual Threads Native** | Zero thread pool tuning, handle millions of concurrent delayed tasks effortlessly |
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
| ULTRA | 10ms | 45,949 | 2.5% | ≤15ms | Distributed lock renewal |
| FAST | 50ms | 44,718 | 5% | ≤60ms | Message retry, backoff |
| HIGH | 100ms | 40,710 | 10% | ≤120ms | Default tier |
| STANDARD | 500ms | 39,974 | 25% | ≤550ms | **Recommended**, order timeout |
| ECONOMY | 1000ms | 42,295 | 25% | ≤1100ms | Massive long-delay tasks |

**Key Insight:** ECONOMY tier achieves 10x per-thread efficiency compared to ULTRA, making it ideal for massive long-delay tasks.

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
    <version>0.6.2</version>
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
java -jar target/loomq-0.6.2.jar
```

Server starts at `http://localhost:8080` by default.

### Create Your First Delayed Task

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

### Check Task Status

```bash
curl http://localhost:8080/v1/intents/{intentId}
```

---

## Precision Tiers

| Tier | Window | Max Delay | Recommended Concurrency | Batch Strategy | Use Case |
|------|--------|-----------|------------------------|----------------|----------|
| ULTRA | 10ms | <1min | 10-50 | Single task | Distributed lock renewal, heartbeat |
| FAST | 50ms | <5min | 50-100 | Small batch | Message retry, exponential backoff |
| HIGH | 100ms | <30min | 100-500 | Medium batch | Default choice, general purpose |
| STANDARD | 500ms | <24h | 500-2000 | Large batch | **Recommended**, order timeout, scheduled notifications |
| ECONOMY | 1000ms | >24h | 2000+ | Massive batch | Long-delay tasks, data retention policies |

**Selection Guide:**
- For distributed locks: **ULTRA** or **FAST**
- For order timeouts: **STANDARD** (best balance)
- For massive long-delay tasks: **ECONOMY**

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
| POST | `/v1/intents` | Create a delayed task |
| GET | `/v1/intents/{id}` | Get task status |
| PATCH | `/v1/intents/{id}` | Update task |
| POST | `/v1/intents/{id}/cancel` | Cancel task |
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

## Known Limitations

LoomQ v0.6.x is designed for standalone deployment with the following limitations:

| Limitation | Description |
|------------|-------------|
| **No Distributed Replication** | REPLICATED ack level is not yet implemented. For HA, run multiple independent instances behind a load balancer |
| **Memory-Bound Capacity** | Task capacity is limited by heap memory. ~10M tasks require ~8GB heap |
| **No Web UI** | Management is via REST API or CLI only |
| **Single-Node** | No clustering or failover in v0.6.x |

---

## Roadmap

### v0.7.0
- Split into `loomq-core` (embeddable) + `loomq-server` (standalone)
- Plugin-based storage engine (RocksDB, LevelDB support)
- REST API v2 with OpenAPI spec

### v0.8.0
- **Loomqex**: Distributed lock shell built on LoomQ (reference implementation)
- Multi-node clustering with Raft consensus
- Web-based management console

### Future
- Cloud-native deployment (Kubernetes operator)
- Multi-region replication
- Schema registry for callback payloads

---

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
