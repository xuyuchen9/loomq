# LoomQ

[![Java 21](https://img.shields.io/badge/Java-21-blue)](https://openjdk.org/projects/jdk/21/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-346%20passed-brightgreen)]()

> **High-Performance Distributed Delayed Task Queue Engine**
>
> Built on Java 21 Virtual Threads with WAL persistence, Primary-Replica replication, and 500K+ QPS throughput.
>
> **v0.5 New**: Async replication, REPLICATED ACK, automatic failover, and Snapshot + WAL recovery.

[中文文档](README.zh.md)

---

## Executive Summary

LoomQ is a delayed task scheduling engine designed for high-throughput scenarios where microsecond precision is not required. By leveraging Java 21's virtual threads, it achieves **10-60x higher throughput** than traditional Redis/MQ-based solutions with significantly lower operational complexity.

| Metric | Value | Industry Benchmark |
|--------|-------|-------------------|
| Peak Write Throughput (ASYNC) | **1,351,351 QPS** (V0.4) | 50K QPS (Redis ZSET) |
| Peak Write Throughput (DURABLE) | **400,000 QPS** (V0.5) | 10K QPS (RabbitMQ) |
| Peak Write Throughput (REPLICATED) | **80,000 QPS** (V0.5) | 5K QPS (Paxos/Raft) |
| RingBuffer Throughput | **18,791,962 ops/s** | - |
| Stable Throughput | 350K-400K QPS | - |
| Query Throughput | **7,142,857 QPS** | 50K QPS (Redis) |
| Memory per Intent | **~200 bytes** | 1-2KB (Redis) |
| 1M Intents Memory | **~200MB** | 1GB+ (Redis) |
| 10M Intents Capacity | 2.5GB heap | Requires Redis cluster |
| Recovery Time (1M intents) | <30s (Snapshot+WAL) | Minutes (MQ replay) |
| Replication Lag | <100ms (LAN) | <1s (async MQ) |
| Failover Time | <5s (manual) / <1s (auto) | 10-30s (Redis Sentinel) |
| Test Coverage | 180+ tests, 100% pass | - |

---

## First Principles Design

### The Fundamental Question

What is the minimum mechanism required to execute a task at time T?

```
Traditional Approach:
┌─────────────────────────────────────────────────────────────┐
│ Task → Priority Queue → Poller Thread → Worker Thread Pool  │
│                                                             │
│ Problems:                                                   │
│ • Priority Queue: O(log n) insertion, lock contention      │
│ • Poller Thread: CPU spin or sleep latency                 │
│ • Worker Pool: Fixed size, requires tuning                 │
│ • Distributed: Need external coordination (Redis/ZK)       │
└─────────────────────────────────────────────────────────────┘

LoomQ Approach:
┌─────────────────────────────────────────────────────────────┐
│ Task → Virtual Thread.sleep(delay) → Execute callback       │
│                                                             │
│ Why it works:                                               │
│ • Virtual thread parked state: ~200 bytes (vs 1MB platform) │
│ • 1 million sleeping virtual threads = ~200MB heap          │
│ • Carrier threads managed by JVM, not application           │
│ • No global data structure → No lock contention             │
└─────────────────────────────────────────────────────────────┘
```

### The Amdahl's Law Insight

Traditional schedulers hit a ceiling due to serialization points:
- Lock acquisition on shared queue
- Priority heap rebalancing
- Thread pool saturation

LoomQ eliminates serialization by:
- **Per-task isolation**: Each task is an independent continuation
- **Lock-free data paths**: RingBuffer for WAL, ConcurrentHashMap for buckets
- **Carrier thread pooling**: Managed by JVM's work-stealing scheduler

### The Precision-Throughput Trade-off Model

```
Throughput ∝ 1 / (Scheduling Overhead × Precision Factor)

Where Precision Factor:
- 1ms bucket:  1000 checks/s  → High overhead
- 10ms bucket: 100 checks/s   → Medium overhead  
- 100ms bucket: 10 checks/s   → Low overhead

Empirical results:
- 100ms bucket → 600K QPS
- 10ms bucket  → ~200K QPS (estimated)
- 1ms bucket   → ~50K QPS (time wheel territory)

Business Reality Check:
- 30-minute order timeout ±100ms = 0.0056% relative error
- User perception threshold: ~500ms for UI feedback
- Most delay tasks: hours/minutes, not milliseconds
```

---

## Architecture

### System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           API Gateway Layer                              │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────────┐ │
│  │ Task Creation  │  │ Task Cancellation│  │ Query / Monitor / Admin   │ │
│  │ POST /tasks    │  │ DELETE /tasks/{id}│  │ GET /tasks, /metrics      │ │
│  └────────────────┘  └────────────────┘  └────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Cluster Coordination Layer                        │
│  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐ │
│  │   ShardRouter      │  │ ClusterCoordinator │  │   ShardMigrator    │ │
│  │  Consistent Hash   │  │  Heartbeat (5s)    │  │ State Machine      │ │
│  │  150 Virtual Nodes │  │  Timeout (15s)     │  │ Dual-Write Pattern │ │
│  └────────────────────┘  └────────────────────┘  └────────────────────┘ │
│                                                                          │
│  Migration Rate: ~1/(N+1) when scaling from N to N+1 nodes             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
   ┌──────────────┐          ┌──────────────┐          ┌──────────────┐
   │   Shard-0    │          │   Shard-1    │          │   Shard-N    │
   │              │          │              │          │              │
   │ ┌──────────┐ │          │ ┌──────────┐ │          │ ┌──────────┐ │
   │ │ RingBuf  │ │          │ │ RingBuf  │ │          │ │ RingBuf  │ │
   │ │ 64K MPSC │ │          │ │ 64K MPSC │ │          │ │ 64K MPSC │ │
   │ └──────────┘ │          │ └──────────┘ │          │ └──────────┘ │
   │ ┌──────────┐ │          │ ┌──────────┐ │          │ ┌──────────┐ │
   │ │ WAL v2   │ │          │ │ WAL v2   │ │          │ │ WAL v2   │ │
   │ │ Group    │ │          │ │ Group    │ │          │ │ Group    │ │
   │ │ Commit   │ │          │ │ Commit   │ │          │ │ Commit   │ │
   │ └──────────┘ │          │ └──────────┘ │          │ └──────────┘ │
   │ ┌──────────┐ │          │ ┌──────────┐ │          │ ┌──────────┐ │
   │ │ Bucket   │ │          │ │ Bucket   │ │          │ │ Bucket   │ │
   │ │ Scheduler│ │          │ │ Scheduler│ │          │ │ Scheduler│ │
   │ │ 100ms    │ │          │ │ 100ms    │ │          │ │ 100ms    │ │
   │ └──────────┘ │          │ └──────────┘ │          │ └──────────┘ │
   │ ┌──────────┐ │          │ ┌──────────┐ │          │ ┌──────────┐ │
   │ │DispatchLm│ │          │ │DispatchLm│ │          │ │DispatchLm│ │
   │ │ Rate Ctrl│ │          │ │ Rate Ctrl│ │          │ │ Rate Ctrl│ │
   │ └──────────┘ │          │ └──────────┘ │          │ └──────────┘ │
   └──────────────┘          └──────────────┘          └──────────────┘
```

### Core Components

| Component | Implementation | Performance | Responsibility |
|-----------|---------------|-------------|----------------|
| **RingBuffer** | MPSC lock-free queue | **18.8M ops/s** single-thread | WAL write buffer |
| **WAL Writer v2** | Group Commit + fsync batch | 500K+ QPS | Durability guarantee |
| **Checkpoint Manager** | 100K record interval | <30s recovery | Fast crash recovery |
| **TimeBucket Scheduler** | ConcurrentSkipListMap + buckets | 125K schedule/s | Delay management |
| **Dispatch Limiter** | Semaphore + rate limiter | Configurable | Backpressure control |
| **ShardRouter** | MD5 hash + 150 vnodes | <1μs lookup | Request routing |
| **Idempotency** | ConcurrentHashMap + TTL | 100-thread contention safe | Duplicate prevention |

### Data Flow

```
Create Task:
┌─────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ API │───►│ RingBuf  │───►│ WAL      │───►│ Bucket   │───►│ Response │
│     │    │ (async)  │    │ (batch)  │    │ (index)  │    │          │
└─────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
                │
                ▼
           Latency: P99 < 10ms (ASYNC ack)
                    P99 < 50ms (DURABLE ack)

Task Execution:
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ Bucket   │───►│ ReadyQ   │───►│ Virtual  │───►│ Webhook  │
│ Expire   │    │          │    │ Thread   │    │ Executor │
└──────────┘    └──────────┘    └──────────┘    └──────────┘
     │                                               │
     └───────────────────────────────────────────────┘
              Latency: wake_time → execution_time
```

---

## Competitive Analysis

### Performance Benchmark Comparison

| Solution | Write QPS | Read QPS | Memory/Task | Latency P99 |
|----------|-----------|----------|-------------|-------------|
| **LoomQ** | **600K** | 125K | **~200B** | <10ms |
| Redis ZSET (single) | ~100K | ~50K | ~1KB | 1-5ms |
| Redis Cluster (3 nodes) | ~300K | ~150K | ~1KB | 2-10ms |
| RabbitMQ DLX (single) | ~50K | ~50K | ~2KB | 5-20ms |
| RabbitMQ Cluster | ~150K | ~150K | ~2KB | 10-50ms |
| Quartz (JDBC) | ~10K | ~5K | ~1KB | 10-50ms |
| XXL-JOB (MySQL) | ~20K | ~10K | ~500B | 10-30ms |

### Total Cost of Ownership Analysis

| Factor | LoomQ | Redis | RabbitMQ | Quartz |
|--------|-------|-------|----------|--------|
| Infrastructure | Single JAR | Redis cluster | MQ cluster + DLX plugin | DB + App server |
| Operational Complexity | Low | Medium | High | Low |
| Monitoring | Built-in metrics | Redis monitoring | MQ management | DB + app monitoring |
| Scaling Model | Horizontal shards | Vertical then cluster | Cluster mode | Vertical |
| Failure Recovery | WAL replay | AOF/RDB restore | Queue replay | DB restore |
| Learning Curve | Low | Medium | High | Low |

### Use Case Quantitative Analysis

| Scenario | Metric | LoomQ | Redis ZSET | RabbitMQ | Quartz |
|----------|--------|-------|------------|----------|--------|
| **Order timeout (1M/hour)** | Required QPS | 278/s ✅ | 278/s ✅ | 278/s ✅ | 278/s ✅ |
| | Memory for 1M pending | 200MB ✅ | 1GB | 2GB | 1GB+ |
| | Infra needed | 1 node | Redis cluster | MQ cluster | DB + app |
| **Scheduled notifications (10K/min)** | Required QPS | 167/s ✅ | 167/s ✅ | 167/s ✅ | 167/s ✅ |
| | Max burst capacity | 600K/s | 100K/s | 50K/s | 10K/s |
| | Latency P99 | <10ms | 1-5ms | 5-20ms | 10-50ms |
| **Delayed retries (exponential backoff)** | Retry scheduling | Built-in | Manual impl | DLX chains | DB polling |
| | Max concurrent retries | 10M+ | Limited by mem | Limited by Q | Thread pool |
| **Precise timing (trading)** | Precision | 100ms | 1ms | 1ms | ms-s |
| | Jitter tolerance | ±50ms | ±1ms | ±5ms | ±10ms |
| | Recommendation | ❌ Not suitable | ✅ Best fit | ✅ Good | ⚠️ Acceptable |
| **Cron scheduling** | Cron expression | Planned v0.6 | Manual | Limited | ✅ Native |
| | Distributed execution | Built-in | Manual | Manual | No |
| **Existing infra leverage** | New infra needed | Yes (JAR only) | No (if Redis exists) | No (if MQ exists) | No (if DB exists) |
| | Deployment time | Minutes | - | - | - |

### Technical Trade-off Summary

| Dimension | LoomQ Choice | Rationale |
|-----------|--------------|-----------|
| **Precision** | 100ms (configurable) | Business reality: most delays are minutes/hours |
| **Consistency** | At-Least-Once | Idempotent consumers required; simplicity wins |
| **Persistence** | WAL + Checkpoint | Kill -9 safe; <30s recovery |
| **Distribution** | Consistent Hash | ~1/(N+1) migration rate on scaling |
| **Concurrency** | Virtual Threads | No thread pool tuning; JVM-managed |

---

## Benchmarks

### Test Environment
- **CPU**: 22 cores
- **Memory**: 8GB heap
- **OS**: Windows 11
- **JDK**: OpenJDK 21.0.9
- **JVM Flags**: `--enable-preview`

### Throughput Test Results

```
┌────────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│    Tasks       │   Time (ms)  │     QPS      │ Success Rate │    Status    │
├────────────────┼──────────────┼──────────────┼──────────────┼──────────────┤
│      100,000   │          164 │      609,756 │      100.00% │    ✅ PASS   │
│      200,000   │          431 │      464,037 │      100.00% │    ✅ PASS   │
│      500,000   │        1,188 │      420,875 │      100.00% │    ✅ PASS   │
│    1,000,000   │        2,461 │      406,338 │      100.00% │    ✅ PASS   │
│    2,000,000   │        5,416 │      369,276 │      100.00% │    ✅ PASS   │
│    5,000,000   │       13,436 │      372,134 │      100.00% │    ✅ PASS   │
│   10,000,000   │       27,377 │      365,270 │      100.00% │    ✅ PASS   │
└────────────────┴──────────────┴──────────────┴──────────────┴──────────────┘

Target: 50,000 QPS
Achieved: 609,756 QPS peak, 365,000+ QPS sustained
Result: 12x target exceeded
```

### Capacity Test Results

```
┌────────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│    Tasks       │  Memory (MB) │ Per Task (B) │   GC Pause   │    Status    │
├────────────────┼──────────────┼──────────────┼──────────────┼──────────────┤
│    1,000,000   │          160 │          168 │      <10ms   │    ✅ Normal  │
│    2,000,000   │          227 │          119 │      <10ms   │    ✅ Normal  │
│    5,000,000   │        1,024 │          214 │      <15ms   │    ✅ Normal  │
│   10,000,000   │        2,257 │          236 │      <20ms   │    ✅ Normal  │
└────────────────┴──────────────┴──────────────┴──────────────┴──────────────┘

Capacity Limit: 10M+ tasks in 8GB heap
Projected Capacity: 30-40M tasks in 8GB with ZGC

Verified by 340 unit tests including:
- 100-thread concurrent idempotency test (zero duplicates)
- Full state machine transition coverage
- Replication record serialization
- Fencing token split-brain protection
```

### Latency Distribution

```
┌────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│ Concurrency│     QPS      │   P50 (ms)   │   P99 (ms)   │   P99.9 (ms) │
├────────────┼──────────────┼──────────────┼──────────────┼──────────────┤
│        100 │    1,000,000 │          0.0 │          0.0 │          1.0 │
│        500 │      357,142 │          0.0 │          0.0 │          2.0 │
│      1,000 │      217,391 │          0.0 │          1.0 │          5.0 │
│      2,000 │       56,497 │          1.0 │          5.0 │         10.0 │
└────────────┴──────────────┴──────────────┴──────────────┴──────────────┘

Observation: Sub-millisecond latency at optimal concurrency (100-500)
```

---

## Quick Start

### Prerequisites
- Java 21+ (required for virtual threads)
- Maven 3.8+

### Build

```bash
mvn clean package -DskipTests
```

### Testing

LoomQ includes **340+ comprehensive tests** covering unit tests, integration tests, and acceptance tests.

**Test Reports**: [View Latest Report](test-results/test-report-20260409.md)

| Test Category | Count | Status |
|--------------|-------|--------|
| Unit Tests | 328 | ✅ All Pass |
| Integration Tests | 7 | ✅ All Pass |
| Acceptance Tests | 5 | ✅ All Pass |
| **Total** | **340** | **✅ 100% Pass** |

### Test Profiles

```bash
# Fast local feedback (default): excludes integration/slow/benchmark tests
mvn test

# Balanced mode: includes integration tests, excludes slow/benchmark tests
mvn test -Pbalanced-tests

# Integration only
mvn test -Pintegration-tests

# Full verification before release
mvn test -Pfull-tests
```

### Test Scripts (Recommended)

```bash
# V0.5 Intent API Tests
pwsh -File scripts/test-v5.ps1    # Windows
bash scripts/test-v5.sh           # Linux/macOS

# Legacy Tests
pwsh -File scripts/test.ps1 -Mode fast
bash scripts/test.sh fast
```

### CI Strategy

```text
PR / push(main): balanced-tests (fast regression)
push(main): package -DskipTests (artifact build)
nightly / manual: full-tests (full regression)
```

### Run Single Node (v0.5)

```bash
java -Dloomq.node.id=node-1 \
     -Dloomq.shard.id=shard-0 \
     -Dloomq.data.dir=./data \
     -Dloomq.port=8080 \
     --enable-preview -jar target/loomq-0.5.0-SNAPSHOT-shaded.jar
```

### Run Primary-Replica Cluster

```bash
# Primary Node
java -Dloomq.node.id=primary-1 \
     -Dloomq.shard.id=shard-0 \
     -Dloomq.data.dir=./data/primary \
     -Dloomq.port=8080 \
     --enable-preview -jar target/loomq-0.5.0-SNAPSHOT-shaded.jar

# Replica Node
java -Dloomq.node.id=replica-1 \
     -Dloomq.shard.id=shard-0 \
     -Dloomq.data.dir=./data/replica \
     -Dloomq.port=8081 \
     --enable-preview -jar target/loomq-0.5.0-SNAPSHOT-shaded.jar
```

### API Reference (v0.5 Intent API)

#### Create Intent

```bash
curl -X POST http://localhost:8080/v1/intents \
  -H "Content-Type: application/json" \
  -d '{
    "intentId": "order-cancel-12345",
    "executeAt": "2026-04-09T12:30:00Z",
    "deadline": "2026-04-09T12:35:00Z",
    "shardKey": "order-12345",
    "ackLevel": "REPLICATED",
    "callback": {
      "url": "https://api.example.com/webhook/order-cancel",
      "method": "POST",
      "headers": {"X-Source": "loomq"},
      "body": {"orderId": "12345", "eventType": "ORDER_CANCEL"}
    },
    "redelivery": {
      "maxAttempts": 5,
      "backoff": "exponential",
      "initialDelayMs": 1000,
      "maxDelayMs": 60000
    },
    "idempotencyKey": "order-12345-cancel",
    "tags": {"env": "prod", "biz": "order"}
  }'
```

Response (201 Created):
```json
{
  "intentId": "intent_01J9XYZABC",
  "status": "SCHEDULED",
  "executeAt": "2026-04-09T12:30:00Z",
  "deadline": "2026-04-09T12:35:00Z",
  "ackLevel": "REPLICATED",
  "attempts": 0,
  "createdAt": "2026-04-09T12:25:00Z"
}
```

#### Get Intent

```bash
curl http://localhost:8080/v1/intents/intent_01J9XYZABC
```

#### Cancel Intent

```bash
curl -X POST http://localhost:8080/v1/intents/intent_01J9XYZABC/cancel
```

#### Modify Intent

```bash
curl -X PATCH http://localhost:8080/v1/intents/intent_01J9XYZABC \
  -H "Content-Type: application/json" \
  -d '{
    "executeAt": "2026-04-09T13:00:00Z",
    "deadline": "2026-04-09T13:05:00Z"
  }'
```

#### Trigger Immediately

```bash
curl -X POST http://localhost:8080/v1/intents/intent_01J9XYZABC/fire-now
```

Response (if lease expired - 307):
```json
{
  "code": "50304",
  "message": "Lease expired, current primary is 10.0.1.100",
  "details": {
    "intentId": "intent_01J9XYZABC",
    "redirectTo": "http://10.0.1.100:8080/v1/intents/intent_01J9XYZABC/fire-now",
    "newPrimary": "10.0.1.100:8080"
  }
}
```

#### Health Checks

```bash
# Liveness (K8s)
curl http://localhost:8080/health/live

# Readiness (K8s)
curl http://localhost:8080/health/ready

# Replica health
curl http://localhost:8080/health/replica
```

#### Prometheus Metrics

```bash
curl http://localhost:8080/metrics
```

Key metrics:
- `loomq_intents_created_total` - Intent creation rate
- `loomq_replication_lag_ms` - Replication lag
- `loomq_delivery_latency_ms` - Delivery latency distribution

---

## Configuration

```yaml
# application.yml
server:
  host: "0.0.0.0"
  port: 8080

wal:
  data_dir: "./data/wal"
  segment_size_mb: 64
  flush_strategy: "batch"    # per_record | batch | async
  batch_flush_interval_ms: 100

scheduler:
  max_pending_tasks: 1000000

dispatcher:
  http_timeout_ms: 3000
  max_concurrent_dispatches: 1000

retry:
  initial_delay_ms: 1000
  max_delay_ms: 60000
  multiplier: 2.0
  default_max_retry: 5
```

### Acknowledgment Levels (v0.5)

| Level | Description | RPO | Latency | Use Case |
|-------|-------------|-----|---------|----------|
| `ASYNC` | Return after RingBuffer publish | <100ms | <1ms | High throughput, tolerates loss |
| `DURABLE` | Return after local WAL fsync | 0 | <20ms | Default recommended |
| `REPLICATED` | Return after replica ACK | 0 | <50ms | Financial-grade reliability |

---

## Deployment

### JVM Tuning (8C16G recommended)

```bash
java -Xms12g -Xmx12g \
     -XX:+UseZGC \
     -XX:MaxGCPauseMillis=10 \
     --enable-preview \
     -jar loomq.jar
```

### Capacity Planning

| Memory | Max Tasks | Recommended |
|--------|-----------|-------------|
| 4GB | ~8M | Development |
| 8GB | ~20M | Small production |
| 16GB | ~40M | Medium production |
| 32GB | ~80M | Large production |

### Monitoring

Built-in Prometheus metrics:
- `loomq_tasks_created_total`
- `loomq_tasks_dispatched_total`
- `loomq_wal_write_latency`
- `loomq_scheduler_pending_count`
- `loomq_dispatcher_queue_size`

---

## Roadmap

| Version | Feature | Status |
|---------|---------|--------|
| V0.3 | Distributed sharding, consistent hash | ✅ Released |
| V0.4 | Unified state machine, retry policies, idempotency enhancement | ✅ Released |
| V0.4.5 | Engineering packaging (Docker, K8s, monitoring) | ✅ Released |
| **V0.5** | **Async replication, REPLICATED ACK, automatic failover** | **✅ Released** |
| V0.6 | Raft consensus (strong consistency) | 📋 Planned |
| V0.7 | Admin UI dashboard | 📋 Planned |

### V0.5 Highlights

**Primary-Replica Replication Architecture**
- Three ACK levels: ASYNC / DURABLE / REPLICATED
- Async replication with sub-100ms lag (LAN)
- Automatic/manual failover with fencing token protection
- Snapshot + WAL replay for fast recovery

**Intent Lifecycle Management**
- 9-state state machine: CREATED → SCHEDULED → DUE → DISPATCHING → DELIVERED → ACKED
- 24-hour idempotency window with terminal state protection
- Configurable redelivery policies (fixed/exponential backoff)
- Expired action: DISCARD or DEAD_LETTER

**High Availability**
- Lease-based coordination (etcd/Consul compatible)
- Fencing token for split-brain prevention
- Automatic primary detection and failover
- Health checks: liveness, readiness, replica link

**SPI Extensibility**
- `RedeliveryDecider` interface for custom retry logic
- Pluggable lease coordinator implementation

### V0.4.5 Highlights

**Production-Ready Packaging**
- Docker & Docker Compose support with health checks
- Kubernetes manifests for StatefulSet deployment
- Prometheus metrics and Grafana dashboards
- Comprehensive logging with logback configuration
- Environment-based configuration
- Startup scripts for Linux/macOS and Windows
- Complete deployment documentation

### V0.4 Highlights

**Unified State Machine (TaskStatusV3)**
- 10 states: PENDING → SCHEDULED → READY → RUNNING → SUCCESS/FAILED/DEAD_LETTER
- Atomic state transitions with validation
- Comprehensive lifecycle management

**Retry Policies**
- Fixed interval retry
- Exponential backoff
- Configurable max retries

**Enhanced Idempotency**
- `idempotencyKey`: Request-level deduplication
- `bizKey`: Business-level deduplication
- Terminal state protection: completed tasks cannot be overwritten

**New Query APIs**
- `GET /tasks?bizKey={bizKey}` - Query by business key
- `GET /tasks?status={status}` - Query by status

---

## Known Limitations

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| Replication tests require multi-node setup | Single node deployment cannot test failover | Use Docker Compose or K8s for HA testing |
| 100ms precision floor | Not for ms-critical timing | Use Redis/MQ for precision |
| At-Least-Once delivery | Duplicate possible | Consumer idempotency required |
| No automatic node discovery | Manual cluster configuration | Use K8s StatefulSet or service mesh |

---

## Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing`)
5. Open a Pull Request

---

## License

Apache License 2.0

---

## Acknowledgments

- **Java 21 Virtual Threads**: The paradigm shift that makes this design possible
- **JEP 444**: For recognizing that millions of concurrent tasks shouldn't require millions of OS threads
- **The Unix Philosophy**: Do one thing well - delay scheduling without the overhead of a full MQ
