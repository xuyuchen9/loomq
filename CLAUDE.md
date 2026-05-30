# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LoomQ is a durable time kernel for distributed systems. It schedules, persists, and delivers future events called **Intent**s. Built on Java 25 Virtual Threads. Core modules: `loomq-bom` (version management), `loomq-core` (embeddable, HTTP-free kernel), `loomq-server` (standalone Netty HTTP server), `loomq-raft` (Raft consensus), `loomq-channel` (pluggable delivery channels: HTTP + gRPC), `loomq-cli` (interactive temporal explorer).

## Build & Run Commands

```bash
# Build
mvn clean package              # full build with tests
mvn clean package -DskipTests  # fast build, skip tests
make build / make build-fast   # Makefile shortcuts

# Formatting (Spotless — enforced in CI; import ordering + unused import removal)
make format                    # apply formatting
make check-format              # verify formatting (same as CI gate)

# Test (Maven Surefire profiles with JUnit 5 tags)
mvn test                       # default: excludes benchmark/slow/integration
mvn test -Pfast-tests          # same as default, used in CI matrix per module
mvn test -Pslow-tests          # @Tag("slow") only
mvn test -Pintegration-tests   # @Tag("integration") only
mvn test -Pfull-tests          # everything including slow/benchmark
mvn test -Dtest=ClassName      # single test class
mvn test -Dtest=ClassName#methodName  # single test method

# Module-scoped test (CI pattern — builds deps with -am)
mvn test -pl loomq-core -am
mvn test -pl loomq-server -am

# Run benchmark suite (Excel + MD reports)
benchmark\benchmark.bat                  # Windows full suite
benchmark\benchmark.bat --quick          # quick validation
benchmark\benchmark.bat --stress         # full + stress sweep
./benchmark/scripts/benchmark.sh         # Linux/macOS

# Pre-push gate (same checks CI runs)
make check                     # check-format + test

# Run
java -jar loomq-server/target/loomq-server-0.9.2.jar
make run-jar                   # Makefile shortcut

# Docker
make docker-build && make docker-run          # single container
make docker-compose-up                        # cluster + monitoring stack
```

### JUnit 5 Tag System

Tests are categorized with `@Tag` annotations. Maven Surefire uses `groups`/`excludedGroups` properties to include/exclude:

| Tag | Maven Profile | What |
|-----|--------------|------|
| *(none)* | default / `fast-tests` | Fast unit tests, always run |
| `slow` | `slow-tests` | PrecisionSchedulerTest, RaftNodeTest, BackPressureTest, SegmentedWalTest |
| `integration` | `integration-tests` | Tests requiring full server startup, HTTP, or multi-node |
| `benchmark` | (included in `full-tests`) | Performance benchmarks with mock server |

## Modules

| Module | Purpose |
|--------|---------|
| `loomq-bom` | Bill of Materials — centralized version management |
| `loomq-core` | Embeddable kernel, zero HTTP/JSON deps |
| `loomq-server` | Standalone Netty HTTP server (`LoomqServerApplication` entry point) |
| `loomq-raft` | Raft consensus (leader election, log replication, snapshot catch-up) |
| `loomq-channel` | Aggregator POM for delivery channels |
| `loomq-channel-http` | HTTP webhook delivery + batch delivery |
| `loomq-channel-grpc` | gRPC streaming delivery (AUTO_ACK / MANUAL_ACK) |
| `loomq-cli` | Interactive temporal explorer shell (10+ commands) |

## Architecture

```
loomq-server (Netty HTTP + JSON + security)
    ├── IntentHandler        — REST API routing (RadixTree) + error recovery advisor
    ├── NettyHttpServer      — epoll + pooled allocator + semaphore backpressure
    ├── SecurityConfig       — token-based authentication
    └── loomq-channel        — pluggable delivery channels
        ├── loomq-channel-http  — HTTP webhook delivery + batch delivery
        └── loomq-channel-grpc  — gRPC streaming delivery (AUTO_ACK / MANUAL_ACK)

loomq-core (embeddable kernel, zero HTTP/JSON deps)
    ├── LoomqEngine           — builder-pattern entry point
    ├── PrecisionScheduler    — time-wheel buckets, per-tier scan + batch consumers
    │   ├── CohortManager     — CSA-style batched wakeup (replaces per-intent VT sleep)
    │   ├── BucketGroupManager — per-tier time-bucket storage
    │   └── ResizableSemaphore — extends Semaphore, runtime-resizable permits
    ├── IntentStore           — pluggable storage (ConcurrentIntentStore / RocksDBIntentStore)
    ├── SimpleWalWriter       — memory-mapped WAL with FFM API (~100ns/record)
    ├── RecoveryPipeline      — snapshot + WAL replay on restart
    └── SPI interfaces        — DeliveryHandler, CallbackHandler, IntentObserver, WalAccessor, RedeliveryDecider
```

**Intent lifecycle:** CREATED → SCHEDULED → DUE → DISPATCHING → DELIVERED → ACKED (branches: CANCELLED, EXPIRED, DEAD_LETTERED)

**Five precision tiers:** ULTRA(10ms, 200 slots), FAST(50ms, 150 slots), HIGH(100ms, 50 slots), STANDARD(500ms, 50 slots), ECONOMY(1000ms, 50 slots).

## Key Design Decisions

- **"Intent" is the public model** — older docs/code may use legacy terminology; always use "Intent" in new code.
- **Core has zero HTTP/JSON dependencies** — `loomq-core` depends only on `slf4j-api` at compile scope. All transport, serialization, and config-parsing concerns live in `loomq-server`.
- **DeliveryHandler SPI** — the scheduler in core delegates delivery through this interface; `loomq-channel-http` provides `NettyHttpDeliveryHandler` and `BatchedHttpDeliveryHandler`, `loomq-channel-grpc` provides `GrpcStreamDeliveryHandler`. Embedders supply their own.
- **Virtual threads everywhere** — `Executors.newVirtualThreadPerTaskExecutor()` for batch consumers; no traditional thread pool tuning.
- **Cohort-based wakeup (CSA-inspired)** — intents with delay > precision window are grouped by cohort key; one daemon thread wakes thousands, replacing per-intent VT sleep.
- **Arrow cross-tier borrowing** — when a tier's semaphore is full, consumers borrow slots from lower-priority tiers via `tryAcquire(100ms)`. AdapTBF bounds lending to 50% of a tier's slots to prevent starvation.
- **ResizableSemaphore extends Semaphore** — zero-overhead acquire/tryAcquire (inherited); only release() is overridden for gradual shrink via permit discarding. Tracks `borrowedCount` per tier.
- **IntentStore is pluggable** — `ConcurrentIntentStore` handles in-memory mode, `RocksDBIntentStore` handles durable local storage; use `upsert()` for current-state writes.
- **Cluster/replication is Beta** — `ClusterManager`, `ShardRouter`, `FailoverController`, and Raft consensus are implemented, but we should still treat the path as beta-hardening and protect the snapshot/failover regressions.

## REST API

`POST /v1/intents`, `GET /v1/intents/{id}`, `PATCH /v1/intents/{id}`, `POST /v1/intents/{id}/cancel`, `POST /v1/intents/{id}/fire-now`, `GET /health`, `GET /health/live`, `GET /health/ready`, `GET /metrics`

## Configuration

Priority (highest→lowest): JVM system properties (`-Dloomq.xxx`) → external `./config/application.yml` → classpath `application.yml` → `@DefaultValue` annotations. Config interfaces use `org.aeonbits.owner` in `com.loomq.config`.

## CI

GitHub Actions (`.github/workflows/ci.yml`): Oracle JDK 25. Jobs: `format-check` → `fast-tests` (matrix per module), `slow-tests`, `integration-tests`, `benchmark-quick` (PR only). On push to main: `package` (fat JAR). Scheduled/manual: `full-regression` + `benchmark-full` with regression detection. Use `make check` locally to simulate the CI gate.

## Language

Documentation and configuration guides are written in Chinese (中文). Code comments and commit messages may be in Chinese or English.
