# Changelog

All notable changes to LoomQ are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.9.1] - 2026-05-15

### Added

- **Raft consensus graduated from beta**
  - Leader-only writes and reads with consistent authority checks
  - `RaftWriteCoordinator`: bounded backpressure, request deduplication, optimistic concurrency via revision checking
  - `RaftRuntimeListener`: scheduler pause/resume/rebuild on leader transitions and committed-intent application
  - `RaftStatusProvider` API: exposes role, leader, cluster topology, read-lease status
  - Structured health endpoints: `/health` and `/ready` return Raft status alongside WAL status
  - `RaftWriteBackPressureException`, `RaftWriteConflictException`, `RaftWriteUnavailableException` for fine-grained error handling

- **Peer replication hardening**
  - `appendInFlight` guard prevents duplicate log entries during concurrent AppendEntries
  - `awaitApplied` / `failPendingWaiters` in `LogReplication` for coordinator-proposal lifecycle
  - Peer reachability tracking via `RaftTransport.isPeerConnected()`

- **Security**
  - `SecurityConfig`: optional token authentication via `X-Loomq-Token` header
  - Constant-time token comparison, Bearer prefix stripping, health endpoints exempt
  - Fail-fast startup validation when security is enabled but no tokens configured

- **Test coverage**
  - `RaftWriteAuthorityIntegrationTest`: 3-node cluster write authority and idempotency
  - `RaftServerSmokeTest`: full-stack single-node Raft server smoke test
  - `IntentHandlerRaftReadTest`: leader-authoritative read rejection on followers
  - `SecurityConfigTest`: token authentication validation

### API Changes

- New error codes: `50301` (follower read rejected), `50302` (follower write rejected), `40902` (stale revision conflict)
- `GET /v1/intents/{intentId}` may return 503 with leader redirect in Raft mode
- `GET /health` returns expanded Raft health section in structured JSON
- `GET /health/ready` returns 503 with Raft reason code when not ready
- `/metrics` endpoint now includes Raft-specific counters

### Changed

- RaftNode directly implements `RaftStatusProvider`
- `IntentHandler` routes writes through `RaftWriteCoordinator` when Raft is enabled
- Raft startup validation: peer consistency, port conflicts, endpoint format checks
- `LoomqServerApplication` starts Raft after engine (not before) for correct lifecycle ordering

### Fixed

- Heartbeat task no longer silently cancelled by `ScheduledExecutorService` on unexpected errors
- Peer replication: in-flight AppendEntries tracking prevents concurrent append to same peer
- Clean shutdown: pending waiters are failed and metrics reset when RaftNode closes
- Follower metrics: role, leader id, replication lag, peer counts now reported correctly

## [0.9.0] - 2026-05-13

### Added

- **Raft consensus for the current runtime**
  - Leader election, AppendEntries replication, InstallSnapshot catch-up
  - Peer connection bootstrap and generation-safe response handling
  - Snapshot boundary recovery and follower catch-up regressions

- **Pluggable IntentStore**
  - `ConcurrentIntentStore` for in-memory mode
  - `RocksDBIntentStore` for durable local storage
  - `upsert()`-based current-state writes for recovery, replication, and snapshot apply

- **Snapshot and recovery hardening**
  - WAL snapshot metadata persisted across restart
  - Segment compaction/truncation aligned with Raft log recovery
  - Late-joining followers can catch up through snapshot installation

### Changed

- **Release-facing assets synchronized for GitHub release**
  - Versioned build metadata updated to `0.9.0`
  - Docker, scripts, README, and deployment docs aligned with the current runtime layout
  - Scheduler pending-intent environment variable standardized to `LOOMQ_SCHEDULER_MAX_PENDING_INTENTS`

### Fixed

- Stale leader responses no longer overwrite newer replication progress
- Recovery paths now clear and rebuild store state deterministically

## [0.8.0] - 2026-04-29

### Added

- **CohortManager**: CSA-inspired batched wakeup for long-delay intents
  - Groups intents by cohort key (floor to precision window)
  - Single daemon thread (`cohort-waker`) wakes thousands of intents
  - Replaces per-intent virtual-thread sleep for delays > precision window
  - Tracks `totalRegistered`, `totalFlushed`, `wakeEventCount`

- **Arrow Cross-Tier Slot Borrowing**: Burst absorption across precision tiers
  - `acquireWithBorrow()` in PrecisionScheduler: try own tier → borrow from lower tiers → block
  - 100ms timeout on borrowed `tryAcquire`
  - `BorrowStats`: `own_direct`, `own_blocking`, `borrowed`, `borrow_timeouts`, `borrow_rate`

- **AdapTBF Lending Constraints**: Bounded cross-tier borrowing
  - `MAX_LEND_RATIO = 0.5`: each tier lends at most 50% of its slots
  - `ResizableSemaphore.borrowedCount` tracks active lends per tier
  - Prevents high-priority tiers from starving low-priority tiers

- **ResizableSemaphore**: Runtime-resizable concurrency control
  - `extends Semaphore` for zero-overhead acquire/tryAcquire (inherited from AQS)
  - Only `release()` overridden: fast path checks `currentMax <= targetMax`
  - Increase: `resize()` calls `super.release(delta)` — immediate
  - Decrease: sets `targetMax`, then `release()` silently discards excess permits
  - No background threads, no contention with normal acquire

- **RTT Metrics**: Per-tier delivery latency tracking
  - `RESULT_RTT` marker in benchmark output: dequeue→webhook-received
  - p50/p95/p99 per tier, independent of scheduling precision
  - Parsed by `run-all.ps1` into JSON report

### Changed

- **PrecisionScheduler**: Replaced `Semaphore` with `ResizableSemaphore`
- **PrecisionScheduler**: Replaced per-intent VT sleep with `CohortManager.register()`
- **PrecisionScheduler**: `runBatchConsumer` now uses `acquireWithBorrow()` instead of `semaphore.acquire()`
- **CLAUDE.md**: Updated architecture diagram and key design decisions
- **README.md / README.zh.md**: Complete rewrite for v0.8.0 accuracy

### Architecture

```
v0.7.1: loomq-core (storage + WAL + scheduler) | loomq-server (HTTP delivery)
v0.8.0: loomq-core (+ CohortManager + ResizableSemaphore + Arrow/AdapTBF) | loomq-server (HTTP delivery)
```

## [0.7.1] - 2026-04-17

### Added

- **DeliveryHandler SPI Interface**: Abstraction for intent delivery
  - `DeliveryHandler` interface in `com.loomq.spi` package
  - `DeliveryResult` enum for delivery outcomes (SUCCESS, RETRY, DEAD_LETTER, EXPIRED)
  - Enables custom delivery implementations (HTTP, message queue, local callback)

- **HttpDeliveryHandler**: HTTP webhook delivery implementation
  - Implements `DeliveryHandler` interface
  - Encapsulates HTTP client logic from `HttpCallbackClient`
  - Located in `loomq-server` module

### Changed

- **PrecisionScheduler**: Migrated from `loomq-server` to `loomq-core`
  - Now uses `DeliveryHandler` SPI instead of direct HTTP calls
  - Supports custom delivery handlers via constructor
  - Falls back to ServiceLoader for handler discovery

- **BatchDispatcher**: Migrated from `loomq-server` to `loomq-core`
  - Now uses `DeliveryHandler` SPI instead of direct HTTP calls
  - Supports custom delivery handlers via constructor

- **LoomqEngine**: Integrated PrecisionScheduler
  - Added `PrecisionScheduler` as core component
  - Added `deliveryHandler()` and `redeliveryDecider()` builder methods
  - Added `getScheduler()` method for advanced usage
  - Removed internal scheduling logic in favor of PrecisionScheduler

- **LoomqEngineFactory**: Added DeliveryHandler support
  - Added factory methods with `DeliveryHandler` parameter
  - Simplified tier configuration (removed internal config)

### Architecture

```
v0.7.0: loomq-core (storage + WAL) | loomq-server (scheduler + HTTP)
v0.7.1: loomq-core (storage + WAL + scheduler) | loomq-server (HTTP delivery only)
```

Module dependency reduction:
- Shells can now use `loomq-core` for complete scheduling capability
- `loomq-server` provides HTTP delivery implementation only

### Migration Guide

For existing users of `loomq-server`:

1. No API changes - all existing code continues to work
2. If using `PrecisionScheduler` directly, provide a `DeliveryHandler`:
   ```java
   PrecisionScheduler scheduler = new PrecisionScheduler(intentStore, new HttpDeliveryHandler());
   ```

For embedded usage:

1. Use `LoomqEngine` with a custom `DeliveryHandler`:
   ```java
   LoomqEngine engine = LoomqEngine.builder()
       .walDir(Path.of("./data"))
       .deliveryHandler(intent -> DeliveryResult.SUCCESS)
       .build();
   ```

## [0.6.1] - 2026-04-10

### Added

- **IntentWalV2**: Simplified WAL implementation with binary codec
  - 8-byte header + binary payload, ~100ns serialization
  - Zero GC pressure compared to JSON-based V1
  - Support for REPLICATED ACK via ReplicationManager

- **BatchDispatcher**: Synchronous batch delivery engine
  - Virtual thread-based synchronous HTTP calls
  - Batch processing (up to 100 intents, max 10ms wait)
  - Backpressure via queue capacity limits

- **SimpleWalWriter**: Minimal WAL writer for raw byte operations
  - Direct file I/O with buffered channels
  - Group commit support
  - Checkpoint-based recovery

- **IntentBinaryCodec**: High-performance intent serialization
  - Binary format vs JSON: ~20x faster
  - Compact encoding with optional compression

- **AdaptiveFlushStrategy**: Intelligent WAL flush optimization
  - Dynamic batch sizing based on load
  - Latency-aware flush timing

### Changed

- `LoomqEngine` now uses `IntentWalV2` and `BatchDispatcher`
- Removed legacy WAL implementations (AsyncWalWriter, SyncWalWriter, ReplicatingWalWriter)
- Removed legacy scheduler (IntentScheduler) - replaced by PrecisionScheduler
- Removed legacy metrics classes (LoomQMetrics, MetricsEndpoint)

### Architecture

```
v0.6.0: Intent → JSON → WAL → Async Dispatch
v0.6.1: Intent → Binary → WAL → Batch Sync Dispatch
```

Code reduction: ~60% less code in core path

## [0.6.0] - 2026-04-10

### Added

- **Hand-written JSON Serializer**: Zero-copy serialization for Intent responses
  - `IntentResponseSerializer`: Pre-defined byte array templates for JSON structure
  - Direct `ByteBuf` write, no intermediate `byte[]` copy
  - Fast-path string serialization (no escape needed for most cases)
  - **6.2x faster** than Jackson ObjectMapper

- **DirectSerializedResponse Interface**: Marker interface for zero-copy serialization
  - Enables response types to bypass Jackson entirely
  - `IntentResponseData` implements this interface

- **Netty HTTP Server**: High-performance HTTP layer
  - 4 I/O threads + virtual thread business pool
  - RadixTree routing for O(k) path lookup
  - Semaphore-based backpressure control
  - Connection limiting with graceful degradation

- **HTTP Benchmark Tool**: `NettyHttpBenchmark` for performance validation
  - Tests: Health Check, Create Intent, Get Intent
  - Multi-threaded virtual thread client
  - Detailed latency distribution (P50, P90, P99)

### Performance Results

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    LoomQ v0.6.0 HTTP API Benchmark                      │
├─────────────────┬──────────────┬──────────────┬──────────────┬─────────┤
│ Endpoint        │ Peak QPS     │ P50 Latency  │ P99 Latency  │ Threads │
├─────────────────┼──────────────┼──────────────┼──────────────┼─────────┤
│ POST /intents   │   41,786     │    2ms       │    7ms       │   100   │
│ GET /intents/{id}│   55,585     │    3ms       │    6ms       │   200   │
│ GET /health     │   61,279     │    1ms       │    3ms       │   100   │
└─────────────────┴──────────────┴──────────────┴──────────────┴─────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                    Precision Tier Benchmark                             │
├──────────────┬──────────────┬──────────────┬──────────────┬───────────┤
│ Tier         │ Peak QPS     │ P50 (ms)     │ P99 (ms)     │ SLO       │
├──────────────┼──────────────┼──────────────┼──────────────┼───────────┤
│ ULTRA        │       45,949 │            1 │           12 │ ≤15ms ✅  │
│ FAST         │       44,718 │            1 │            5 │ ≤60ms ✅  │
│ HIGH         │       40,710 │            1 │            3 │ ≤120ms ✅ │
│ STANDARD     │       39,974 │            1 │            2 │ ≤550ms ✅ │
│ ECONOMY      │       42,295 │            1 │            4 │ ≤1100ms ✅│
└──────────────┴──────────────┴──────────────┴──────────────┴───────────┘
```

### Changed

- `IntentHandler` now returns `IntentResponseData` instead of `Map`
- `NettyRequestHandler` detects `DirectSerializedResponse` for zero-copy path
- Updated README with new performance benchmarks

### Technical Details

**JSON Serialization Optimization:**
- Before: Jackson `ObjectMapper.writeValueAsBytes()` → `Unpooled.wrappedBuffer()`
- After: Direct `ByteBuf` allocation → `writeTo(buf)` → HTTP response
- Eliminates one `byte[]` allocation and copy per response

## [0.5.2] - 2026-04-09

### Added
- **Virtual Thread HTTP Server**: Eliminate HTTP layer thread model bottleneck
  - Enable Javalin's built-in `useVirtualThreads` configuration
  - Leverage Java 21 virtual threads for HTTP request handling
  - Expected 5-10x throughput improvement (target: >= 150K QPS)

- **HTTP Server Configuration**: New configuration options
  - `server.virtual_threads`: Enable/disable virtual threads (default: true)
  - `server.backlog`: OS connection queue size (default: 0 = system default)
  - `server.max_request_size`: Maximum request body size (default: 10MB)

- **HTTP Performance Metrics**: Observability for HTTP layer
  - `loomq_http_requests_total`: Total HTTP request count
  - `loomq_http_request_duration_ms`: Request duration histogram
  - `loomq_http_active_connections`: Active connection gauge
  - `loomq_http_2xx_responses`: 2xx response count
  - `loomq_http_4xx_responses`: 4xx response count
  - `loomq_http_5xx_responses`: 5xx response count
  - `loomq_http_json_serialization_duration_ms`: JSON serialization timing

- **HTTP Benchmark Test**: Performance validation tool
  - `HttpVirtualThreadBenchmark`: Benchmark for virtual thread performance
  - Target validation: >= 150K QPS, P99 <= 20ms

### Changed
- Upgraded HTTP server to use virtual threads by default
- Updated `ServerConfig` with new configuration options
- Extended `LoomQMetrics` with HTTP performance metrics

## [0.5.1] - 2026-04-09

### Added
- **SLA Precision Tiers**: Configurable scheduling precision for resource optimization
  - `PrecisionTier` enum with 5 levels: ULTRA (10ms), FAST (50ms), HIGH (100ms), STANDARD (500ms), ECONOMY (1000ms)
  - Per-tier bucket groups with independent scan intervals
  - Dynamic sleep calculation with jitter for load distribution
  - Short-delay optimization (delay ≤ precisionWindow bypasses sleep)

- **BucketGroup**: Time-bucketed task storage per precision tier
  - `ConcurrentSkipListMap` for O(log n) operations
  - No global locks, per-tier isolation
  - Automatic bucket cleanup after dispatch

- **BucketGroupManager**: Centralized tier management
  - `Map<PrecisionTier, BucketGroup>` for tier isolation
  - Unified add/scan interface
  - Pending count tracking per tier

- **PrecisionScheduler**: Multi-tier scheduling engine
  - Independent scan threads per precision tier
  - Virtual thread sleep with jitter-based wake-up
  - Integrated with `MetricsCollector` for observability

- **Precision Tier Metrics**: Per-tier Prometheus metrics
  - `loomq_intent_total{precision_tier}`: Intent creation count by tier
  - `loomq_intent_due_total{precision_tier}`: Due intent count by tier
  - `loomq_scheduler_bucket_size{precision_tier}`: Bucket size by tier
  - `loomq_scheduler_wakeup_late_ms_p95{precision_tier}`: Wake-up latency by tier

- **API Enhancement**: Precision tier support in Intent API
  - `precisionTier` field in `CreateIntentRequest`
  - `precisionTier` field in `IntentResponse`
  - Defaults to `STANDARD` when not specified
  - Jackson `@JsonCreator` for case-insensitive parsing

- **Test Coverage**: Comprehensive precision tier testing
  - `PrecisionTierTest`: 6 unit tests for tier enum
  - `BucketGroupTest`: 12 unit tests for bucket operations
  - `PrecisionTierIntegrationTest`: 10 integration tests for mixed-tier scenarios
  - `PrecisionTierBenchmark`: Benchmark tool for tier comparison

### Changed
- Intent model extended with `precisionTier` field (default: STANDARD)
- `MetricsCollector` enhanced with per-tier metric collection
- `IntentResponse` includes `precisionTier` field
- `CreateIntentRequest` accepts optional `precisionTier` parameter

### SLO Commitments

| Tier | Precision Window | SLO (p99 Wake-up Latency) |
|------|-----------------|---------------------------|
| ULTRA | 10ms | ≤15ms |
| FAST | 50ms | ≤60ms |
| HIGH | 100ms | ≤120ms |
| STANDARD | 500ms | ≤550ms |
| ECONOMY | 1000ms | ≤1100ms |

### Architecture Notes
- Maintains virtual thread per-task sleep architecture (no centralized time wheel)
- Tier is fixed at Intent creation time (no runtime tier modification)
- WAL serialization automatically handles `precisionTier` field
- Backward compatible: missing tier defaults to HIGH for legacy data

## [0.5.0] - 2026-04-09

### Added
- **Intent-based Architecture (v5)**: Complete rewrite with Intent as the core abstraction
  - `Intent`: Unified delayed task entity with lifecycle management
  - `IntentStatus`: State machine with transitions (CREATED → SCHEDULED → DUE → DISPATCHING → DELIVERED → ACKED)
  - `IntentStore`: In-memory storage with skip-list index for efficient time-based queries
  - `IntentScheduler`: Virtual thread-based scheduling with pause/resume support
  - `IntentController`: REST API for Intent CRUD operations

- **WAL Health Monitoring**: Comprehensive health monitoring for Write-Ahead Log
  - `WalWriter.isHealthy()`: Real-time health status
  - `WalWriter.getHealthStatus()`: Detailed health metrics (idle time, error count, pending writes)
  - Last flush time tracking with configurable idle threshold
  - Flush error counting with automatic degradation detection

- **WAL Backpressure Control**: RingBuffer overflow protection
  - `appendAsyncWithTimeout()`: Timeout-based write with fast-fail semantics
  - `WALOverloadException`: Dedicated exception for overload conditions
  - Configurable timeout for backpressure scenarios
  - Graceful degradation under high load

- **WAL Future Separation**: Split commit and flush semantics
  - `appendAsync()`: ASYNC level - returns after buffer commit
  - `waitForFlush()`: DURABLE level - waits for fsync
  - `flushNow()`: Force immediate flush
  - Clear separation of concerns for different durability requirements

- **Prometheus Metrics for WAL**: Full observability integration
  - `loomq_wal_healthy`: Health status gauge
  - `loomq_wal_idle_time_ms`: Time since last flush
  - `loomq_wal_flush_errors_total`: Flush error counter
  - `loomq_wal_flush_latency_avg_ms`: Average flush latency
  - `loomq_wal_pending_writes`: Pending writes in buffer
  - `loomq_wal_ring_buffer_size`: Current RingBuffer size

- **Health Endpoints**: Comprehensive health check API
  - `GET /health`: Overall system health with component status
  - `GET /health/wal`: Detailed WAL health metrics
  - `GET /health/replica`: Replication lag status
  - `GET /health/ready`: Readiness probe for K8s
  - `GET /health/live`: Liveness probe for K8s

- **Idempotency Enhancement**: Window-based duplicate detection
  - `IdempotencyRecord`: Time-windowed idempotency tracking
  - Automatic cleanup of expired idempotency records
  - Support for both request-level and business-level deduplication

- **Test Infrastructure**: Comprehensive test suite with 340+ tests
  - `TestUtils`: Reusable test utilities and configurations
  - `WalBackpressureTest`: Stress tests for RingBuffer backpressure
  - `WalWriterIntegrationTest`: End-to-end WAL integration tests
  - `WalWriterTest`: Unified unit tests for WAL operations
  - All tests pass with optimized execution time

### Changed
- Migrated from Task-based to Intent-based architecture
- Removed legacy Task, TaskStatus, TaskStore, TaskScheduler classes
- Improved WAL with self-healing flush loop
- Enhanced `LoomQMetrics` with WAL-specific metrics
- Optimized test execution time (3x faster than before)
- Updated all health endpoints to use `MetricsEndpoint`

### Removed
- Legacy `Task` entity and related classes
- Legacy `TaskStore` implementation
- Legacy `TaskScheduler` implementation
- Legacy `RecoveryService` implementations
- Legacy `HealthController` (merged into `MetricsEndpoint`)
- Legacy benchmark framework (replaced with simpler tests)

### Fixed
- Fixed duplicate `/health` endpoint registration
- Fixed null payload handling in WAL records
- Fixed health status not reflecting running state

## [0.4.8] - 2026-04-08

### Added
- **Replication Framework**: Primary-Replica replication with ACK mechanisms
  - `ReplicationManager`: Central replication coordinator
  - `ReplicatingWalWriter`: WAL writer with replication support
  - `Ack`, `AckLevel`, `AckStatus`: ACK semantics for durability guarantees
  - `ReplicationRecord`: Task change propagation format
  - `ReplicaClient` / `ReplicaServer`: Netty-based replication protocol
  - `WalCatchUpManager`: Replica catch-up on reconnection
  - Configurable ACK levels: `ALL`, `QUORUM`, `ONE`, `NONE`

- **Lease-based Cluster Coordination**: Improved failover with fencing
  - `CoordinatorLease`: Epoch-based lease mechanism
  - `FencingToken`: Monotonic fencing for split-brain prevention
  - `HeartbeatManager`: Bidirectional health monitoring
  - `ShardStateMachine`: Shard state transitions with fencing
  - `FailoverController`: Automated failover with catch-up support
  - Lease arbitration for "explainable high availability"

- **Cluster Stability Enhancements**
  - Routing table version monotonicity with CAS updates
  - Node flapping detection (consecutive failures + time window)
  - Idempotent routing: consistent task-to-shard mapping
  - Task draining during failover with configurable timeout

### Changed
- Cluster coordination redesigned with lease arbitration (v0.5 lease concepts backported)
- Enhanced failure handling with fencing tokens
- Improved WAL with replication hooks

## [0.4.5] - 2026-04-08

### Added
- **Docker Support**: Complete Docker and Docker Compose configuration
  - Multi-stage Dockerfile with health checks
  - Docker Compose for single node and cluster modes
  - Docker Compose profiles for monitoring stack
- **Kubernetes Support**: Sample K8s manifests for deployment
  - ConfigMap for configuration management
  - StatefulSet for stateful deployment
  - Service for load balancing
- **Monitoring Stack**
  - Prometheus configuration and scrape targets
  - Grafana dashboards with key metrics
  - Health check endpoints (/health, /ready, /live)
- **Enhanced Configuration**
  - Environment variable support in application.yml
  - Comprehensive cluster configuration options
  - Metrics and health check configuration
- **Logging Enhancement**
  - Separate log files for WAL, scheduler, and audit
  - Async appender for high-throughput logging
  - Configurable log levels via environment variables
- **Startup Scripts**
  - Bash script for Linux/macOS (scripts/start.sh)
  - PowerShell script for Windows (scripts/start.ps1)
  - Automatic Java version checking
- **Build Automation**
  - Makefile with common build targets
  - Docker build integration
- **Documentation**
  - Comprehensive deployment guide (docs/DEPLOYMENT.md)
  - Systemd service configuration
  - Nginx load balancer example
  - Troubleshooting guide

### Changed
- Updated application.yml with environment variable placeholders
- Enhanced logback.xml with structured logging
- Updated README.md with v0.4.5 features

## [0.4.4] - 2026-04-08

### Added
- **RoutingTable**: Version-controlled routing table with CAS semantics
  - Monotonically increasing version numbers
  - Atomic compare-and-swap updates
  - Version mismatch detection for routing requests
- **Flapping Detection**: Node instability detection
  - Consecutive failure threshold (default: 3)
  - Time window-based detection (default: 30s)
  - Prevents frequent routing changes during node instability
- **ClusterCoordinatorV2**: Improved cluster coordination
  - Health state tracking (HEALTHY, SUSPECT, OFFLINE)
  - Heartbeat-based failure detection
  - CAS-based routing table updates
- **Failure Handling Config**
  - `keepRunningTasks`: Continue executing tasks on old node during failover
  - `rerouteNewRequests`: Route new requests to healthy nodes
  - `taskDrainTimeoutMs`: Timeout for draining tasks during failover
- **Idempotent Routing**: Consistent task-to-shard mapping
  - Same task ID always routes to same shard
  - Consistent across routing table changes
- **ShardStabilityTest**: 13 comprehensive test cases
  - Routing table version monotonicity tests
  - Flapping detection tests
  - Failure handling strategy tests
  - Concurrent idempotency tests

## [0.4.3] - 2026-04-08

### Added
- **Benchmark Framework**: Standardized performance testing
  - BenchmarkBase with configurable parameters
  - InMemoryBenchmark for fast validation
  - BenchmarkReporter with Markdown/CSV/JSON output
  - Statistical analysis with CV and outlier detection
- **BenchmarkReporter**: Multi-format report generation
  - Markdown with credibility assessment
  - CSV for data analysis
  - JSON for programmatic access
- **BenchmarkFrameworkTest**: Unit tests for benchmark framework
  - Report format validation
  - Credibility assessment tests
  - Latency recorder tests

### Changed
- Improved benchmark repeatability with warmup phases
- Added GC statistics collection

## [0.4.2] - 2026-04-08

### Added
- **Recovery Service**: Complete recovery pipeline
  - WAL replay with batch processing
  - State reconstruction for pending tasks
  - Safe mode for corrupted WAL segments
- **WalReplayer**: Optimized WAL replay
  - Batch read for performance
  - Checkpoint-based fast recovery
- **Recovery Integration Tests**: End-to-end recovery validation
  - Full recovery scenario tests
  - Partial corruption handling
  - Concurrent recovery tests

## [0.4.1] - 2026-04-08

### Added
- **TaskStatusV3**: Unified state machine with 10 states
  - PENDING → SCHEDULED → READY → RUNNING → SUCCESS/FAILED/DEAD_LETTER
  - Atomic state transitions with validation
- **RetryPolicy**: Configurable retry strategies
  - Fixed interval retry
  - Exponential backoff
  - Maximum retry limits
- **Idempotency Enhancement**
  - `idempotencyKey`: Request-level deduplication
  - `bizKey`: Business-level deduplication
  - Terminal state protection
- **Query APIs**
  - Query by business key
  - Query by status
  - Task lifecycle tracking

## [0.4.0] - 2026-04-08

### Added
- **AsyncWalWriter**: High-throughput WAL with RingBuffer
  - MPSC lock-free ring buffer
  - Group commit with configurable batch size
  - Dual ACK levels (ASYNC/DURABLE)
- **TimeBucketScheduler**: 100ms time bucket scheduling
  - ConcurrentSkipListMap for time index
  - Massive throughput optimization
- **DispatchLimiter**: Backpressure control
  - Semaphore-based rate limiting
  - Configurable concurrent dispatch limit
- **ShardRouter**: Consistent hash routing
  - MD5-based hash function
  - 150 virtual nodes for even distribution
- **ClusterManager**: Cluster lifecycle management
  - Node join/leave handling
  - Shard migration support

### Changed
- Refactored from single-node to distributed architecture
- Virtual threads throughout for high concurrency

## [0.3.0] - 2026-04-01

### Added
- **Core Engine**: Basic delayed task scheduling
  - Task creation with delay
  - Webhook dispatch
  - In-memory storage with skip-list index
- **WAL Engine**: Write-ahead log for durability
  - Segment-based storage
  - Sync/async flush strategies
- **REST API**: Javalin-based HTTP API
  - Task CRUD operations
  - Health check endpoint
- **Virtual Threads**: Java 21 virtual thread support

## [0.1.0] - 2026-03-15

### Added
- Initial project skeleton
- Basic build configuration
- README documentation

---

## Version Naming

- **Major (X.0.0)**: Breaking changes, architecture redesign
- **Minor (0.X.0)**: New features, enhancements
- **Patch (0.0.X)**: Bug fixes, documentation updates
- **Sub-patch (0.0.0.X)**: Engineering improvements, packaging

## Compatibility

| Version | Java | Compatibility |
|---------|------|---------------|
| 0.4.5+ | 21+  | Fully compatible |
| 0.4.0+ | 21+  | Requires Java 21 for virtual threads |
| 0.3.0+ | 21+  | Requires Java 21 for virtual threads |
| 0.1.0+ | 17+  | Legacy support |

## Migration Guide

### From 0.4.4 to 0.4.5

1. Configuration files now support environment variables
2. New health check endpoints available
3. Docker deployment recommended for production

### From 0.4.0 to 0.4.4

1. Update task status enum to TaskStatusV3
2. Implement idempotency key handling
3. Configure retry policies

### From 0.3.0 to 0.4.0

1. Add cluster configuration for distributed mode
2. Update API endpoints to /api/v1/tasks
3. Configure shard routing
