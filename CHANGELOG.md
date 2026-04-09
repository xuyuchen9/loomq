# Changelog

All notable changes to LoomQ are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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
