# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LoomQ is a durable time kernel for distributed systems. It schedules, persists, and delivers future events called **Intent**s. Built on Java 25 Virtual Threads. Two modules: `loomq-core` (embeddable, HTTP-free kernel) and `loomq-server` (standalone Netty HTTP server).

## Build & Run Commands

```bash
# Build
mvn clean package              # full build with tests
mvn clean package -DskipTests  # fast build, skip tests
make build / make build-fast   # Makefile shortcuts

# Test (Maven Surefire profiles with JUnit 5 tags)
mvn test                       # default: excludes benchmark/slow/integration
mvn test -Pintegration-tests   # integration group only
mvn test -Pfull-tests          # everything including slow/benchmark
mvn test -Dtest=ClassName      # single test class
mvn test -Dtest=ClassName#methodName  # single test method

# Run
java -jar loomq-server/target/loomq-server-0.7.0-SNAPSHOT.jar
make run-jar                   # Makefile shortcut

# Docker
make docker-build && make docker-run          # single container
make docker-compose-up                        # cluster + monitoring stack
```

## Architecture

```
loomq-server (Netty HTTP + JSON + webhook delivery)
    │
    └── loomq-core (embeddable kernel, zero HTTP/JSON deps)
            │
            ├── LoomqEngine          — builder-pattern entry point
            ├── PrecisionScheduler   — time-wheel buckets, per-tier scan threads
            ├── IntentStore          — in-memory ConcurrentHashMap storage
            ├── SimpleWalWriter      — binary WAL (~100ns/record, IntentBinaryCodec)
            ├── RecoveryPipeline     — snapshot + WAL replay on restart
            └── SPI interfaces       — DeliveryHandler, CallbackHandler, RedeliveryDecider
```

**Intent lifecycle:** CREATED → SCHEDULED → DUE → DISPATCHING → DELIVERED → ACKED (branches: CANCELLED, EXPIRED, DEAD_LETTERED)

**Five precision tiers:** ULTRA(10ms), FAST(50ms), HIGH(100ms default), STANDARD(500ms), ECONOMY(1000ms) — each has dedicated scan threads and batch consumers using virtual threads.

## Key Design Decisions

- **"Intent" is the public model** — older docs/code may say "task"; always use "Intent" in new code.
- **Core has zero HTTP/JSON dependencies** — `loomq-core` depends only on SLF4J, Owner, SnakeYAML, HdrHistogram. All transport concerns live in `loomq-server`.
- **DeliveryHandler SPI** — the scheduler in core delegates delivery through this interface; `loomq-server` provides `HttpDeliveryHandler`. Embedders supply their own.
- **Virtual threads everywhere** — `Executors.newVirtualThreadPerTaskExecutor()` for intent sleep/dispatch; no traditional thread pool tuning.
- **IntentStore is in-memory only** — ~10M intents ≈ ~8GB heap. Pluggable storage planned for v0.8.0.
- **Cluster/replication is Beta** — `ClusterManager`, `ShardRouter`, `LeaseCoordinator` exist but are experimental; Raft consensus is on the v0.8.0 roadmap.

## REST API

`POST /v1/intents`, `GET /v1/intents/{id}`, `PATCH /v1/intents/{id}`, `POST /v1/intents/{id}/cancel`, `POST /v1/intents/{id}/fire-now`, `GET /health`, `GET /health/live`, `GET /health/ready`, `GET /metrics`

## Configuration

Priority (highest→lowest): JVM system properties (`-Dloomq.xxx`) → external `./config/application.yml` → classpath `application.yml` → `@DefaultValue` annotations. Config interfaces use `org.aeonbits.owner` in `com.loomq.config`.

## CI

GitHub Actions (`.github/workflows/ci.yml`): Oracle JDK 25. `balanced-tests` on PR/push, `full-regression` on schedule/manual, `package` for fat JAR on push.

## Language

Documentation and configuration guides are written in Chinese (中文). Code comments and commit messages may be in Chinese or English.
