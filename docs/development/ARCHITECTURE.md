# LoomQ Architecture

This document describes the current codebase structure, not the historical task-oriented version.

## High-Level Layers

```mermaid
flowchart TB
    Server["loomq-server"] --> Api["IntentHandler / Netty HTTP"]
    Server --> Metrics["LoomQMetrics / HttpMetrics"]
    Server --> Engine["LoomqEngine"]

    Engine --> Scheduler["PrecisionScheduler"]
    Engine --> Wal["SimpleWalWriter"]
    Engine --> Recovery["RecoveryPipeline"]
    Engine --> Store["IntentStore"]
    Engine --> SPI["CallbackHandler / DeliveryHandler / RedeliveryDecider"]
```

## Core Responsibilities

### `loomq-core`

- durable intent lifecycle
- scheduling and rescheduling
- WAL persistence
- recovery after restart
- delivery and retry hooks
- precision-tier metrics

### `loomq-server`

- HTTP transport
- request validation
- JSON serialization
- Netty routing and backpressure
- standalone runtime bootstrap

## Intent Lifecycle

The code currently uses the `Intent` model and the `IntentStatus` state machine.

```mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> SCHEDULED
    SCHEDULED --> DUE
    SCHEDULED --> CANCELED
    DUE --> DISPATCHING
    DUE --> CANCELED
    DISPATCHING --> DELIVERED
    DISPATCHING --> DEAD_LETTERED
    DELIVERED --> ACKED
    DELIVERED --> EXPIRED
    CANCELED --> [*]
    ACKED --> [*]
    EXPIRED --> [*]
    DEAD_LETTERED --> [*]
```

## Extension Points

The kernel is intentionally shell-friendly:

- `CallbackHandler` reports lifecycle events back to the host
- `DeliveryHandler` owns the actual delivery mechanism
- `RedeliveryDecider` decides whether to retry after failure

That boundary keeps LoomQ focused on time semantics and lets higher-level products define lock or lease behavior outside the core.

## Config Path

The standalone server loads `LoomqConfig`, prints a runtime summary, and passes the effective WAL config into `LoomqEngine`.

For the canonical key list, see [`../operations/CONFIGURATION.md`](../operations/CONFIGURATION.md).

