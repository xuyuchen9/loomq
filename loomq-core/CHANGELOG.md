# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2026-04-15

### Added
- **Module Split**: Split monolithic project into multi-module Maven project
  - `loomq-bom`: Bill of Materials for version management
  - `loomq-core`: Zero-dependency embedded kernel (IntentStore, Scheduler, WAL)
  - `loomq-server`: HTTP service layer with Netty, REST API, Jackson
- **Core API**: New embedded API for direct Java integration
  - `LoomqEngine` class with Builder pattern configuration
  - `CallbackHandler` interface for event notifications (DUE, CANCELLED, FAILED)
  - `LoomqEngineFactory` for configuration from YAML/Properties
  - `AckMode` enum (ASYNC, DURABLE, REPLICATED)
- **Embedded Support**: Applications can now embed LoomQ directly without HTTP overhead

### Changed
- **PrecisionTier**: Removed Jackson annotations (`@JsonValue`, `@JsonCreator`) for zero-dependency core
- **Result**: Removed Jackson annotations (`@JsonInclude`) for zero-dependency core
- **Dependencies**: Migrated HTTP/JSON dependencies from core to server module

### Migration Guide (v0.6.x → v0.7.0)

#### For HTTP API Users
No changes required. The REST API remains fully compatible with v0.6.x.

#### For Embedded Usage
Use the new core API - see README for examples.

### Technical Debt
- Core module still has temporary dependencies (owner, snakeyaml, HdrHistogram)

[0.7.0]: https://github.com/yourusername/loomq/releases/tag/v0.7.0
