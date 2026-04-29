# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 0.8.x   | :white_check_mark: |
| 0.7.x   | :white_check_mark: |
| < 0.7.0 | :x:                |

## Reporting a Vulnerability

To report a security vulnerability, please use one of these channels:

- **Preferred**: GitHub Security Advisory → [Report a vulnerability](https://github.com/loomq/loomq/security/advisories/new)
- **Alternative**: Email the maintainers directly (do not open a public issue)

### What to include

- Description of the vulnerability
- Steps to reproduce
- Affected versions
- Any potential mitigations you've identified

### Disclosure Timeline

1. Report received → acknowledgment within 48 hours
2. Investigation and fix → typically within 7 days
3. Coordinated disclosure with CVE assignment if applicable
4. Patch released + advisory published

## Security Design Notes

### Core Module (`loomq-core`)

- No authentication or authorization — that is the embedder's responsibility
- WAL files are written to the local filesystem with OS-level permissions
- Binary codec uses CRC32 checksums for data integrity
- No network listening in core — all network I/O is in `loomq-server`

### Server Module (`loomq-server`)

- Token-based authentication via `security.tokens` config (disabled by default)
- HTTP request size limits via `server.max_request_size` (default: 10MB)
- Connection limiting via `netty.maxConnections` (default: 10000)
- Semaphore-based concurrent business request limiting via `netty.maxConcurrentBusinessRequests`
- Netty graceful shutdown with configurable timeout

### Recommended Production Hardening

- Enable token authentication: set `security.enabled=true` and configure tokens
- Place LoomQ behind a reverse proxy (nginx/Caddy) for TLS termination
- Restrict WAL data directory to the LoomQ process user only
- Use network policies to restrict access to the replication port (default 9090)
- Set appropriate JVM security manager or use container security contexts
