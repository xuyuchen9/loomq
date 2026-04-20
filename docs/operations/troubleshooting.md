# Troubleshooting

This guide focuses on the issues most likely to show up in the current codebase.

## Server Does Not Start

Check:

- the configured port is free
- `server.host` and `server.port` are valid
- the WAL directory is writable
- the startup log shows the runtime summary

## `/health` Is Unhealthy

Check:

- WAL flush activity
- `walHealthy`
- recent errors in the server log
- disk space and file permissions

## `/metrics` Looks Flat

Check:

- whether there was any traffic
- whether the path was hit through the standalone server
- whether the metrics snapshot is being scraped from the right instance

## Requests Are Rejected

If you see service-unavailable responses or rising rejects:

- inspect `loomq_http_concurrency_limit_exceeded_total`
- check `netty.maxConcurrentBusinessRequests`
- check `activeRequests`
- verify the downstream callback target is healthy

## Recovery Is Slow

Check:

- WAL directory size
- snapshot cadence
- restart disk throughput
- the `recovery.batch_size` and `recovery.concurrency_limit` settings

## Intent Behavior Looks Wrong

Check the lifecycle first:

- `executeAt` must be in the future on create
- `deadline` must be after `executeAt`
- only cancellable states can be cancelled
- only modifiable states can be patched

If the behavior still looks inconsistent, compare the API response with the lifecycle defined in `IntentStatus`.

