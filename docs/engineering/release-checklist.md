# Release Checklist

Use this before tagging or publishing a LoomQ release.

## Code

- Confirm the root README matches the current model vocabulary: `Intent`, `durable time kernel`, and the current capability split.
- Check that public docs do not describe experimental features as stable.
- Verify any config changes are reflected in `docs/operations/CONFIGURATION.md`.
- Verify any API changes are reflected in `docs/development/API.md`.
- Run the formatting gate (`make check-format` or `mvn -B -ntp com.diffplug.spotless:spotless-maven-plugin:3.0.0:check`) before tagging.

## Build

- Run the normal test suite.
- Run any required integration or full-test profile if the change touches persistence, recovery, or HTTP behavior.
- Build the server artifact from the root module.
- Confirm the PR CI package job passed, not just the test jobs.
- Start the server locally once and confirm it boots with the intended config.

## Runtime

- Confirm the startup log prints the effective runtime summary.
- Confirm `/health` returns healthy.
- Confirm `/metrics` returns a response and the key counters move as expected.
- Confirm a basic `POST /v1/intents` create flow still works.
- Confirm `server.host` / `server.port` are the actual HTTP bind target expected by the deployment.
- Confirm `SIGTERM` or orchestrator stop drains active HTTP requests within `netty.gracefulShutdownTimeoutMs`.
- If the release touches Raft, confirm follower reads and writes return retryable `503` responses and `/health` / `/metrics` expose Raft role, leader id, term, commit index, peer reachability, and write safety signals.

## Release Notes

- Call out which capabilities are stable, beta, or not yet committed.
- Avoid mixing historical benchmark numbers with current guarantee language.
- Note any compatibility changes explicitly.

## Final Gate

- Tag only after build, smoke test, docs, and formatting checks are aligned.
- If a change affects persistence or recovery, include a short rollback note in the release discussion.
