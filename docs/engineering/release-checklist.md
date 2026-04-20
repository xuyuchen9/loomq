# Release Checklist

Use this before tagging or publishing a LoomQ release.

## Code

- Confirm the root README matches the current model vocabulary: `Intent`, `durable time kernel`, and the current capability split.
- Check that public docs do not describe experimental features as stable.
- Verify any config changes are reflected in `docs/operations/CONFIGURATION.md`.
- Verify any API changes are reflected in `docs/development/API.md`.

## Build

- Run the normal test suite.
- Run any required integration or full-test profile if the change touches persistence, recovery, or HTTP behavior.
- Build the server artifact from the root module.
- Start the server locally once and confirm it boots with the intended config.

## Runtime

- Confirm the startup log prints the effective runtime summary.
- Confirm `/health` returns healthy.
- Confirm `/metrics` returns a response and the key counters move as expected.
- Confirm a basic `POST /v1/intents` create flow still works.

## Release Notes

- Call out which capabilities are stable, beta, or not yet committed.
- Avoid mixing historical benchmark numbers with current guarantee language.
- Note any compatibility changes explicitly.

## Final Gate

- Tag only after build, smoke test, and docs are aligned.
- If a change affects persistence or recovery, include a short rollback note in the release discussion.

