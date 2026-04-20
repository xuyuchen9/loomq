# Benchmark Checklist

Use this when collecting or updating benchmark results.

## Before You Run

- Record the exact commit SHA.
- Record JDK version, CPU model, RAM, storage type, and OS.
- Make sure the benchmark script and the server artifact come from the same build.
- Clear any stale result files if you want a fresh baseline.

## What To Run

- Use the repository benchmark script in `benchmark/scripts/`.
- Run the same workload at least twice if you are comparing results.
- If the change touches core scheduling or WAL, include both throughput and latency runs.

## What To Capture

- Request rate / throughput
- P95 / P99 latency
- Recovery time after restart
- WAL flush behavior
- Backpressure or rejection counts

## Reporting Rules

- Keep historical runs comparable by using the same script arguments.
- Do not mix experimental numbers into the stable release summary.
- Annotate any run that used a different hardware profile or JDK.

## Acceptance Criteria

- Results are reproducible from the documented command.
- The summary shows what changed and why it matters.
- Any regression has a short explanation and a follow-up plan.

