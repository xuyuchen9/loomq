#!/usr/bin/env bash
# ============================================================
#  LoomQ Stress Test — Convenience Wrapper
# ============================================================
#  Runs gRPC stress test via benchmark.sh with stress mode.
#  Usage: ./stress.sh [--threads 500,1000,2000] [--duration 30]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec "$SCRIPT_DIR/benchmark.sh" --stress --stress-only grpc "$@"
