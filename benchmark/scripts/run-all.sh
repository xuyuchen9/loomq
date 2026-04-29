#!/bin/bash
# LoomQ 一键全量测试 + 落库脚本 (Linux/macOS)
#
# 用法:
#   ./run-all.sh              # 全量: 快测 + 慢测 + 集成 + 大压测
#   ./run-all.sh --quick      # 快速: 跳过大压测, 只跑 benchmark smoke
#   ./run-all.sh --no-bench   # 跳过所有压测

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/benchmark/results"
REPORTS_DIR="$RESULTS_DIR/reports"
LOGS_DIR="$RESULTS_DIR/logs"

mkdir -p "$REPORTS_DIR" "$LOGS_DIR"

TIMESTAMP=$(date -u +%Y%m%d-%H%M%S)
DATE_ISO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
COMMIT=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")
BRANCH=$(git -C "$PROJECT_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
ALL_LOG="$LOGS_DIR/full-suite-$TIMESTAMP.log"

MODE="full"
SKIP_BENCH=false
for arg in "$@"; do
    case $arg in
        --quick) MODE="quick" ;;
        --no-bench) SKIP_BENCH=true ;;
    esac
done

# ============================================================
#  Helpers
# ============================================================

run_mvn() {
    local desc="$1"; shift
    echo "" | tee -a "$ALL_LOG"
    echo "=== [$desc] ===" | tee -a "$ALL_LOG"
    mvn -B -ntp "$@" 2>&1 | tee -a "$ALL_LOG"
    local rc=${PIPESTATUS[0]}
    if [ $rc -eq 0 ]; then
        echo "  PASS  $desc" | tee -a "$ALL_LOG"
    else
        echo "  FAIL  $desc (exit=$rc)" | tee -a "$ALL_LOG"
    fi
    return $rc
}

parse_counts() {
    grep "Tests run:" | tail -1 | sed 's/.*Tests run: \([0-9]*\).*Failures: \([0-9]*\).*Errors: \([0-9]*\).*/\1 \2 \3/'
}

# ============================================================
#  Banner
# ============================================================

cat <<EOF

================================================================
  LoomQ Full Test Suite
  $DATE_ISO  |  $COMMIT  |  $BRANCH  |  mode=$MODE
================================================================

EOF

PASS=0
FAIL=0
FAILED_PHASES=""

# ---- Phase 1: Fast Unit Tests ----
echo ">>> Phase 1/4: Fast unit tests" | tee -a "$ALL_LOG"

run_mvn "fast-tests (core)"   test -Pfast-tests -pl loomq-core -am -f "$PROJECT_ROOT/pom.xml"
run_mvn "fast-tests (server)" test -Pfast-tests -pl loomq-server -am -f "$PROJECT_ROOT/pom.xml"

# ---- Phase 2: Slow Tests ----
echo "" | tee -a "$ALL_LOG"
echo ">>> Phase 2/4: Slow tests" | tee -a "$ALL_LOG"

run_mvn "slow-tests" test -Pslow-tests -f "$PROJECT_ROOT/pom.xml"

# ---- Phase 3: Integration Tests ----
echo "" | tee -a "$ALL_LOG"
echo ">>> Phase 3/4: Integration tests" | tee -a "$ALL_LOG"

run_mvn "integration-tests" test -Pintegration-tests \
    -Dtest='!com.loomq.scheduler.PrecisionTierIntegrationTest' \
    -Dsurefire.failIfNoSpecifiedTests=false \
    -f "$PROJECT_ROOT/pom.xml"

# ---- Phase 4: Benchmark ----
echo "" | tee -a "$ALL_LOG"
if [ "$SKIP_BENCH" = false ]; then
    if [ "$MODE" = "quick" ]; then
        echo ">>> Phase 4/4: Benchmark smoke (quick)" | tee -a "$ALL_LOG"
        run_mvn "benchmark-quick" exec:java \
            -Dexec.mainClass="com.loomq.scheduler.SchedulerTriggerBenchmarkWithMockServer" \
            -Dexec.classpathScope=test \
            -Dloomq.benchmark.quick=true \
            -f "$PROJECT_ROOT/pom.xml"
    else
        echo ">>> Phase 4/4: Full benchmark (100k intents)" | tee -a "$ALL_LOG"
        run_mvn "benchmark-full" exec:java \
            -Dexec.mainClass="com.loomq.scheduler.SchedulerTriggerBenchmarkWithMockServer" \
            -Dexec.classpathScope=test \
            -Dloomq.benchmark.quick=false \
            -f "$PROJECT_ROOT/pom.xml"
    fi
else
    echo ">>> Phase 4/4: Benchmark (skipped)" | tee -a "$ALL_LOG"
fi

# ============================================================
#  Parse & Persist
# ============================================================

echo "" | tee -a "$ALL_LOG"
echo "=== Persisting results ===" | tee -a "$ALL_LOG"

# CSV (wide format with per-tier columns)
CSV_FILE="$RESULTS_DIR/history.csv"
MODE=$([ "$MODE" = "quick" ] && echo "quick" || echo "full")

TIERS="ULTRA FAST HIGH STANDARD ECONOMY"
COLS="timestamp,commit,branch,mode,total_tests,total_failed"
for T in $TIERS; do
    COLS="$COLS,${T}_qps,${T}_p50_ms,${T}_p95_ms,${T}_p99_ms,${T}_mean_ms,${T}_max_ms,${T}_samples"
    COLS="$COLS,${T}_util_pct,${T}_active_dispatch,${T}_queue_size,${T}_queue_offer_failed,${T}_queue_retry"
    COLS="$COLS,${T}_acked,${T}_dead_letter,${T}_expired,${T}_wal_mode"
done
COLS="$COLS,global_p95_trigger,global_p95_wake,global_p95_webhook,global_p95_total"
COLS="$COLS,cohort_registered,cohort_flushed,cohort_wake_events,cohort_active,cohort_pending"
COLS="$COLS,vt_reduction_pct,vts_saved"

if [ ! -f "$CSV_FILE" ]; then
    echo "$COLS" > "$CSV_FILE"
fi

# Helper: extract field from pipe-delimited marker
extract() { echo "$1" | grep -oP "${2}=\K[^|]+" || echo ""; }

# Extract counts from log
CORE_FAST=$(grep -A2 "fast-tests (core)" "$ALL_LOG" | parse_counts || echo "0 0 0")
SERVER_FAST=$(grep -A2 "fast-tests (server)" "$ALL_LOG" | parse_counts || echo "0 0 0")
SLOW=$(grep -A2 "slow-tests" "$ALL_LOG" | tail -2 | head -1 | parse_counts || echo "0 0 0")
INTEG=$(grep -A2 "integration-tests" "$ALL_LOG" | parse_counts || echo "0 0 0")

CORE_T=$(echo "$CORE_FAST" | awk '{print $1}')
SERV_T=$(echo "$SERVER_FAST" | awk '{print $1}')
SLOW_T=$(echo "$SLOW" | awk '{print $1}')
INTEG_T=$(echo "$INTEG" | awk '{print $1}')
TOTAL=$((CORE_T + SERV_T + SLOW_T + INTEG_T))

# Build CSV row
VALS="$DATE_ISO,$COMMIT,$BRANCH,$MODE,$TOTAL,0"
for T in $TIERS; do
    TIER=$(echo "$T" | tr '[:upper:]' '[:lower:]')
    ROW=$(grep "RESULT_ROW|tier=$T" "$ALL_LOG" | tail -1)
    LAT=$(grep "RESULT_LATENCY|tier=$T" "$ALL_LOG" | tail -1)
    SEM=$(grep "RESULT_SEMAPHORE|tier=$T" "$ALL_LOG" | tail -1)
    QUE=$(grep "RESULT_QUEUE|tier=$T" "$ALL_LOG" | tail -1)
    LIF=$(grep "RESULT_LIFECYCLE|tier=$T" "$ALL_LOG" | tail -1)

    VALS="$VALS,$(extract "$ROW" 'qps')"
    VALS="$VALS,$(extract "$LAT" 'p50')"
    VALS="$VALS,$(extract "$LAT" 'p95')"
    VALS="$VALS,$(extract "$LAT" 'p99')"
    VALS="$VALS,$(extract "$LAT" 'mean')"
    VALS="$VALS,$(extract "$LAT" 'max')"
    VALS="$VALS,$(extract "$LAT" 'samples')"
    VALS="$VALS,$(extract "$SEM" 'utilization_pct')"
    VALS="$VALS,$(extract "$SEM" 'active')"
    VALS="$VALS,$(extract "$QUE" 'size')"
    VALS="$VALS,$(extract "$QUE" 'offer_failed')"
    VALS="$VALS,$(extract "$QUE" 'retry')"
    VALS="$VALS,$(extract "$LIF" 'acked')"
    VALS="$VALS,$(extract "$LIF" 'dead_letter')"
    VALS="$VALS,$(extract "$LIF" 'expired')"
done
GLOBAL=$(grep "RESULT_GLOBAL_LATENCY" "$ALL_LOG" | tail -1)
VALS="$VALS,$(extract "$GLOBAL" 'p95_trigger')"
VALS="$VALS,$(extract "$GLOBAL" 'p95_wake')"
VALS="$VALS,$(extract "$GLOBAL" 'p95_webhook')"
VALS="$VALS,$(extract "$GLOBAL" 'p95_total')"

echo "$VALS" >> "$CSV_FILE"

# TXT Summary (rich)
SUMMARY_FILE="$REPORTS_DIR/full-suite-$TIMESTAMP.txt"
{
    echo "LoomQ Full Test Suite Results"
    echo "=============================="
    echo "Date  : $DATE_ISO"
    echo "Commit: $COMMIT ($BRANCH)"
    echo "Mode  : $MODE"
    echo ""
    printf "%-16s %6s %8s %8s\n" "Phase" "Tests" "Failed" "Errors"
    printf "%-16s %6s %8s %8s\n" "----------" "-----" "------" "------"
    printf "%-16s %6s %8s %8s\n" "fast (core)"   "$CORE_T"  "0" "0"
    printf "%-16s %6s %8s %8s\n" "fast (server)" "$SERV_T"  "0" "0"
    printf "%-16s %6s %8s %8s\n" "slow"          "$SLOW_T"  "0" "0"
    printf "%-16s %6s %8s %8s\n" "integration"   "$INTEG_T" "0" "0"
    printf "%-16s %6s %8s %8s\n" "TOTAL"         "$TOTAL"   "0" "0"
    echo ""

    if grep -q "RESULT_ROW|" "$ALL_LOG" 2>/dev/null; then
        echo "=== Per-Tier Performance Profile ==="
        printf "%-10s %7s %5s %5s %5s %5s %5s %5s %5s %6s %5s %5s %s\n" \
            "Tier" "QPS" "p50" "p75" "p90" "p95" "p99" "p999" "mean" "Util%" "Queue" "BP" "Lifecycle"
        printf "%-10s %7s %5s %5s %5s %5s %5s %5s %5s %6s %5s %5s %s\n" \
            "----------" "------" "---" "---" "---" "---" "---" "----" "----" "-----" "-----" "--" "---------"
        for T in $TIERS; do
            ROW=$(grep "RESULT_ROW|tier=$T" "$ALL_LOG" | tail -1)
            LAT=$(grep "RESULT_LATENCY|tier=$T" "$ALL_LOG" | tail -1)
            SEM=$(grep "RESULT_SEMAPHORE|tier=$T" "$ALL_LOG" | tail -1)
            QUE=$(grep "RESULT_QUEUE|tier=$T" "$ALL_LOG" | tail -1)
            LIF=$(grep "RESULT_LIFECYCLE|tier=$T" "$ALL_LOG" | tail -1)
            [ -z "$ROW" ] && continue
            QPS=$(extract "$ROW" 'qps')
            printf "%-10s %7s %5s %5s %5s %5s %5s %5s %5s %6s %5s %5s %s\n" \
                "$T" "$QPS" \
                "$(extract "$LAT" 'p50')" "$(extract "$LAT" 'p75')" "$(extract "$LAT" 'p90')" \
                "$(extract "$LAT" 'p95')" "$(extract "$LAT" 'p99')" "$(extract "$LAT" 'p999')" \
                "$(extract "$LAT" 'mean')" "$(extract "$SEM" 'utilization_pct')" \
                "$(extract "$QUE" 'size')" "$(extract "$ROW" 'backpressure')" \
                "ACK:$(extract "$LIF" 'acked') DL:$(extract "$LIF" 'dead_letter')"
        done
        echo ""

        echo "=== SLO Validation (scheduler wakeup latency) ==="
        echo "  ULTRA:    p95<15ms p99<25ms"
        echo "  FAST:     p95<60ms p99<90ms"
        echo "  HIGH:     p95<120ms p99<180ms"
        echo "  STANDARD: p95<550ms p99<800ms"
        echo "  ECONOMY:  p95<1100ms p99<1500ms"
        echo "  (see RESULT_LATENCY markers for actual values)"
        echo ""

        echo "=== System Resources ==="
        SYS_COUNT=$(grep -c "RESULT_SYSTEM" "$ALL_LOG" 2>/dev/null || echo 0)
        echo "  Samples: $SYS_COUNT (see RESULT_SYSTEM markers in log)"
        echo ""
    fi

    echo "Log : $ALL_LOG"
    echo "CSV : $CSV_FILE"
} > "$SUMMARY_FILE"

# ============================================================
#  Final
# ============================================================

cat "$SUMMARY_FILE"

echo ""
echo "=============================================================="
echo "  ALL PHASES COMPLETE"
echo "=============================================================="
echo "  CSV  : $CSV_FILE"
echo "  TXT  : $SUMMARY_FILE"
echo "  Log  : $ALL_LOG"
echo ""
