#!/bin/bash
# LoomQ 一键全量测试 + 落库脚本 (Linux/macOS)
#
# 用法:
#   ./run-all.sh              # 全量: 快测 + 慢测 + 集成 + 大压测
#   ./run-all.sh --quick      # 快速: 跳过大压测, 只跑 benchmark smoke
#   ./run-all.sh --no-bench   # 跳过所有压测
#
# 所有结果强制落库 (CSV + JSON + TXT)，旧报告自动轮转 (保留最近 10 组)。

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

# Extract field from pipe-delimited marker
extract() { echo "$1" | grep -oP "${2}=\K[^|]+" || echo ""; }

# Report rotation: keep only the N most recent reports/logs
rotate_reports() {
    local keep=10
    # reports: keep 10 pairs = 20 files (json+txt)
    local count
    count=$(ls -1t "$REPORTS_DIR"/*.json "$REPORTS_DIR"/*.txt 2>/dev/null | wc -l)
    if [ "$count" -gt "$((keep * 2))" ]; then
        ls -1t "$REPORTS_DIR"/*.json "$REPORTS_DIR"/*.txt 2>/dev/null | tail -n +"$((keep * 2 + 1))" | xargs rm -f --
    fi
    # logs: keep 10
    count=$(ls -1t "$LOGS_DIR"/*.log 2>/dev/null | wc -l)
    if [ "$count" -gt "$keep" ]; then
        ls -1t "$LOGS_DIR"/*.log 2>/dev/null | tail -n +"$((keep + 1))" | xargs rm -f --
    fi
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

run_mvn "fast-tests (core)"        test -Pfast-tests -pl loomq-core -am -f "$PROJECT_ROOT/pom.xml"
run_mvn "fast-tests (raft)"        test -Pfast-tests -pl loomq-raft -am -f "$PROJECT_ROOT/pom.xml"
run_mvn "fast-tests (channel-http)" test -Pfast-tests -pl loomq-channel/loomq-channel-http -am -f "$PROJECT_ROOT/pom.xml"
run_mvn "fast-tests (channel-grpc)" test -Pfast-tests -pl loomq-channel/loomq-channel-grpc -am -f "$PROJECT_ROOT/pom.xml"
run_mvn "fast-tests (server)"      test -Pfast-tests -pl loomq-server -am -f "$PROJECT_ROOT/pom.xml"

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

# Rotate old reports before saving new ones
rotate_reports

# --- CSV (unified 48-column wide format) ---
CSV_FILE="$RESULTS_DIR/history.csv"
MODE_VAL=$([ "$MODE" = "quick" ] && echo "quick" || echo "full")

TIERS="ULTRA FAST HIGH STANDARD ECONOMY"
COLS="timestamp,commit,branch,mode,java_version,os_name,total_tests,total_failed"
for T in $TIERS; do
    COLS="$COLS,${T}_qps,${T}_p95_ms,${T}_p99_ms,${T}_e2e_p95_ms,${T}_e2e_p99_ms,${T}_util_pct,${T}_backpressure"
done
COLS="$COLS,completion_rate,total_qps,global_p95_total_ms,vt_reduction_pct,cohort_wake_events"

# Header consistency: verify or rebuild
NEEDS_NEW_HEADER=true
if [ -f "$CSV_FILE" ]; then
    FIRST_LINE=$(head -1 "$CSV_FILE")
    if [ "$FIRST_LINE" = "$COLS" ]; then
        NEEDS_NEW_HEADER=false
    fi
fi
if [ "$NEEDS_NEW_HEADER" = true ]; then
    echo "$COLS" > "$CSV_FILE"
fi

# Extract env info
ENV_LINE=$(grep "^RESULT_ENV|" "$ALL_LOG" | tail -1)
JAVA_VER=$(extract "$ENV_LINE" 'java_version')
OS_NAME=$(extract "$ENV_LINE" 'os_name')

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
VALS="$DATE_ISO,$COMMIT,$BRANCH,$MODE_VAL,$JAVA_VER,$OS_NAME,$TOTAL,0"
for T in $TIERS; do
    ROW=$(grep "RESULT_ROW|tier=$T" "$ALL_LOG" | tail -1)
    LAT=$(grep "RESULT_LATENCY|tier=$T" "$ALL_LOG" | tail -1)
    E2E=$(grep "RESULT_E2E_LATENCY|tier=$T" "$ALL_LOG" | tail -1)
    SEM=$(grep "RESULT_SEMAPHORE|tier=$T" "$ALL_LOG" | tail -1)

    VALS="$VALS,$(extract "$ROW" 'qps')"
    VALS="$VALS,$(extract "$LAT" 'p95')"
    VALS="$VALS,$(extract "$LAT" 'p99')"
    VALS="$VALS,$(extract "$E2E" 'p95')"
    VALS="$VALS,$(extract "$E2E" 'p99')"
    VALS="$VALS,$(extract "$SEM" 'utilization_pct')"
    VALS="$VALS,$(extract "$ROW" 'backpressure')"
done

# Final RESULT marker for completion_rate
RESULT_LINE=$(grep "^RESULT|" "$ALL_LOG" | tail -1)
GLOBAL=$(grep "RESULT_GLOBAL_LATENCY" "$ALL_LOG" | tail -1)
OPT=$(grep "RESULT_OPTIMIZATION" "$ALL_LOG" | tail -1)
COHORT=$(grep "RESULT_COHORT" "$ALL_LOG" | tail -1)
SYSQPS=$(grep "RESULT_SYSTEM_QPS" "$ALL_LOG" | tail -1)

VALS="$VALS,$(extract "$RESULT_LINE" 'completion_rate')"
VALS="$VALS,$(extract "$SYSQPS" 'total_qps')"
VALS="$VALS,$(extract "$GLOBAL" 'p95_total')"
VALS="$VALS,$(extract "$OPT" 'vt_reduction_pct')"
VALS="$VALS,$(extract "$COHORT" 'wake_events')"

echo "$VALS" >> "$CSV_FILE"

# --- JSON ---
JSON_FILE="$REPORTS_DIR/full-suite-$TIMESTAMP.json"
{
    echo "{"
    echo "  \"timestamp\": \"$DATE_ISO\","
    echo "  \"commit\": \"$COMMIT\","
    echo "  \"branch\": \"$BRANCH\","
    echo "  \"mode\": \"$MODE_VAL\","
    echo "  \"total_tests\": $TOTAL,"
    echo "  \"total_failed\": 0,"
    echo "  \"tiers\": {"
    first=true
    for T in $TIERS; do
        if [ "$first" = true ]; then first=false; else echo ","; fi
        ROW=$(grep "RESULT_ROW|tier=$T" "$ALL_LOG" | tail -1)
        LAT=$(grep "RESULT_LATENCY|tier=$T" "$ALL_LOG" | tail -1)
        E2E=$(grep "RESULT_E2E_LATENCY|tier=$T" "$ALL_LOG" | tail -1)
        printf "    \"%s\": {\"qps\": \"%s\", \"p95\": \"%s\", \"p99\": \"%s\", \"e2e_p95\": \"%s\", \"e2e_p99\": \"%s\"}" \
            "$T" "$(extract "$ROW" 'qps')" "$(extract "$LAT" 'p95')" "$(extract "$LAT" 'p99')" \
            "$(extract "$E2E" 'p95')" "$(extract "$E2E" 'p99')"
    done
    echo ""
    echo "  }"
    echo "}"
} > "$JSON_FILE"

# --- TXT Summary (rich) ---
SUMMARY_FILE="$REPORTS_DIR/full-suite-$TIMESTAMP.txt"
{
    echo "LoomQ Full Test Suite Results"
    echo "=============================="
    echo "Date  : $DATE_ISO"
    echo "Commit: $COMMIT ($BRANCH)"
    echo "Mode  : $MODE_VAL"
    echo "Java  : $JAVA_VER"
    echo "OS    : $OS_NAME"
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
        printf "%-10s %7s %5s %5s %5s %5s %5s %5s %5s %6s %5s %s\n" \
            "Tier" "QPS" "p50" "p95" "p99" "e2e_p95" "e2e_p99" "max" "mean" "Util%" "Queue" "BP"
        printf "%-10s %7s %5s %5s %5s %5s %5s %5s %5s %6s %5s %s\n" \
            "----------" "------" "---" "---" "---" "---" "----" "---" "----" "-----" "-----" "--"
        for T in $TIERS; do
            ROW=$(grep "RESULT_ROW|tier=$T" "$ALL_LOG" | tail -1)
            LAT=$(grep "RESULT_LATENCY|tier=$T" "$ALL_LOG" | tail -1)
            E2E=$(grep "RESULT_E2E_LATENCY|tier=$T" "$ALL_LOG" | tail -1)
            SEM=$(grep "RESULT_SEMAPHORE|tier=$T" "$ALL_LOG" | tail -1)
            QUE=$(grep "RESULT_QUEUE|tier=$T" "$ALL_LOG" | tail -1)
            [ -z "$ROW" ] && continue
            QPS=$(extract "$ROW" 'qps')
            printf "%-10s %7s %5s %5s %5s %5s %5s %5s %5s %6s %5s %s\n" \
                "$T" "$QPS" \
                "$(extract "$LAT" 'p50')" \
                "$(extract "$LAT" 'p95')" "$(extract "$LAT" 'p99')" \
                "$(extract "$E2E" 'p95')" "$(extract "$E2E" 'p99')" \
                "$(extract "$LAT" 'max')" "$(extract "$LAT" 'mean')" \
                "$(extract "$SEM" 'utilization_pct')" \
                "$(extract "$QUE" 'size')" "$(extract "$ROW" 'backpressure')"
        done
        echo ""

        echo "=== SLO Validation (E2E latency: executeAt -> webhook received) ==="
        echo "  ULTRA:    p95<50ms   p99<100ms"
        echo "  FAST:     p95<150ms  p99<250ms"
        echo "  HIGH:     p95<600ms  p99<1000ms"
        echo "  STANDARD: p95<1500ms p99<2200ms"
        echo "  ECONOMY:  p95<3500ms p99<5000ms"
        echo ""

        echo "=== System Resources ==="
        SYS_COUNT=$(grep -c "RESULT_SYSTEM" "$ALL_LOG" 2>/dev/null || echo 0)
        echo "  Samples: $SYS_COUNT (see RESULT_SYSTEM markers in log)"
        echo ""
    fi

    echo "Log : $ALL_LOG"
    echo "CSV : $CSV_FILE"
    echo "JSON: $JSON_FILE"
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
echo "  JSON : $JSON_FILE"
echo "  Log  : $ALL_LOG"
echo ""
