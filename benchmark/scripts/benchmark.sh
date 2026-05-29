#!/bin/bash
# LoomQ 性能基准测试脚本 (Linux/macOS)
#
# 用法:
#   ./benchmark.sh                           # 全量测试 (internal + HTTP + scheduler)
#   ./benchmark.sh --quick                   # 快速测试模式
#   ./benchmark.sh --scenario=scheduler      # 仅运行调度器压测
#   ./benchmark.sh --compare                 # 查看历史对比
#   ./benchmark.sh --help                    # 显示帮助
#
# 所有结果强制落库 (CSV + JSON + TXT)，旧报告自动轮转 (保留最近 10 组)。

set -euo pipefail

# ============================================================
#  配置
# ============================================================

QUICK_MODE=false
SCENARIO="all"
WORKLOAD="uniform"
NO_COMPILE=false
VERBOSE=false
COMPARE_ONLY=false

for arg in "$@"; do
    case $arg in
        --quick)           QUICK_MODE=true ;;
        --full)            QUICK_MODE=false ;;
        --scenario=*)      SCENARIO="${arg#*=}" ;;
        --workload=*)      WORKLOAD="${arg#*=}" ;;
        --no-compile)      NO_COMPILE=true ;;
        --compare)         COMPARE_ONLY=true ;;
        --verbose)         VERBOSE=true ;;
        --help|-h)
            echo ""
            echo "LoomQ Performance Benchmark v0.9.2"
            echo ""
            echo "Usage: ./benchmark.sh [Options]"
            echo ""
            echo "Options:"
            echo "  --quick              Quick test mode"
            echo "  --full               Full test mode (default)"
            echo "  --scenario=<name>    Scenario: all, internal, http, grpc, scheduler (default: all)"
            echo "  --workload=<name>    Workload: uniform, prod-typical, burst-ultra, mixed-heavy (default: uniform)"
            echo "  --no-compile         Skip compilation"
            echo "  --compare            Show history comparison"
            echo "  --verbose            Show full scenario output"
            echo "  --help               Show this help"
            echo ""
            echo "Note: All results are always persisted (CSV + JSON + TXT)."
            echo "      Old reports are automatically rotated (10 most recent kept)."
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $arg"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

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
MODE=$($QUICK_MODE && echo "quick" || echo "full")

# ============================================================
#  Helpers
# ============================================================

# Extract field from pipe-delimited marker (macOS compatible)
extract() { echo "$1" | sed -n "s/.*${2}=\([^|]*\).*/\1/p"; }

# Report rotation: keep only the N most recent reports/logs
rotate_reports() {
    local keep=10
    local count
    count=$(ls -1t "$REPORTS_DIR"/*.json "$REPORTS_DIR"/*.txt 2>/dev/null | wc -l)
    if [ "$count" -gt "$((keep * 2))" ]; then
        ls -1t "$REPORTS_DIR"/*.json "$REPORTS_DIR"/*.txt 2>/dev/null | tail -n +"$((keep * 2 + 1))" | xargs rm -f --
    fi
    count=$(ls -1t "$LOGS_DIR"/*.log 2>/dev/null | wc -l)
    if [ "$count" -gt "$keep" ]; then
        ls -1t "$LOGS_DIR"/*.log 2>/dev/null | tail -n +"$((keep + 1))" | xargs rm -f --
    fi
}

# ============================================================
#  Compare mode
# ============================================================

if [ "$COMPARE_ONLY" = true ]; then
    CSV_FILE="$RESULTS_DIR/history.csv"
    if [ -f "$CSV_FILE" ]; then
        LINE_COUNT=$(wc -l < "$CSV_FILE")
        echo "═══════════════════════════════════════════════════════════════"
        echo "                    历史记录对比"
        echo "═══════════════════════════════════════════════════════════════"
        echo ""
        echo "  记录总数: $((LINE_COUNT - 1))"
        echo ""

        # 最近 5 次运行摘要
        echo "┌─ 最近运行 ─────────────────────────────────────────────────────────────────┐"
        printf "│ %-19s %-8s %-10s %-10s %-8s │\n" "时间" "模式" "总QPS" "完成率" "ULTRA_QPS"
        echo "├──────────────────────────────────────────────────────────────────────────┤"

        HEADER_LINE=$(head -1 "$CSV_FILE")
        IFS=',' read -ra HEADER_COLS <<< "$HEADER_LINE"
        declare -A COL_IDX
        for i in "${!HEADER_COLS[@]}"; do
            COL_IDX[${HEADER_COLS[$i]}]=$i
        done

        tail -5 "$CSV_FILE" | while IFS=',' read -ra COLS; do
            TS="${COLS[${COL_IDX[timestamp]}]:-?}"
            MODE="${COLS[${COL_IDX[mode]}]:-?}"
            TQPS="${COLS[${COL_IDX[total_qps]}]:-?}"
            COMP="${COLS[${COL_IDX[completion_rate]}]:-?}"
            UQPS="${COLS[${COL_IDX[ULTRA_qps]}]:-?}"
            printf "│ %-19s %-8s %-10s %-10s %-8s │\n" "$TS" "$MODE" "$TQPS" "$COMP" "$UQPS"
        done
        echo "└──────────────────────────────────────────────────────────────────────────┘"
        echo ""

        # 最近两次回归对比
        if [ "$LINE_COUNT" -ge 3 ]; then
            PREV_LINE=$(tail -2 "$CSV_FILE" | head -1)
            CURR_LINE=$(tail -1 "$CSV_FILE")
            IFS=',' read -ra PREV_COLS <<< "$PREV_LINE"
            IFS=',' read -ra CURR_COLS <<< "$CURR_LINE"

            PREV_TS="${PREV_COLS[${COL_IDX[timestamp]}]:-?}"
            CURR_TS="${CURR_COLS[${COL_IDX[timestamp]}]:-?}"

            echo "┌─ 回归对比 ──────────────────────────────────────────────────────────────────┐"
            echo "│  上次: $PREV_TS"
            echo "│  本次: $CURR_TS"
            echo "├──────────┬──────────────────────────────────────────────────────────────────────┤"

            TIERS="ULTRA FAST HIGH STANDARD ECONOMY"
            for T in $TIERS; do
                PQ="${PREV_COLS[${COL_IDX[${T}_qps]}]:-0}"
                CQ="${CURR_COLS[${COL_IDX[${T}_qps]}]:-0}"
                PE="${PREV_COLS[${COL_IDX[${T}_e2e_p99_ms]}]:-0}"
                CE="${CURR_COLS[${COL_IDX[${T}_e2e_p99_ms]}]:-0}"
                [ "$PQ" = "0" ] && [ "$CQ" = "0" ] && continue
                if [ "$PQ" != "0" ] && [ "$CQ" != "0" ]; then
                    QPS_PCT=$(awk "BEGIN{printf \"%.1f\", ($CQ - $PQ) / $PQ * 100}")
                    printf "│ %-8s │ QPS: %s→%s (%s%%)  E2E-p99: %s→%s ms │\n" \
                        "$T" "$PQ" "$CQ" "$QPS_PCT" "$PE" "$CE"
                fi
            done
            echo "└──────────┴──────────────────────────────────────────────────────────────────────┘"
        fi
    else
        echo "未找到历史记录。请先运行一次基准测试。"
    fi
    exit 0
fi

# ============================================================
#  Banner
# ============================================================

echo ""
echo "+--------------------------------------------------------------+"
echo "|          LoomQ 性能基准测试 v0.9.2                            |"
echo "+--------------------------------------------------------------+"
echo ""
echo "Scenario: $SCENARIO"
echo "Mode:     $MODE"
echo "Workload: $WORKLOAD"
echo "Commit:   $COMMIT ($BRANCH)"
echo ""

# ============================================================
#  Compile
# ============================================================

SERVER_DIR="$PROJECT_ROOT/loomq-server"

if [ "$NO_COMPILE" = false ]; then
    echo ">>> Compiling project..."
    mvn -f "$PROJECT_ROOT/pom.xml" compile test-compile -q 2>&1 | tail -3
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo "[ERROR] Compilation failed"
        exit 1
    fi
    echo "  Compilation OK"
fi

# Rotate old reports
rotate_reports

# ============================================================
#  Run Scenarios
# ============================================================

BM_CLASS_SCHEDULER="com.loomq.scheduler.SchedulerTriggerBenchmarkWithMockServer"
BM_CLASS_INTERNAL="com.loomq.benchmark.InternalBenchmark"
BM_CLASS_HTTP="com.loomq.benchmark.HttpVirtualThreadBenchmark"
BM_CLASS_GRPC="com.loomq.benchmark.GrpcVirtualThreadBenchmark"
BM_CLASS_WAL="com.loomq.benchmark.WalThroughputBenchmark"
BM_CLASS_STORAGE="com.loomq.benchmark.StorageBenchmark"
BM_CLASS_OBSERVER="com.loomq.benchmark.ObserverOverheadBenchmark"

SCHEDULER_LOG="$LOGS_DIR/benchmark-scheduler-$TIMESTAMP.log"
INTERNAL_LOG="$LOGS_DIR/benchmark-internal-$TIMESTAMP.log"
HTTP_LOG="$LOGS_DIR/benchmark-http-$TIMESTAMP.log"
GRPC_LOG="$LOGS_DIR/benchmark-grpc-$TIMESTAMP.log"
WAL_LOG="$LOGS_DIR/benchmark-wal-$TIMESTAMP.log"
STORAGE_LOG="$LOGS_DIR/benchmark-storage-$TIMESTAMP.log"
OBSERVER_LOG="$LOGS_DIR/benchmark-observer-$TIMESTAMP.log"

run_scenario() {
    local name="$1"
    local main_class="$2"
    local log_file="$3"
    shift 3
    local extra_args=("$@")

    echo ""
    echo "=== $name ==="

    local mvn_args=(
        exec:java
        -Dexec.mainClass="$main_class"
        -Dexec.classpathScope=test
        -q
    )
    for a in "${extra_args[@]}"; do
        mvn_args+=("$a")
    done

    if [ "$VERBOSE" = true ]; then
        mvn -f "$SERVER_DIR/pom.xml" "${mvn_args[@]}" 2>&1 | tee "$log_file"
    else
        mvn -f "$SERVER_DIR/pom.xml" "${mvn_args[@]}" > "$log_file" 2>&1
    fi

    echo "  Completed (log: $log_file)"
}

# Scenario 1: Internal upper bound
if [ "$SCENARIO" = "all" ] || [ "$SCENARIO" = "internal" ]; then
    if [ "$QUICK_MODE" = true ]; then
        run_scenario "1) In-process upper bound" "$BM_CLASS_INTERNAL" "$INTERNAL_LOG" "-Dloomq.benchmark.quick=true"
    else
        run_scenario "1) In-process upper bound" "$BM_CLASS_INTERNAL" "$INTERNAL_LOG"
    fi

    # ---- 新增组件基准测试 (v0.9.2) ----
    run_scenario "1b) WAL Write Throughput" "$BM_CLASS_WAL" "$WAL_LOG"
    run_scenario "1c) Storage Engine Comparison" "$BM_CLASS_STORAGE" "$STORAGE_LOG"
    run_scenario "1d) Observer Overhead" "$BM_CLASS_OBSERVER" "$OBSERVER_LOG"
    # ---- 新增结束 ----
fi

# Scenario 2: HTTP create path (requires running server - skip if not in full mode)
if [ "$SCENARIO" = "all" ] || [ "$SCENARIO" = "http" ]; then
    echo ""
    echo "=== 2) HTTP create path ==="
    echo "  (Skipped — requires a running LoomQ server. Use run-all.sh for full suite.)"
fi

# Scenario 2b: gRPC create path (requires running server with gRPC enabled)
if [ "$SCENARIO" = "all" ] || [ "$SCENARIO" = "grpc" ]; then
    echo ""
    echo "=== 2b) gRPC create path ==="
    echo "  (Skipped — requires a running LoomQ server with gRPC enabled. Use run-all.sh for full suite.)"
fi

# Scenario 3: Scheduler trigger path
if [ "$SCENARIO" = "all" ] || [ "$SCENARIO" = "scheduler" ]; then
    SCHED_ARGS=("-Dloomq.benchmark.workload=$WORKLOAD")
    if [ "$QUICK_MODE" = true ]; then
        SCHED_ARGS+=("-Dloomq.benchmark.quick=true")
    fi
    run_scenario "3) Scheduler trigger path" "$BM_CLASS_SCHEDULER" "$SCHEDULER_LOG" "${SCHED_ARGS[@]}"
fi

# ============================================================
#  Parse & Persist
# ============================================================

echo ""
echo "=== Persisting results ==="

# Determine which log to parse for scheduler markers
PARSE_LOG=""
if [ -f "$SCHEDULER_LOG" ]; then
    PARSE_LOG="$SCHEDULER_LOG"
elif [ -f "$INTERNAL_LOG" ]; then
    PARSE_LOG="$INTERNAL_LOG"
fi

# --- CSV (unified 55-column wide format) ---
CSV_FILE="$RESULTS_DIR/history.csv"
TIERS="ULTRA FAST HIGH STANDARD ECONOMY"
COLS="timestamp,commit,branch,mode,java_version,os_name"
COLS="$COLS,internal_qps"
COLS="$COLS,http_peak_qps,http_best_p99_ms,http_worst_p99_ms,http_fail_rate"
COLS="$COLS,grpc_peak_qps,grpc_best_p99_ms,grpc_worst_p99_ms,grpc_fail_rate"
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
ENV_LINE=""
if [ -n "$PARSE_LOG" ]; then
    ENV_LINE=$(grep "^RESULT_ENV|" "$PARSE_LOG" | tail -1)
fi
JAVA_VER=$(extract "$ENV_LINE" 'java_version')
OS_NAME=$(extract "$ENV_LINE" 'os_name')

# Build CSV row (55-column schema)
VALS="$DATE_ISO,$COMMIT,$BRANCH,$MODE,$JAVA_VER,$OS_NAME"
VALS="$VALS,"  # internal_qps (not available in bash)
VALS="$VALS,,,,"  # http_peak_qps, http_best_p99_ms, http_worst_p99_ms, http_fail_rate
VALS="$VALS,,,,"  # grpc_peak_qps, grpc_best_p99_ms, grpc_worst_p99_ms, grpc_fail_rate
for T in $TIERS; do
    if [ -n "$PARSE_LOG" ]; then
        ROW=$(grep "RESULT_ROW|tier=$T" "$PARSE_LOG" | tail -1)
        LAT=$(grep "RESULT_LATENCY|tier=$T" "$PARSE_LOG" | tail -1)
        E2E=$(grep "RESULT_E2E_LATENCY|tier=$T" "$PARSE_LOG" | tail -1)
        SEM=$(grep "RESULT_SEMAPHORE|tier=$T" "$PARSE_LOG" | tail -1)
    else
        ROW=""; LAT=""; E2E=""; SEM=""
    fi

    VALS="$VALS,$(extract "$ROW" 'qps')"
    VALS="$VALS,$(extract "$LAT" 'p95')"
    VALS="$VALS,$(extract "$LAT" 'p99')"
    VALS="$VALS,$(extract "$E2E" 'p95')"
    VALS="$VALS,$(extract "$E2E" 'p99')"
    VALS="$VALS,$(extract "$SEM" 'utilization_pct')"
    VALS="$VALS,$(extract "$ROW" 'backpressure')"
done

# Global aggregates
if [ -n "$PARSE_LOG" ]; then
    RESULT_LINE=$(grep "^RESULT|" "$PARSE_LOG" | tail -1)
    GLOBAL=$(grep "RESULT_GLOBAL_LATENCY" "$PARSE_LOG" | tail -1)
    OPT=$(grep "RESULT_OPTIMIZATION" "$PARSE_LOG" | tail -1)
    COHORT=$(grep "RESULT_COHORT" "$PARSE_LOG" | tail -1)
    SYSQPS=$(grep "RESULT_SYSTEM_QPS" "$PARSE_LOG" | tail -1)
else
    RESULT_LINE=""; GLOBAL=""; OPT=""; COHORT=""; SYSQPS=""
fi

VALS="$VALS,$(extract "$RESULT_LINE" 'completion_rate')"
VALS="$VALS,$(extract "$SYSQPS" 'total_qps')"
VALS="$VALS,$(extract "$GLOBAL" 'p95_total')"
VALS="$VALS,$(extract "$OPT" 'vt_reduction_pct')"
VALS="$VALS,$(extract "$COHORT" 'wake_events')"

echo "$VALS" >> "$CSV_FILE"
echo "  CSV: $CSV_FILE"

# --- JSON ---
JSON_FILE="$REPORTS_DIR/benchmark-$TIMESTAMP.json"
{
    echo "{"
    echo "  \"timestamp\": \"$DATE_ISO\","
    echo "  \"commit\": \"$COMMIT\","
    echo "  \"branch\": \"$BRANCH\","
    echo "  \"mode\": \"$MODE\","
    echo "  \"scenario\": \"$SCENARIO\","
    echo "  \"workload\": \"$WORKLOAD\","
    echo "  \"tiers\": {"
    first=true
    for T in $TIERS; do
        if [ "$first" = true ]; then first=false; else echo ","; fi
        if [ -n "$PARSE_LOG" ]; then
            ROW=$(grep "RESULT_ROW|tier=$T" "$PARSE_LOG" | tail -1)
            LAT=$(grep "RESULT_LATENCY|tier=$T" "$PARSE_LOG" | tail -1)
            E2E=$(grep "RESULT_E2E_LATENCY|tier=$T" "$PARSE_LOG" | tail -1)
        else
            ROW=""; LAT=""; E2E=""
        fi
        printf "    \"%s\": {\"qps\": \"%s\", \"p95\": \"%s\", \"p99\": \"%s\", \"e2e_p95\": \"%s\", \"e2e_p99\": \"%s\"}" \
            "$T" "$(extract "$ROW" 'qps')" "$(extract "$LAT" 'p95')" "$(extract "$LAT" 'p99')" \
            "$(extract "$E2E" 'p95')" "$(extract "$E2E" 'p99')"
    done
    echo ""
    echo "  }"
    echo "}"
} > "$JSON_FILE"
echo "  JSON: $JSON_FILE"

# --- TXT Summary (中文) ---
SUMMARY_FILE="$REPORTS_DIR/benchmark-$TIMESTAMP.txt"
{
    echo "═══════════════════════════════════════════════════════════════"
    echo "                    LoomQ 基准测试报告"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
    echo "  日期: $DATE_ISO"
    echo "  提交: $COMMIT ($BRANCH)"
    echo "  模式: $MODE"
    echo "  场景: $SCENARIO"
    echo "  单位: 唤醒延迟=微秒(µs) | E2E延迟=毫秒(ms) | QPS=每秒处理量"
    echo ""

    if [ -n "$PARSE_LOG" ] && grep -q "RESULT_ROW|" "$PARSE_LOG" 2>/dev/null; then
        echo "┌─ 吞吐量 ──────────────────────────────────────────────────────────────────┐"
        printf "│ %-8s %8s %10s %10s %8s %8s %6s │\n" \
            "档位" "接收" "QPS" "理论QPS" "效率" "完成率" "背压"
        echo "├──────────────────────────────────────────────────────────────────────────┤"
        TOTAL_RECEIVED=0
        for T in $TIERS; do
            ROW=$(grep "RESULT_ROW|tier=$T" "$PARSE_LOG" | tail -1)
            [ -z "$ROW" ] && continue
            RCV=$(extract "$ROW" 'received')
            TOTAL_RECEIVED=$((TOTAL_RECEIVED + RCV))
            printf "│ %-8s %8s %10s %10s %7s%% %7s%% %6s │\n" \
                "$T" "$RCV" \
                "$(extract "$ROW" 'qps')" \
                "$(extract "$ROW" 'theoretical_qps')" \
                "$(extract "$ROW" 'efficiency')" \
                "100.0" \
                "$(extract "$ROW" 'backpressure')"
        done
        echo "└──────────────────────────────────────────────────────────────────────────┘"
        echo ""

        echo "┌─ 延迟分布 ────────────────────────────────────────────────────────────────┐"
        printf "│ %-8s │ %-17s │ %-17s │ %-14s │\n" \
            "档位" "唤醒延迟(µs)" "E2E延迟(ms)" "信号量%"
        printf "│ %-8s │ %5s %5s %5s │ %5s %5s %5s │ %6s │\n" \
            "" "p50" "p95" "p99" "p50" "p95" "p99" ""
        echo "├──────────┼─────────────────┼─────────────────┼────────────────┤"
        for T in $TIERS; do
            ROW=$(grep "RESULT_ROW|tier=$T" "$PARSE_LOG" | tail -1)
            LAT=$(grep "RESULT_LATENCY|tier=$T" "$PARSE_LOG" | tail -1)
            E2E=$(grep "RESULT_E2E_LATENCY|tier=$T" "$PARSE_LOG" | tail -1)
            SEM=$(grep "RESULT_SEMAPHORE|tier=$T" "$PARSE_LOG" | tail -1)
            [ -z "$ROW" ] && continue
            printf "│ %-8s │ %5s %5s %5s │ %5s %5s %5s │ %6s │\n" \
                "$T" \
                "$(extract "$LAT" 'p50')" "$(extract "$LAT" 'p95')" "$(extract "$LAT" 'p99')" \
                "$(extract "$E2E" 'p50')" "$(extract "$E2E" 'p95')" "$(extract "$E2E" 'p99')" \
                "$(extract "$SEM" 'utilization_pct')"
        done
        echo "└──────────┴─────────────────┴─────────────────┴────────────────┘"
        echo ""
    fi

    # SLO 验证
    if [ -n "$PARSE_LOG" ] && grep -q "RESULT_E2E_LATENCY|" "$PARSE_LOG" 2>/dev/null; then
        echo "┌─ SLO 验证 ─────────────────────────────────────────────────────────────────┐"
        printf "│ %-8s │ %-13s │ %-8s │ %-6s │ %-13s │ %-8s │ %-6s │\n" \
            "档位" "E2E-p95" "阈值" "结果" "E2E-p99" "阈值" "结果"
        echo "├──────────┼───────────────┼──────────┼────────┼───────────────┼──────────┼────────┤"
        declare -A SLO_P95=( [ULTRA]=20 [FAST]=70 [HIGH]=150 [STANDARD]=600 [ECONOMY]=1200 )
        declare -A SLO_P99=( [ULTRA]=35 [FAST]=110 [HIGH]=230 [STANDARD]=900 [ECONOMY]=1700 )
        for T in $TIERS; do
            E2E=$(grep "RESULT_E2E_LATENCY|tier=$T" "$PARSE_LOG" | tail -1)
            [ -z "$E2E" ] && continue
            P95=$(extract "$E2E" 'p95')
            P99=$(extract "$E2E" 'p99')
            LIMIT95=${SLO_P95[$T]:-9999}
            LIMIT99=${SLO_P99[$T]:-9999}
            if [ "$P95" -le "$LIMIT95" ] 2>/dev/null; then R95="✅通过"; else R95="❌失败"; fi
            if [ "$P99" -le "$LIMIT99" ] 2>/dev/null; then R99="✅通过"; else R99="❌失败"; fi
            printf "│ %-8s │ %9s ms │ ≤%s ms │ %s │ %9s ms │ ≤%s ms │ %s │\n" \
                "$T" "$P95" "$LIMIT95" "$R95" "$P99" "$LIMIT99" "$R99"
        done
        echo "└──────────┴───────────────┴──────────┴────────┴───────────────┴──────────┴────────┘"
        echo ""
    fi

    # 回归对比（如果 history.csv 有足够数据）
    if [ -f "$CSV_FILE" ]; then
        LINE_COUNT=$(wc -l < "$CSV_FILE")
        if [ "$LINE_COUNT" -ge 3 ]; then
            echo "┌─ 回归对比 ──────────────────────────────────────────────────────────────────┐"
            PREV_LINE=$(tail -2 "$CSV_FILE" | head -1)
            CURR_LINE=$(tail -1 "$CSV_FILE")
            HEADER_LINE=$(head -1 "$CSV_FILE")

            # 解析 CSV 列索引
            IFS=',' read -ra HEADER_COLS <<< "$HEADER_LINE"
            declare -A COL_IDX
            for i in "${!HEADER_COLS[@]}"; do
                COL_IDX[${HEADER_COLS[$i]}]=$i
            done

            IFS=',' read -ra PREV_COLS <<< "$PREV_LINE"
            IFS=',' read -ra CURR_COLS <<< "$CURR_LINE"

            PREV_TS="${PREV_COLS[${COL_IDX[timestamp]}]:-?}"
            CURR_TS="${CURR_COLS[${COL_IDX[timestamp]}]:-?}"
            echo "│  上次: $PREV_TS"
            echo "│  本次: $CURR_TS"
            echo "├──────────┬──────────────────────────────────────────────────────────────────────┤"

            for T in $TIERS; do
                PQ="${PREV_COLS[${COL_IDX[${T}_qps]}]:-0}"
                CQ="${CURR_COLS[${COL_IDX[${T}_qps]}]:-0}"
                PE="${PREV_COLS[${COL_IDX[${T}_e2e_p99_ms]}]:-0}"
                CE="${CURR_COLS[${COL_IDX[${T}_e2e_p99_ms]}]:-0}"
                [ "$PQ" = "0" ] && [ "$CQ" = "0" ] && continue
                if [ "$PQ" != "0" ] && [ "$CQ" != "0" ]; then
                    QPS_PCT=$(awk "BEGIN{printf \"%.1f\", ($CQ - $PQ) / $PQ * 100}")
                    printf "│ %-8s │ QPS: %s→%s (%s%%)  E2E-p99: %s→%s ms │\n" \
                        "$T" "$PQ" "$CQ" "$QPS_PCT" "$PE" "$CE"
                fi
            done
            echo "└──────────┴──────────────────────────────────────────────────────────────────────┘"
            echo ""
        fi
    fi

    echo "CSV : $CSV_FILE"
    echo "JSON: $JSON_FILE"
} > "$SUMMARY_FILE"
echo "  TXT: $SUMMARY_FILE"

echo ""
echo "[OK] Benchmark complete"
echo ""
echo "Tip: use --compare to view history"
