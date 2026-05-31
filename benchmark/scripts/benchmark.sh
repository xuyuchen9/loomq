#!/bin/bash
# LoomQ 性能基准测试 (Linux/macOS)
#
# 用法:
#   ./benchmark.sh                    # 全量测试
#   ./benchmark.sh --quick            # 快速测试
#   ./benchmark.sh --stress           # 全量 + 压力测试
#   ./benchmark.sh --stress-only=grpc # 仅 gRPC 压力测试
#   ./benchmark.sh --scenario=internal # 仅内部组件
#   ./benchmark.sh --compare          # 查看上次报告
#
# 输出:
#   Excel: benchmark/results/reports/benchmark-report-{timestamp}.xlsx
#   MD:    benchmark/results/reports/benchmark-report-{timestamp}.md

set -euo pipefail

# ============================================================
#  参数解析
# ============================================================

QUICK_MODE=false
STRESS=false
STRESS_ONLY=""
SCENARIO="all"
GRPC_TIER=""
NO_COMPILE=false
COMPARE_ONLY=false
VERBOSE=false

for arg in "$@"; do
    case $arg in
        --quick)           QUICK_MODE=true ;;
        --stress)          STRESS=true ;;
        --stress-only=*)   STRESS_ONLY="${arg#*=}"; STRESS=true ;;
        --scenario=*)      SCENARIO="${arg#*=}" ;;
        --grpc-tier=*)     GRPC_TIER="${arg#*=}" ;;
        --no-compile)      NO_COMPILE=true ;;
        --compare)         COMPARE_ONLY=true ;;
        --verbose)         VERBOSE=true ;;
        --help|-h)
            echo "LoomQ 性能基准测试"
            echo ""
            echo "用法: ./benchmark.sh [选项]"
            echo ""
            echo "选项:"
            echo "  --quick              快速测试模式"
            echo "  --stress             包含压力测试"
            echo "  --stress-only=NAME   仅运行压力测试: http / grpc"
            echo "  --scenario=NAME      指定场景: internal / create / scheduler / all"
            echo "  --grpc-tier=TIER     gRPC 精度档位: ULTRA/FAST/HIGH/STANDARD/ECONOMY"
            echo "  --no-compile         跳过编译"
            echo "  --compare            查看上次报告对比"
            echo "  --verbose            显示完整日志"
            echo "  --help               显示帮助"
            echo ""
            echo "输出:"
            echo "  Excel: benchmark/results/reports/benchmark-report-{时间戳}.xlsx"
            echo "  MD:    benchmark/results/reports/benchmark-report-{时间戳}.md"
            exit 0
            ;;
        *)
            echo "未知选项: $arg"
            echo "使用 --help 查看帮助。"
            exit 1
            ;;
    esac
done

# ============================================================
#  环境初始化
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/benchmark/results"
REPORTS_DIR="$RESULTS_DIR/reports"
LOGS_DIR="$RESULTS_DIR/logs"
SERVER_DIR="$PROJECT_ROOT/loomq-server"

mkdir -p "$REPORTS_DIR" "$LOGS_DIR"

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
DATE_ISO=$(date +%Y-%m-%dT%H:%M:%S)
COMMIT=$(git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")
BRANCH=$(git -C "$PROJECT_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
JAVA_VER=$(java -version 2>&1 | head -1 || echo "unknown")
OS_NAME=$(uname -srm)
CPU_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "?")

# Banner
echo ""
echo "+============================================================+"
echo "|          LoomQ 性能基准测试                                 |"
echo "+============================================================+"
echo ""
echo "  Commit:   $COMMIT ($BRANCH)"
echo "  Java:     $JAVA_VER"
echo "  OS:       $OS_NAME"
echo "  CPU:      $CPU_CORES cores"
echo "  Scenario: $SCENARIO"
echo "  Mode:     $($QUICK_MODE && echo "quick" || echo "full")"
if [ "$STRESS" = true ]; then
    echo "  Stress:   enabled ($STRESS_ONLY)"
fi
echo ""

# ============================================================
#  工具函数
# ============================================================

extract() { echo "$1" | sed -n "s/.*${2}=\([^|]*\).*/\1/p"; }

rotate_reports() {
    local keep=10
    local count
    count=$(ls -1t "$REPORTS_DIR"/*.xlsx "$REPORTS_DIR"/*.md 2>/dev/null | wc -l || true)
    if [ "$count" -gt "$((keep * 2))" ]; then
        ls -1t "$REPORTS_DIR"/*.xlsx "$REPORTS_DIR"/*.md 2>/dev/null | tail -n +"$((keep * 2 + 1))" | xargs rm -f -- || true
    fi
    count=$(ls -1t "$LOGS_DIR"/*.log 2>/dev/null | wc -l || true)
    if [ "$count" -gt "$keep" ]; then
        ls -1t "$LOGS_DIR"/*.log 2>/dev/null | tail -n +"$((keep + 1))" | xargs rm -f -- || true
    fi
}

# Scenario timeout (seconds)
get_timeout() {
    local name=$1
    local quick=$2
    case $name in
        internal)  $quick && echo 120 || echo 240 ;;
        wal)       $quick && echo 60  || echo 120 ;;
        storage)   $quick && echo 60  || echo 120 ;;
        http|grpc) $quick && echo 180 || echo 420 ;;
        scheduler) $quick && echo 180 || echo 420 ;;
        *)         echo 300 ;;
    esac
}

# Run a benchmark scenario via mvn exec:java
# Args: name, main_class, timeout_sec, extra_mvn_args...
run_scenario() {
    local name="$1"
    local main_class="$2"
    local timeout_sec="$3"
    shift 3
    local extra_args=("$@")

    echo ""
    echo "=== $name ==="

    local log_file="$LOGS_DIR/scenario-$(echo "$main_class" | tr '.' '_')-$TIMESTAMP.log"

    local mvn_args=(
        exec:java
        "-Dexec.mainClass=$main_class"
        -Dexec.classpathScope=test
        "-Dmaven.repo.local=$M2_REPO"
        -q
    )
    if [ "$QUICK_MODE" = true ]; then
        mvn_args+=("-Dloomq.benchmark.quick=true")
    fi
    for a in "${extra_args[@]}"; do
        mvn_args+=("$a")
    done

    local start_time=$SECONDS
    if [ "$VERBOSE" = true ]; then
        mvn -f "$SERVER_DIR/pom.xml" "${mvn_args[@]}" 2>&1 | tee "$log_file" &
    else
        mvn -f "$SERVER_DIR/pom.xml" "${mvn_args[@]}" > "$log_file" 2>&1 &
    fi
    local mvn_pid=$!

    # Timeout monitoring
    while kill -0 "$mvn_pid" 2>/dev/null; do
        local elapsed=$((SECONDS - start_time))
        if [ $elapsed -ge "$timeout_sec" ]; then
            echo "  TIMEOUT after ${timeout_sec}s, killing..."
            kill -- -$mvn_pid 2>/dev/null || kill "$mvn_pid" 2>/dev/null
            wait "$mvn_pid" 2>/dev/null
            return 1
        fi
        sleep 2
    done
    wait "$mvn_pid"
    local rc=$?

    local elapsed=$((SECONDS - start_time))
    if [ $rc -ne 0 ]; then
        echo "  FAILED (exit $rc, ${elapsed}s)"
        return $rc
    fi

    # Print RESULT markers
    grep "^RESULT|" "$log_file" 2>/dev/null | tail -1 | while read -r line; do
        echo "  $line"
    done

    echo "  OK (${elapsed}s)"
    return 0
}

# ============================================================
#  Compare mode
# ============================================================

if [ "$COMPARE_ONLY" = true ]; then
    LATEST_MD=$(ls -1t "$REPORTS_DIR"/benchmark-report-*.md 2>/dev/null | head -1)
    if [ -n "$LATEST_MD" ]; then
        echo "=== 最新报告: $LATEST_MD ==="
        echo ""
        cat "$LATEST_MD"
    else
        echo "未找到报告。请先运行一次基准测试。"
    fi
    exit 0
fi

# ============================================================
#  Compile
# ============================================================

M2_REPO="$RESULTS_DIR/m2repo"
mkdir -p "$M2_REPO"

if [ "$NO_COMPILE" = false ]; then
    echo ">>> 编译项目..."
    mvn -f "$PROJECT_ROOT/pom.xml" compile test-compile -Dmaven.repo.local="$M2_REPO" -q 2>&1 | tail -5
    if [ ${PIPESTATUS[0]} -ne 0 ]; then
        echo "[ERROR] 编译失败"
        exit 1
    fi
    echo "  编译完成"
fi

rotate_reports

# ============================================================
#  场景过滤
# ============================================================

RUN_INTERNAL=false
RUN_CREATE=false
RUN_SCHEDULER=false

if [ -z "$STRESS_ONLY" ]; then
    case $SCENARIO in
        all)       RUN_INTERNAL=true; RUN_CREATE=true; RUN_SCHEDULER=true ;;
        internal)  RUN_INTERNAL=true ;;
        create)    RUN_CREATE=true ;;
        scheduler) RUN_SCHEDULER=true ;;
    esac
fi

# Stress-only mode: only run stress scenarios
if [ -n "$STRESS_ONLY" ]; then
    RUN_CREATE=true
fi

# ============================================================
#  Phase 1: Internal (no server)
# ============================================================

INTERNAL_LOG=""; WAL_LOG=""; STORAGE_LOG=""

if [ "$RUN_INTERNAL" = true ]; then
    TIMEOUT=$(get_timeout "internal" $QUICK_MODE)
    INTERNAL_LOG="$LOGS_DIR/scenario-com_loomq_benchmark_InternalBenchmark-$TIMESTAMP.log"
    run_scenario "1) IntentStore & Memory" "com.loomq.benchmark.InternalBenchmark" "$TIMEOUT"

    TIMEOUT=$(get_timeout "wal" $QUICK_MODE)
    WAL_LOG="$LOGS_DIR/scenario-com_loomq_benchmark_WalThroughputBenchmark-$TIMESTAMP.log"
    run_scenario "1b) WAL Write Throughput" "com.loomq.benchmark.WalThroughputBenchmark" "$TIMEOUT"

    TIMEOUT=$(get_timeout "storage" $QUICK_MODE)
    STORAGE_LOG="$LOGS_DIR/scenario-com_loomq_benchmark_StorageBenchmark-$TIMESTAMP.log"
    run_scenario "1c) Storage Engine Comparison" "com.loomq.benchmark.StorageBenchmark" "$TIMEOUT"
fi

# ============================================================
#  Phase 2: Server-based tests
# ============================================================

HTTP_LOG=""; GRPC_LOG=""; SCHEDULER_LOG=""
SERVER_PID=""

cleanup_server() {
    if [ -n "$SERVER_PID" ]; then
        echo "  Stopping server (PID: $SERVER_PID)..."
        kill -- -$SERVER_PID 2>/dev/null || kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    fi
}
trap cleanup_server EXIT

if [ "$RUN_CREATE" = true ] || [ "$RUN_SCHEDULER" = true ] || [ "$STRESS" = true ]; then
    echo ""
    echo ">>> 冷却 30s..."
    sleep 30

    # Find server JAR
    SERVER_JAR=$(find "$SERVER_DIR/target" -name "loomq-server-*.jar" -not -name "*-sources.jar" -not -name "original-*" 2>/dev/null | head -1)
    if [ -z "$SERVER_JAR" ]; then
        echo "[ERROR] Server JAR not found. Build first: mvn package -DskipTests"
        exit 1
    fi

    # Find free port
    TEMP_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()" 2>/dev/null || echo 8080)
    GRPC_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()" 2>/dev/null || echo 7929)
    DATA_DIR="$RESULTS_DIR/runtime/data-$TIMESTAMP"
    mkdir -p "$DATA_DIR"

    echo ">>> Starting server on port $TEMP_PORT (gRPC: $GRPC_PORT)..."
    java -Dserver.port="$TEMP_PORT" \
        -Dgrpc.enabled=true \
        -Dgrpc.port="$GRPC_PORT" \
        -Ddata.dir="$DATA_DIR" \
        -jar "$SERVER_JAR" \
        > "$LOGS_DIR/server-$TIMESTAMP.log" 2>&1 &
    SERVER_PID=$!

    # Wait for server
    echo "  Waiting for server..."
    for i in $(seq 1 60); do
        if curl -s "http://127.0.0.1:$TEMP_PORT/health" > /dev/null 2>&1; then
            echo "  Server ready"
            break
        fi
        if [ $i -eq 60 ]; then
            echo "[ERROR] Server failed to start"
            exit 1
        fi
        sleep 2
    done

    BASE_URL="http://127.0.0.1:$TEMP_PORT"

    # HTTP Create Path
        if [ "$RUN_CREATE" = true ] && [ -z "$STRESS_ONLY" -o "$STRESS_ONLY" = "http" ]; then
            TIMEOUT=$(get_timeout "http" $QUICK_MODE)
            HTTP_LOG="$LOGS_DIR/scenario-com_loomq_benchmark_HttpVirtualThreadBenchmark-$TIMESTAMP.log"
            run_scenario "2) HTTP Create Path" "com.loomq.benchmark.HttpVirtualThreadBenchmark" "$TIMEOUT" \
                "-Dloomq.benchmark.baseUrl=$BASE_URL"
        fi

        # gRPC Create Path
        if [ "$RUN_CREATE" = true ] && [ -z "$STRESS_ONLY" -o "$STRESS_ONLY" = "grpc" ]; then
            echo ">>> 冷却 15s..."
            sleep 15
            TIMEOUT=$(get_timeout "grpc" $QUICK_MODE)
            GRPC_ARGS=("-Dloomq.benchmark.grpc.host=127.0.0.1" "-Dloomq.benchmark.grpc.port=$GRPC_PORT")
            if [ -n "$GRPC_TIER" ]; then
                GRPC_ARGS+=("-Dloomq.benchmark.grpc.tier=$GRPC_TIER")
            fi
            GRPC_LOG="$LOGS_DIR/scenario-com_loomq_benchmark_GrpcVirtualThreadBenchmark-$TIMESTAMP.log"
            run_scenario "2b) gRPC Create Path" "com.loomq.benchmark.GrpcVirtualThreadBenchmark" "$TIMEOUT" "${GRPC_ARGS[@]}"
        fi

        # Scheduler Trigger
        if [ "$RUN_SCHEDULER" = true ]; then
            echo ">>> 冷却 15s..."
            sleep 15
            TIMEOUT=$(get_timeout "scheduler" $QUICK_MODE)
            SCHEDULER_LOG="$LOGS_DIR/scenario-com_loomq_scheduler_SchedulerTriggerBenchmarkWithMockServer-$TIMESTAMP.log"
            run_scenario "3) Scheduler Trigger" "com.loomq.scheduler.SchedulerTriggerBenchmarkWithMockServer" "$TIMEOUT"
        fi

        # Stress test
        if [ "$STRESS" = true ]; then
            echo ""
            echo "+============================================================+"
            echo "|  STRESS MODE                                               |"
            echo "+============================================================+"
            echo ""
            echo ">>> 冷却 30s..."
            sleep 30

            STRESS_ARGS=(
                "-Dloomq.benchmark.stress=true"
                "-Dloomq.benchmark.duration_sec=30"
                "-Dloomq.benchmark.cooldown_ms=5000"
            )
            if [ -n "$GRPC_TIER" ]; then
                STRESS_ARGS+=("-Dloomq.benchmark.grpc.tier=$GRPC_TIER")
            fi

            if [ -z "$STRESS_ONLY" ] || [ "$STRESS_ONLY" = "http" ]; then
                run_scenario "2c) HTTP Stress" "com.loomq.benchmark.HttpVirtualThreadBenchmark" 600 \
                    "-Dloomq.benchmark.baseUrl=$BASE_URL" "${STRESS_ARGS[@]}"
                echo ">>> 冷却 15s..."
                sleep 15
            fi

            if [ -z "$STRESS_ONLY" ] || [ "$STRESS_ONLY" = "grpc" ]; then
                run_scenario "2d) gRPC Stress" "com.loomq.benchmark.GrpcVirtualThreadBenchmark" 600 \
                    "-Dloomq.benchmark.grpc.host=127.0.0.1" "-Dloomq.benchmark.grpc.port=$GRPC_PORT" "${STRESS_ARGS[@]}"
            fi
        fi
fi

# ============================================================
#  生成报告
# ============================================================

echo ""
echo "=== 生成报告 ==="

# 构建 JSON 中间文件
JSON_FILE="$REPORTS_DIR/benchmark-$TIMESTAMP.json"

# 解析所有 RESULT markers
parse_result() {
    local log="$1"
    [ -f "$log" ] && grep "^RESULT|" "$log" | tail -1 || echo ""
}

INTERNAL_RESULT=$(parse_result "$INTERNAL_LOG")
WAL_RESULT=$(parse_result "$WAL_LOG")
STORAGE_RESULT=$(parse_result "$STORAGE_LOG")
HTTP_RESULT=$(parse_result "$HTTP_LOG")
GRPC_RESULT=$(parse_result "$GRPC_LOG")
SCHED_RESULT=$(parse_result "$SCHEDULER_LOG")

# 构建 JSON（Python 辅助）
python3 - "$TIMESTAMP" "$DATE_ISO" "$COMMIT" "$BRANCH" "$JAVA_VER" "$OS_NAME" "$CPU_CORES" \
    "$INTERNAL_LOG" "$WAL_LOG" "$STORAGE_LOG" "$HTTP_LOG" "$GRPC_LOG" "$SCHEDULER_LOG" \
    "$JSON_FILE" << 'PYEOF'
import json, sys, os, re

timestamp, date_iso, commit, branch, java_ver, os_name, cpu_cores = sys.argv[1:8]
internal_log, wal_log, storage_log, http_log, grpc_log, sched_log = sys.argv[8:14]
json_file = sys.argv[14]

def parse_markers(log_path):
    """Parse all RESULT| and RESULT_ROW| markers from a log file."""
    results = []
    rows = []
    if not os.path.exists(log_path):
        return results, rows
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line.startswith("RESULT_ROW|"):
                d = {}
                for part in line.split("|")[1:]:
                    if "=" in part:
                        k, v = part.split("=", 1)
                        try: v = float(v) if "." in v else int(v)
                        except: pass
                        d[k] = v
                rows.append(d)
            elif line.startswith("RESULT|"):
                d = {}
                for part in line.split("|")[1:]:
                    if "=" in part:
                        k, v = part.split("=", 1)
                        try: v = float(v) if "." in v else int(v)
                        except: pass
                        d[k] = v
                results.append(d)
    return results, rows

def get_val(results, key, default=None):
    for r in results:
        if key in r:
            return r[key]
    return default

# Parse all logs
int_res, int_rows = parse_markers(internal_log)
wal_res, wal_rows = parse_markers(wal_log)
sto_res, sto_rows = parse_markers(storage_log)
http_res, http_rows = parse_markers(http_log)
grpc_res, grpc_rows = parse_markers(grpc_log)
sched_res, sched_rows = parse_markers(sched_log)

# Environment
env_data = {
    "timestamp": date_iso, "commit": commit, "branch": branch,
    "java_version": java_ver, "os": os_name, "cpu_cores": cpu_cores,
    "max_memory": "N/A"
}

# Create Path rows
cp_rows = []
max_len = max(len(http_rows), len(grpc_rows), 1)
for i in range(max_len):
    hr = http_rows[i] if i < len(http_rows) else {}
    gr = grpc_rows[i] if i < len(grpc_rows) else {}
    cp_rows.append({
        "threads": hr.get("threads", gr.get("threads")),
        "http_qps": hr.get("qps"), "http_p50": hr.get("p50_ms"), "http_p90": hr.get("p90_ms"), "http_p99": hr.get("p99_ms"),
        "grpc_qps": gr.get("qps"), "grpc_p50": gr.get("p50_ms"), "grpc_p90": gr.get("p90_ms"), "grpc_p99": gr.get("p99_ms"),
    })

# Summary
http_peak = get_val(http_res, "peak_qps")
grpc_peak = get_val(grpc_res, "peak_qps")
summary = {
    "http_peak_qps": http_peak, "grpc_peak_qps": grpc_peak,
    "http_inflection_threads": get_val(http_res, "inflection_threads"),
    "grpc_inflection_threads": get_val(grpc_res, "inflection_threads"),
    "http_fail_rate": get_val(http_res, "fail_rate"),
    "grpc_fail_rate": get_val(grpc_res, "fail_rate"),
    "memory_per_intent": get_val(int_res, "memory_bytes_per_intent"),
}

# Scheduler tiers
tier_data = []
for row in sched_rows:
    tier_name = row.get("tier")
    if tier_name:
        tier_data.append({
            "tier": tier_name, "concurrency": row.get("concurrency"),
            "qps": row.get("qps"),
            "wake_p50": row.get("wake_p50_us"), "wake_p95": row.get("wake_p95_us"), "wake_p99": row.get("wake_p99_us"),
            "e2e_p50": row.get("e2e_p50_ms"), "e2e_p95": row.get("e2e_p95_ms"), "e2e_p99": row.get("e2e_p99_ms"),
            "util_pct": row.get("semaphore_util_pct"), "backpressure_pct": row.get("backpressure_pct"),
            "completion_pct": row.get("completion_rate"),
        })

# Internal
internal_items = []
if int_res:
    internal_items.append({"benchmark": "IntentStore", "metric": "Write QPS", "value": get_val(int_res, "direct_store_qps")})
    internal_items.append({"benchmark": "Memory", "metric": "bytes/intent", "value": get_val(int_res, "memory_bytes_per_intent")})

# WAL results - parse mode-specific entries
wal_durable = None
wal_async = None
for r in wal_res:
    if r.get("mode") == "DURABLE":
        wal_durable = r.get("throughput")
    elif r.get("mode") == "ASYNC":
        wal_async = r.get("throughput")
if wal_durable:
    internal_items.append({"benchmark": "WAL DURABLE", "metric": "ops/s", "value": wal_durable})
if wal_async:
    internal_items.append({"benchmark": "WAL ASYNC", "metric": "ops/s", "value": wal_async})

# Storage results - parse type-specific entries
sto_concurrent = None
sto_rocksdb = None
for r in sto_res:
    if r.get("type") == "concurrent":
        sto_concurrent = r.get("save_ms")
    elif r.get("type") == "rocksdb":
        sto_rocksdb = r.get("save_ms")
if sto_concurrent:
    internal_items.append({"benchmark": "Storage InMem", "metric": "save_ms", "value": sto_concurrent})
if sto_rocksdb:
    internal_items.append({"benchmark": "Storage RocksDB", "metric": "save_ms", "value": sto_rocksdb})

data = {
    "timestamp": timestamp, "environment": env_data, "summary": summary,
    "create_path": {"rows": cp_rows}, "scheduler": {"tiers": tier_data},
    "internal": {"items": internal_items}, "slo": {"items": []}, "regression": None
}

with open(json_file, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
print(f"JSON: {json_file}")
PYEOF

# Generate Excel
PYTHON_SCRIPT="$SCRIPT_DIR/lib/gen_excel.py"
EXCEL_FILE="$REPORTS_DIR/benchmark-report-$TIMESTAMP.xlsx"
if [ -f "$PYTHON_SCRIPT" ]; then
    python3 "$PYTHON_SCRIPT" "$JSON_FILE" "$EXCEL_FILE" 2>&1 && echo "  Excel: $EXCEL_FILE" || echo "  [WARN] Excel 生成失败"
else
    echo "  [WARN] gen_excel.py 不存在，跳过 Excel"
fi

# Generate MD
MD_FILE="$REPORTS_DIR/benchmark-report-$TIMESTAMP.md"
python3 - "$JSON_FILE" "$MD_FILE" << 'PYEOF'
import json, sys
json_file, md_file = sys.argv[1], sys.argv[2]
with open(json_file, "r", encoding="utf-8") as f:
    data = json.load(f)

env = data.get("environment", {})
s = data.get("summary", {})
cp = data.get("create_path", {}).get("rows", [])
tiers = data.get("scheduler", {}).get("tiers", [])
items = data.get("internal", {}).get("items", [])

lines = []
a = lines.append
a("# LoomQ Benchmark 报告\n")
a(f"**时间**: {env.get('timestamp', 'N/A')}")
a(f"**Commit**: `{env.get('commit', 'N/A')}` | **分支**: {env.get('branch', 'N/A')}")
a(f"**Java**: {env.get('java_version', 'N/A')} | **OS**: {env.get('os', 'N/A')} | **CPU**: {env.get('cpu_cores', 'N/A')} cores\n")

a("## 吞吐量对比\n")
a("| 指标 | HTTP | gRPC |")
a("|------|------|------|")
h = s.get("http_peak_qps")
g = s.get("grpc_peak_qps")
a(f"| Peak QPS | {h:,} | {g:,} |" if h and g else f"| Peak QPS | {h or 'N/A'} | {g or 'N/A'} |")
a(f"| 拐点线程数 | {s.get('http_inflection_threads', 'N/A')} | {s.get('grpc_inflection_threads', 'N/A')} |")
a(f"| 失败率 | {s.get('http_fail_rate', 'N/A')} | {s.get('grpc_fail_rate', 'N/A')} |")
a(f"| 内存/Intent | {s.get('memory_per_intent', 'N/A')} | {s.get('memory_per_intent', 'N/A')} |\n")

if cp:
    a("## Create Path 详情\n")
    a("| 线程数 | HTTP QPS | HTTP P50 | HTTP P90 | HTTP P99 | gRPC QPS | gRPC P50 | gRPC P90 | gRPC P99 |")
    a("|--------|----------|----------|----------|----------|----------|----------|----------|----------|")
    for r in cp:
        def f(v): return f"{v:,}" if isinstance(v, (int, float)) and v is not None else str(v or "-")
        a(f"| {r.get('threads', '-')} | {f(r.get('http_qps'))} | {f(r.get('http_p50'))} | {f(r.get('http_p90'))} | {f(r.get('http_p99'))} | {f(r.get('grpc_qps'))} | {f(r.get('grpc_p50'))} | {f(r.get('grpc_p90'))} | {f(r.get('grpc_p99'))} |")
    a("")

if tiers:
    a("## 调度精度\n")
    a("| 档位 | 并发 | QPS | Wake P50 | Wake P95 | Wake P99 | E2E P50 | E2E P95 | E2E P99 | 利用率 | 背压 | 完成率 |")
    a("|------|------|-----|----------|----------|----------|---------|---------|---------|--------|------|--------|")
    for t in tiers:
        def f(v): return f"{v:,}" if isinstance(v, (int, float)) and v is not None else str(v or "-")
        def p(v): return f"{v:.1f}%" if v is not None else "-"
        a(f"| {t.get('tier', '?')} | {f(t.get('concurrency'))} | {f(t.get('qps'))} | {f(t.get('wake_p50'))} | {f(t.get('wake_p95'))} | {f(t.get('wake_p99'))} | {f(t.get('e2e_p50'))} | {f(t.get('e2e_p95'))} | {f(t.get('e2e_p99'))} | {p(t.get('util_pct'))} | {p(t.get('backpressure_pct'))} | {p(t.get('completion_pct'))} |")
    a("")

if items:
    a("## 内部组件\n")
    a("| 基准测试 | 指标 | 值 |")
    a("|----------|------|-----|")
    for item in items:
        a(f"| {item.get('benchmark', '-')} | {item.get('metric', '-')} | {item.get('value', '-')} |")
    a("")

with open(md_file, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print(f"MD: {md_file}")
PYEOF

echo "  Markdown: $MD_FILE"

# ============================================================
#  Summary
# ============================================================

echo ""
echo "=== Summary ==="
if [ -n "$HTTP_RESULT" ]; then
    HTTP_PEAK=$(extract "$HTTP_RESULT" "peak_qps")
    [ -n "$HTTP_PEAK" ] && echo "  HTTP Peak QPS: $HTTP_PEAK"
fi
if [ -n "$GRPC_RESULT" ]; then
    GRPC_PEAK=$(extract "$GRPC_RESULT" "peak_qps")
    [ -n "$GRPC_PEAK" ] && echo "  gRPC Peak QPS: $GRPC_PEAK"
fi

echo ""
echo "报告已生成:"
echo "  Excel: $EXCEL_FILE"
echo "  MD:    $MD_FILE"
echo ""
echo "使用 --compare 查看上次报告。"
