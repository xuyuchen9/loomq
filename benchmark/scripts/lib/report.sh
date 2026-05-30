#!/usr/bin/env bash
# report.sh — Excel + MD 报告生成
# 依赖: Python 3 + xlsxwriter (pip install xlsxwriter)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPORT_DIR="${SCRIPT_DIR}/../../results/reports"
PYTHON_SCRIPT="${SCRIPT_DIR}/gen_excel.py"

# ── Excel 生成 ──
generate_excel() {
    local json_path="$1"
    local output_path="$2"

    if [[ -z "$output_path" ]]; then
        local ts
        ts=$(date +%Y%m%d-%H%M%S)
        mkdir -p "$REPORT_DIR"
        output_path="${REPORT_DIR}/benchmark-report-${ts}.xlsx"
    fi

    if [[ ! -f "$PYTHON_SCRIPT" ]]; then
        echo "WARNING: Excel 生成脚本不存在: $PYTHON_SCRIPT" >&2
        return 1
    fi

    python3 "$PYTHON_SCRIPT" "$json_path" "$output_path" 2>&1
    local rc=$?
    if [[ $rc -ne 0 ]]; then
        echo "WARNING: Excel 生成失败 (exit $rc)" >&2
        return 1
    fi

    rm -f "$json_path"
    echo "$output_path"
}

# ── MD 生成 ──
generate_markdown() {
    local json_path="$1"
    local output_path="$2"

    local ts
    ts=$(date +%Y%m%d-%H%M%S)
    if [[ -z "$output_path" ]]; then
        mkdir -p "$REPORT_DIR"
        output_path="${REPORT_DIR}/benchmark-report-${ts}.md"
    fi

    # 用 Python 解析 JSON 并生成 MD（bash 处理 JSON 太弱）
    python3 - "$json_path" "$output_path" << 'PYEOF'
import json, sys

json_path, md_path = sys.argv[1], sys.argv[2]
with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

env = data.get("environment", {})
summary = data.get("summary", {})
cp = data.get("create_path", {})
sched = data.get("scheduler", {})
internal = data.get("internal", {})
slo = data.get("slo", {})
regression = data.get("regression")

lines = []
a = lines.append

a("# LoomQ Benchmark 报告\n")
a(f"**时间**: {env.get('timestamp', 'N/A')}")
a(f"**Commit**: `{env.get('commit', 'N/A')}` | **分支**: {env.get('branch', 'N/A')}")
a(f"**Java**: {env.get('java_version', 'N/A')} | **OS**: {env.get('os', 'N/A')} | **CPU**: {env.get('cpu_cores', 'N/A')} cores | **内存**: {env.get('max_memory', 'N/A')}\n")

a("## 吞吐量对比\n")
a("| 指标 | HTTP | gRPC |")
a("|------|------|------|")
h = summary.get("http_peak_qps")
g = summary.get("grpc_peak_qps")
a(f"| Peak QPS | {h:,} | {g:,} |" if h and g else f"| Peak QPS | {h or 'N/A'} | {g or 'N/A'} |")
a(f"| 拐点线程数 | {summary.get('http_inflection_threads', 'N/A')} | {summary.get('grpc_inflection_threads', 'N/A')} |")
a(f"| P99 @ 拐点 | {summary.get('http_inflection_p99', 'N/A')}ms | {summary.get('grpc_inflection_p99', 'N/A')}ms |")
a(f"| 失败率 | {summary.get('http_fail_rate', 'N/A')} | {summary.get('grpc_fail_rate', 'N/A')} |")
a(f"| 内存/Intent | {summary.get('memory_per_intent', 'N/A')} | {summary.get('memory_per_intent', 'N/A')} |")
a(f"| SLO Pass | {summary.get('slo_pass_http', 'N/A')} | {summary.get('slo_pass_grpc', 'N/A')} |\n")

rows = cp.get("rows", [])
if rows:
    a("## Create Path 详情\n")
    a("| 线程数 | HTTP QPS | HTTP P50 | HTTP P90 | HTTP P99 | gRPC QPS | gRPC P50 | gRPC P90 | gRPC P99 |")
    a("|--------|----------|----------|----------|----------|----------|----------|----------|----------|")
    for r in rows:
        def f(v): return f"{v:,}" if isinstance(v, (int, float)) and v is not None else str(v or "-")
        a(f"| {r.get('threads', '-')} | {f(r.get('http_qps'))} | {f(r.get('http_p50'))} | {f(r.get('http_p90'))} | {f(r.get('http_p99'))} | {f(r.get('grpc_qps'))} | {f(r.get('grpc_p50'))} | {f(r.get('grpc_p90'))} | {f(r.get('grpc_p99'))} |")
    a("")

tiers = sched.get("tiers", [])
if tiers:
    a("## 调度精度\n")
    a("| 档位 | 并发 | QPS | Wake P50 | Wake P95 | Wake P99 | E2E P50 | E2E P95 | E2E P99 | 利用率 | 背压 | 完成率 |")
    a("|------|------|-----|----------|----------|----------|---------|---------|---------|--------|------|--------|")
    for t in tiers:
        def f(v): return f"{v:,}" if isinstance(v, (int, float)) and v is not None else str(v or "-")
        def p(v): return f"{v:.1f}%" if v is not None else "-"
        a(f"| {t.get('tier', '?')} | {f(t.get('concurrency'))} | {f(t.get('qps'))} | {f(t.get('wake_p50'))} | {f(t.get('wake_p95'))} | {f(t.get('wake_p99'))} | {f(t.get('e2e_p50'))} | {f(t.get('e2e_p95'))} | {f(t.get('e2e_p99'))} | {p(t.get('util_pct'))} | {p(t.get('backpressure_pct'))} | {p(t.get('completion_pct'))} |")
    a("")

items = internal.get("items", [])
if items:
    a("## 内部组件\n")
    a("| 基准测试 | 指标 | 值 |")
    a("|----------|------|-----|")
    for item in items:
        a(f"| {item.get('benchmark', '-')} | {item.get('metric', '-')} | {item.get('value', '-')} |")
    a("")

slo_items = slo.get("items", [])
if slo_items:
    a("## SLO 验证\n")
    a("| 档位 | Wake P95 目标 | Wake P95 实际 | 结果 | Wake P99 目标 | Wake P99 实际 | 结果 | E2E P95 目标 | E2E P95 实际 | 结果 | E2E P99 目标 | E2E P99 实际 | 结果 |")
    a("|------|---------------|---------------|------|---------------|---------------|------|--------------|--------------|------|--------------|--------------|------|")
    for item in slo_items:
        cells = [item.get("tier", "?")]
        for key in ("wake_p95", "wake_p99", "e2e_p95", "e2e_p99"):
            cells.append(item.get(f"{key}_target", "-"))
            cells.append(item.get(f"{key}_actual", "-"))
            cells.append("PASS" if item.get(f"{key}_pass") else "FAIL")
        a("| " + " | ".join(str(c) for c in cells) + " |")
    a("")

if regression:
    a("## 回归对比（vs 上次运行）\n")
    hd = regression.get("http_qps_delta")
    gd = regression.get("grpc_qps_delta")
    if hd is not None:
        sign = "+" if hd >= 0 else ""
        a(f"- HTTP QPS: {sign}{hd:.1%}")
    if gd is not None:
        sign = "+" if gd >= 0 else ""
        a(f"- gRPC QPS: {sign}{gd:.1%}")
    a("")

with open(md_path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print(f"MD 报告已生成: {md_path}")
PYEOF

    echo "$output_path"
}
