#!/usr/bin/env python3
"""LoomQ Benchmark Excel 报告生成器。

用法: python gen_excel.py <input.json> <output.xlsx>

输入 JSON 结构:
{
  "timestamp": "20260530-010000",
  "environment": { "commit", "branch", "java_version", "os", "cpu_cores", "max_memory" },
  "summary": { "http_peak_qps", "grpc_peak_qps", "http_inflection_threads", "grpc_inflection_threads",
               "http_inflection_p99", "grpc_inflection_p99", "http_fail_rate", "grpc_fail_rate",
               "memory_per_intent", "slo_pass_http", "slo_pass_grpc" },
  "create_path": {
    "rows": [ { "threads", "http_qps", "http_p50", "http_p90", "http_p99",
                "grpc_qps", "grpc_p50", "grpc_p90", "grpc_p99" } ]
  },
  "scheduler": {
    "tiers": [ { "tier", "concurrency", "qps", "wake_p50", "wake_p95", "wake_p99",
                 "e2e_p50", "e2e_p95", "e2e_p99", "util_pct", "backpressure_pct", "completion_pct" } ]
  },
  "internal": {
    "items": [ { "benchmark", "metric", "value" } ]
  },
  "slo": {
    "items": [ { "tier",
                 "wake_p95_target", "wake_p95_actual", "wake_p95_pass",
                 "wake_p99_target", "wake_p99_actual", "wake_p99_pass",
                 "e2e_p95_target", "e2e_p95_actual", "e2e_p95_pass",
                 "e2e_p99_target", "e2e_p99_actual", "e2e_p99_pass" } ]
  },
  "regression": { "http_qps_delta", "grpc_qps_delta" }
}
"""

import json
import sys
import os

import xlsxwriter


def fmt_num(val):
    """格式化数值，返回 int 或 float"""
    if val is None:
        return "-"
    if isinstance(val, str):
        try:
            val = float(val.replace(",", "").replace("ms", "").replace("us", "").replace("B", "").replace("%", ""))
        except ValueError:
            return val
    return val


def pct(val):
    """百分比值转小数"""
    if val is None:
        return None
    if isinstance(val, str):
        val = float(val.replace("%", ""))
    return val / 100 if val > 1 else val


def main():
    if len(sys.argv) < 3:
        print("用法: gen_excel.py <input.json> <output.xlsx>", file=sys.stderr)
        sys.exit(1)

    json_path = sys.argv[1]
    xlsx_path = sys.argv[2]

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    wb = xlsxwriter.Workbook(xlsx_path)

    # ── 通用格式 ──
    title_fmt = wb.add_format({
        "bold": True, "font_size": 16, "font_color": "#1a1a2e",
        "bottom": 3, "bottom_color": "#e94560", "font_name": "Segoe UI",
    })
    header_fmt = wb.add_format({
        "bold": True, "bg_color": "#16213e", "font_color": "#ffffff",
        "border": 1, "text_wrap": True, "align": "center", "valign": "vcenter",
        "font_name": "Segoe UI", "font_size": 10,
    })
    num_fmt = wb.add_format({
        "num_format": "#,##0", "border": 1, "align": "center",
        "font_name": "Segoe UI", "font_size": 10,
    })
    num_dec_fmt = wb.add_format({
        "num_format": "#,##0.0", "border": 1, "align": "center",
        "font_name": "Segoe UI", "font_size": 10,
    })
    pct_fmt = wb.add_format({
        "num_format": "0.0%", "border": 1, "align": "center",
        "font_name": "Segoe UI", "font_size": 10,
    })
    str_fmt = wb.add_format({
        "border": 1, "align": "center", "font_name": "Segoe UI", "font_size": 10,
    })
    str_left = wb.add_format({
        "border": 1, "align": "left", "font_name": "Segoe UI", "font_size": 10,
    })
    pass_fmt = wb.add_format({
        "bg_color": "#d4edda", "font_color": "#155724", "border": 1,
        "align": "center", "bold": True, "font_name": "Segoe UI", "font_size": 10,
    })
    fail_fmt = wb.add_format({
        "bg_color": "#f8d7da", "font_color": "#721c24", "border": 1,
        "align": "center", "bold": True, "font_name": "Segoe UI", "font_size": 10,
    })
    label_fmt = wb.add_format({
        "bold": True, "border": 1, "align": "left",
        "bg_color": "#f0f0f0", "font_name": "Segoe UI", "font_size": 10,
    })
    metric_label = wb.add_format({
        "bold": True, "border": 1, "align": "left",
        "font_name": "Segoe UI", "font_size": 10, "indent": 1,
    })

    # ══════════════════════════════════════════════════════
    # Sheet 1: Summary
    # ══════════════════════════════════════════════════════
    ws = wb.add_worksheet("Summary")
    ws.set_tab_color("#e94560")
    ws.set_column("A:A", 22)
    ws.set_column("B:C", 18)
    ws.hide_gridlines(2)

    summary = data.get("summary", {})
    ws.merge_range("B1:C1", "LoomQ Benchmark Report", title_fmt)
    ws.write("A3", "", header_fmt)
    ws.write("B3", "HTTP", header_fmt)
    ws.write("C3", "gRPC", header_fmt)

    rows = [
        ("Peak QPS", summary.get("http_peak_qps"), summary.get("grpc_peak_qps"), num_fmt),
        ("拐点线程数", summary.get("http_inflection_threads"), summary.get("grpc_inflection_threads"), num_fmt),
        ("P99 @ 拐点", summary.get("http_inflection_p99"), summary.get("grpc_inflection_p99"), str_fmt),
        ("失败率", pct(summary.get("http_fail_rate")), pct(summary.get("grpc_fail_rate")), pct_fmt),
        ("内存/Intent", summary.get("memory_per_intent"), summary.get("memory_per_intent"), str_fmt),
        ("SLO Pass", summary.get("slo_pass_http"), summary.get("slo_pass_grpc"), str_fmt),
    ]
    for i, (label, http_val, grpc_val, fmt) in enumerate(rows, 3):
        ws.write(i, 0, label, label_fmt)
        if fmt == pct_fmt and http_val is not None:
            ws.write(i, 1, http_val, pct_fmt)
            ws.write(i, 2, grpc_val, pct_fmt)
        elif fmt == num_fmt and http_val is not None:
            ws.write(i, 1, fmt_num(http_val), num_fmt)
            ws.write(i, 2, fmt_num(grpc_val), num_fmt)
        else:
            ws.write(i, 1, str(http_val) if http_val is not None else "-", str_fmt)
            ws.write(i, 2, str(grpc_val) if grpc_val is not None else "-", str_fmt)

    # 回归对比
    regression = data.get("regression")
    if regression:
        ws.write(10, 0, "回归对比", label_fmt)
        h_delta = regression.get("http_qps_delta")
        g_delta = regression.get("grpc_qps_delta")
        if h_delta is not None:
            delta_fmt = wb.add_format({
                "num_format": "+0.0%;-0.0%", "border": 1, "align": "center",
                "font_color": "#155724" if h_delta >= 0 else "#721c24",
                "bold": True, "font_name": "Segoe UI", "font_size": 10,
            })
            ws.write(11, 0, "HTTP QPS 变化", metric_label)
            ws.write(11, 1, h_delta, delta_fmt)
        if g_delta is not None:
            delta_fmt = wb.add_format({
                "num_format": "+0.0%;-0.0%", "border": 1, "align": "center",
                "font_color": "#155724" if g_delta >= 0 else "#721c24",
                "bold": True, "font_name": "Segoe UI", "font_size": 10,
            })
            ws.write(12, 0, "gRPC QPS 变化", metric_label)
            ws.write(12, 1, g_delta, delta_fmt)

    # ══════════════════════════════════════════════════════
    # Sheet 2: Create Path
    # ══════════════════════════════════════════════════════
    ws2 = wb.add_worksheet("Create Path")
    ws2.set_tab_color("#0f3460")
    ws2.hide_gridlines(2)
    cp_headers = [
        "线程数", "HTTP QPS", "HTTP P50", "HTTP P90", "HTTP P99",
        "gRPC QPS", "gRPC P50", "gRPC P90", "gRPC P99",
    ]
    cp_widths = [10, 14, 12, 12, 12, 14, 12, 12, 12]
    for j, (h, w) in enumerate(zip(cp_headers, cp_widths)):
        ws2.write(0, j, h, header_fmt)
        ws2.set_column(j, j, w)

    cp_rows = data.get("create_path", {}).get("rows", [])
    for i, row in enumerate(cp_rows, 1):
        ws2.write(i, 0, row.get("threads", "-"), num_fmt)
        ws2.write(i, 1, fmt_num(row.get("http_qps")), num_fmt)
        ws2.write(i, 2, fmt_num(row.get("http_p50")), num_fmt)
        ws2.write(i, 3, fmt_num(row.get("http_p90")), num_fmt)
        ws2.write(i, 4, fmt_num(row.get("http_p99")), num_fmt)
        ws2.write(i, 5, fmt_num(row.get("grpc_qps")), num_fmt)
        ws2.write(i, 6, fmt_num(row.get("grpc_p50")), num_fmt)
        ws2.write(i, 7, fmt_num(row.get("grpc_p90")), num_fmt)
        ws2.write(i, 8, fmt_num(row.get("grpc_p99")), num_fmt)

    # ══════════════════════════════════════════════════════
    # Sheet 3: Scheduler
    # ══════════════════════════════════════════════════════
    ws3 = wb.add_worksheet("Scheduler")
    ws3.set_tab_color("#533483")
    ws3.hide_gridlines(2)
    sched_headers = [
        "档位", "并发", "QPS",
        "Wake P50", "Wake P95", "Wake P99",
        "E2E P50", "E2E P95", "E2E P99",
        "利用率", "背压", "完成率",
    ]
    sched_widths = [12, 10, 12, 12, 12, 12, 12, 12, 12, 10, 10, 10]
    for j, (h, w) in enumerate(zip(sched_headers, sched_widths)):
        ws3.write(0, j, h, header_fmt)
        ws3.set_column(j, j, w)

    tiers = data.get("scheduler", {}).get("tiers", [])
    tier_colors = {
        "ULTRA": "#ff6b6b", "FAST": "#ffa502", "HIGH": "#ffd93d",
        "STANDARD": "#6bcb77", "ECONOMY": "#4d96ff",
    }
    for i, tier in enumerate(tiers, 1):
        tier_name = tier.get("tier", "?")
        tier_fmt = wb.add_format({
            "border": 1, "align": "center", "bold": True,
            "bg_color": tier_colors.get(tier_name, "#ffffff"),
            "font_color": "#000000" if tier_name in ("HIGH", "FAST") else "#ffffff" if tier_name == "ULTRA" else "#000000",
            "font_name": "Segoe UI", "font_size": 10,
        })
        ws3.write(i, 0, tier_name, tier_fmt)
        ws3.write(i, 1, fmt_num(tier.get("concurrency")), num_fmt)
        ws3.write(i, 2, fmt_num(tier.get("qps")), num_fmt)
        ws3.write(i, 3, fmt_num(tier.get("wake_p50")), num_fmt)
        ws3.write(i, 4, fmt_num(tier.get("wake_p95")), num_fmt)
        ws3.write(i, 5, fmt_num(tier.get("wake_p99")), num_fmt)
        ws3.write(i, 6, fmt_num(tier.get("e2e_p50")), num_dec_fmt)
        ws3.write(i, 7, fmt_num(tier.get("e2e_p95")), num_dec_fmt)
        ws3.write(i, 8, fmt_num(tier.get("e2e_p99")), num_dec_fmt)
        ws3.write(i, 9, pct(tier.get("util_pct")), pct_fmt)
        ws3.write(i, 10, pct(tier.get("backpressure_pct")), pct_fmt)
        ws3.write(i, 11, pct(tier.get("completion_pct")), pct_fmt)

    # ══════════════════════════════════════════════════════
    # Sheet 4: Internal
    # ══════════════════════════════════════════════════════
    ws4 = wb.add_worksheet("Internal")
    ws4.set_tab_color("#e94560")
    ws4.hide_gridlines(2)
    ws4.set_column("A:A", 22)
    ws4.set_column("B:B", 18)
    ws4.set_column("C:C", 18)
    ws4.write(0, 0, "基准测试", header_fmt)
    ws4.write(0, 1, "指标", header_fmt)
    ws4.write(0, 2, "值", header_fmt)

    internal_items = data.get("internal", {}).get("items", [])
    for i, item in enumerate(internal_items, 1):
        ws4.write(i, 0, item.get("benchmark", "-"), str_left)
        ws4.write(i, 1, item.get("metric", "-"), str_fmt)
        ws4.write(i, 2, str(item.get("value", "-")), str_fmt)

    # ══════════════════════════════════════════════════════
    # Sheet 5: SLO
    # ══════════════════════════════════════════════════════
    ws5 = wb.add_worksheet("SLO")
    ws5.set_tab_color("#ffd93d")
    ws5.hide_gridlines(2)
    ws5.set_column("A:A", 12)
    ws5.set_column("B:M", 14)

    slo_headers = [
        "档位",
        "Wake P95 目标", "Wake P95 实际", "结果",
        "Wake P99 目标", "Wake P99 实际", "结果",
        "E2E P95 目标", "E2E P95 实际", "结果",
        "E2E P99 目标", "E2E P99 实际", "结果",
    ]
    for j, h in enumerate(slo_headers):
        ws5.write(0, j, h, header_fmt)

    slo_items = data.get("slo", {}).get("items", [])
    for i, item in enumerate(slo_items, 1):
        ws5.write(i, 0, item.get("tier", "?"), label_fmt)
        col = 1
        for key in ("wake_p95", "wake_p99", "e2e_p95", "e2e_p99"):
            ws5.write(i, col, item.get(f"{key}_target", "-"), str_fmt)
            ws5.write(i, col + 1, item.get(f"{key}_actual", "-"), str_fmt)
            passed = item.get(f"{key}_pass", False)
            ws5.write(i, col + 2, "PASS" if passed else "FAIL", pass_fmt if passed else fail_fmt)
            col += 3

    # ══════════════════════════════════════════════════════
    # Sheet 6: Environment
    # ══════════════════════════════════════════════════════
    ws6 = wb.add_worksheet("Environment")
    ws6.set_tab_color("#16213e")
    ws6.hide_gridlines(2)
    ws6.set_column("A:A", 18)
    ws6.set_column("B:B", 40)
    ws6.write(0, 0, "Key", header_fmt)
    ws6.write(0, 1, "Value", header_fmt)

    env = data.get("environment", {})
    env_fields = [
        ("timestamp", "时间"),
        ("commit", "Commit"),
        ("branch", "分支"),
        ("java_version", "Java 版本"),
        ("os", "操作系统"),
        ("cpu_cores", "CPU 核心数"),
        ("max_memory", "最大内存"),
    ]
    for i, (key, label) in enumerate(env_fields, 1):
        ws6.write(i, 0, label, label_fmt)
        ws6.write(i, 1, str(env.get(key, "-")), str_fmt)

    # ══════════════════════════════════════════════════════
    # 完成
    # ══════════════════════════════════════════════════════
    wb.close()
    print(f"Excel 报告已生成: {xlsx_path}")


if __name__ == "__main__":
    main()
