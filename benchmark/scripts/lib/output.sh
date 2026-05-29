#!/usr/bin/env bash
# ============================================================
#  LoomQ Benchmark — Output Formatting (CSV, JSON, TXT)
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/benchmark/results"
REPORTS_DIR="$RESULTS_DIR/reports"
LOGS_DIR="$RESULTS_DIR/logs"

# Source dependencies
source "$SCRIPT_DIR/ui.sh"
source "$SCRIPT_DIR/util.sh"

# --- Report rotation ---

rotate_reports() {
    local keep="${1:-10}"
    mkdir -p "$REPORTS_DIR"

    # Rotate JSON reports
    local json_files
    json_files=$(ls -t "$REPORTS_DIR"/benchmark-*.json "$REPORTS_DIR"/full-suite-*.json 2>/dev/null | tail -n +$((keep + 1)))
    if [[ -n "$json_files" ]]; then
        echo "$json_files" | xargs rm -f
    fi

    # Rotate TXT summaries
    local txt_files
    txt_files=$(ls -t "$REPORTS_DIR"/summary-*.txt "$REPORTS_DIR"/full-suite-*.txt 2>/dev/null | tail -n +$((keep + 1)))
    if [[ -n "$txt_files" ]]; then
        echo "$txt_files" | xargs rm -f
    fi

    # Rotate logs
    local log_files
    log_files=$(ls -t "$LOGS_DIR"/scenario-*.log "$LOGS_DIR"/full-suite-*.log 2>/dev/null | tail -n +$((keep + 1)))
    if [[ -n "$log_files" ]]; then
        echo "$log_files" | xargs rm -f
    fi
}

# --- CSV ---

CSV_TIERS="ULTRA FAST HIGH STANDARD ECONOMY"

get_csv_header() {
    local cols="timestamp,commit,branch,mode,java_version,os_name"
    cols="$cols,internal_qps"
    cols="$cols,http_peak_qps,http_best_p99_ms,http_worst_p99_ms,http_fail_rate"
    cols="$cols,grpc_peak_qps,grpc_best_p99_ms,grpc_worst_p99_ms,grpc_fail_rate"
    for t in $CSV_TIERS; do
        cols="$cols,${t}_qps,${t}_p95_ms,${t}_p99_ms,${t}_e2e_p95_ms,${t}_e2e_p99_ms,${t}_util_pct,${t}_backpressure"
    done
    cols="$cols,completion_rate,total_qps,global_p95_total_ms,vt_reduction_pct,cohort_wake_events"
    echo "$cols"
}

ensure_csv_header() {
    local csv_file="$1"
    local expected_header
    expected_header=$(get_csv_header)

    if [[ -f "$csv_file" ]]; then
        local first_line
        first_line=$(head -1 "$csv_file")
        if [[ "$first_line" == "$expected_header" ]]; then
            return 0
        fi
    fi
    echo "$expected_header" > "$csv_file"
}

write_csv_row() {
    local csv_file="$1"
    local row="$2"
    ensure_csv_header "$csv_file"
    echo "$row" >> "$csv_file"
    print_info "CSV appended: $csv_file"
}

# --- JSON ---

write_json_report() {
    local json_file="$1"
    local timestamp="$2"
    local commit="$3"
    local branch="$4"
    local mode="$5"
    local scenario="$6"
    local scenario_data="$7"

    mkdir -p "$(dirname "$json_file")"
    cat > "$json_file" <<EOF
{
  "timestamp": "$timestamp",
  "commit": "$commit",
  "branch": "$branch",
  "mode": "$mode",
  "scenario": "$scenario",
  "data": $scenario_data
}
EOF
    print_info "JSON report: $json_file"
}

# --- TXT Summary ---

write_txt_summary() {
    local txt_file="$1"
    shift
    local content="$*"

    mkdir -p "$(dirname "$txt_file")"
    echo "$content" > "$txt_file"
    print_info "Summary: $txt_file"
}

# --- Latest summary symlink ---

update_latest_summary() {
    local latest_file="$REPORTS_DIR/latest-summary.txt"
    local source_file="$1"
    mkdir -p "$REPORTS_DIR"
    if [[ -f "$source_file" ]]; then
        cp "$source_file" "$latest_file"
    fi
}

# --- Initialization ---

init_output_dirs() {
    mkdir -p "$RESULTS_DIR" "$REPORTS_DIR" "$LOGS_DIR"
}
