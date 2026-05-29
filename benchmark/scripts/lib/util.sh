#!/usr/bin/env bash
# ============================================================
#  LoomQ Benchmark — Utility Functions
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# --- Git helpers ---

get_git_commit() {
    git -C "$PROJECT_ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown"
}

get_git_branch() {
    git -C "$PROJECT_ROOT" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown"
}

# --- Java detection ---

find_java() {
    if [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]]; then
        echo "$JAVA_HOME/bin/java"
        return
    fi
    if command -v java &>/dev/null; then
        echo "java"
        return
    fi
    print_error "Java not found. Set JAVA_HOME or add java to PATH."
    exit 1
}

get_java_version() {
    local java_cmd
    java_cmd=$(find_java)
    "$java_cmd" -version 2>&1 | head -1 | sed -E 's/.*"([0-9]+(\.[0-9]+)*).*/\1/'
}

# --- Result marker parsing ---
# Java benchmark classes print lines like:
#   RESULT|scenario=internal|tier=ULTRA|qps=12345|p95_us=100|p99_us=200

extract() {
    local line="$1"
    local key="$2"
    # Use sed instead of grep -oP for macOS compatibility
    echo "$line" | sed -n "s/.*${key}=\([^|]*\).*/\1/p"
}

extract_float() {
    local line="$1"
    local key="$2"
    local default="${3:-0}"
    local val
    val=$(extract "$line" "$key")
    if [[ -z "$val" ]]; then
        echo "$default"
    else
        echo "$val"
    fi
}

# --- Statistics ---

mean() {
    local sum=0
    local count=0
    for val in "$@"; do
        sum=$(echo "$sum + $val" | bc -l 2>/dev/null || echo "$sum")
        count=$((count + 1))
    done
    if [[ $count -eq 0 ]]; then
        echo "0"
    else
        echo "scale=2; $sum / $count" | bc -l 2>/dev/null || echo "0"
    fi
}

# --- TCP port check ---

wait_for_port() {
    local port="$1"
    local timeout_sec="${2:-30}"
    local start
    start=$(date +%s)
    while true; do
        if command -v nc &>/dev/null; then
            nc -z localhost "$port" 2>/dev/null && return 0
        elif command -v bash &>/dev/null; then
            (echo >/dev/tcp/localhost/"$port") 2>/dev/null && return 0
        fi
        local now
        now=$(date +%s)
        if (( now - start >= timeout_sec )); then
            return 1
        fi
        sleep 1
    done
}

# --- Config loading ---

load_config() {
    local config_file="$PROJECT_ROOT/benchmark/config.json"
    if [[ -f "$config_file" ]]; then
        cat "$config_file"
    else
        echo "{}"
    fi
}

get_config_value() {
    local json="$1"
    local path="$2"
    local default="${3:-}"
    # Simple JSON path extraction (works for flat keys)
    local val
    val=$(echo "$json" | sed -n "s/.*\"${path}\"[[:space:]]*:[[:space:]]*\([^,}]*\).*/\1/p" | tr -d '"' | tr -d ' ')
    if [[ -z "$val" ]]; then
        echo "$default"
    else
        echo "$val"
    fi
}

# --- OS detection ---

get_os_name() {
    case "$(uname -s)" in
        Linux*)     echo "Linux";;
        Darwin*)    echo "macOS";;
        CYGWIN*|MINGW*|MSYS*) echo "Windows";;
        *)          echo "Unknown";;
    esac
}
