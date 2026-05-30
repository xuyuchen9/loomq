#!/usr/bin/env bash
# ============================================================
#  LoomQ Benchmark — Server Lifecycle Management
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
RUNTIME_DIR="$PROJECT_ROOT/benchmark/results/runtime"

# Source dependencies
source "$SCRIPT_DIR/ui.sh"
source "$SCRIPT_DIR/util.sh"

SERVER_PID=""
SERVER_PORT=7928
TEMP_DATA_DIR=""

# --- JAR detection ---

find_server_jar() {
    local jar_pattern="$PROJECT_ROOT/loomq-server/target/loomq-server-*.jar"
    local jar
    jar=$(ls $jar_pattern 2>/dev/null | grep -v original | head -1)
    if [[ -z "$jar" ]]; then
        print_error "Server JAR not found. Run 'mvn package -DskipTests' first."
        return 1
    fi
    echo "$jar"
}

# --- Server start ---

start_server() {
    local port="${1:-$SERVER_PORT}"
    local extra_args="${2:-}"

    local jar
    jar=$(find_server_jar) || return 1

    # Create temp data directory
    TEMP_DATA_DIR="$RUNTIME_DIR/data-$(date +%s)"
    mkdir -p "$TEMP_DATA_DIR"

    local java_cmd
    java_cmd=$(find_java)

    print_info "Starting server on port $port..."
    print_info "JAR: $jar"
    print_info "Data: $TEMP_DATA_DIR"

    $java_cmd \
        -Xms1g -Xmx1g \
        -XX:+UseZGC \
        -Dloomq.server.host=0.0.0.0 \
        -Dloomq.server.port=$port \
        -Dloomq.wal.data_dir="$TEMP_DATA_DIR/wal" \
        -Dloomq.wal.flush_strategy=async \
        -jar "$jar" \
        $extra_args \
        > "$LOGS_DIR/server-$(date +%Y%m%d-%H%M%S).log" 2>&1 &

    SERVER_PID=$!
    SERVER_PORT=$port

    print_info "Server PID: $SERVER_PID"
    wait_for_port "$port" 30
    if [[ $? -eq 0 ]]; then
        print_success "Server started on port $port"
        return 0
    else
        print_error "Server failed to start within 30s"
        stop_server
        return 1
    fi
}

# --- Server stop ---

stop_server() {
    if [[ -n "$SERVER_PID" ]]; then
        print_info "Stopping server (PID $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
        SERVER_PID=""
        print_success "Server stopped"
    fi

    # Cleanup temp data
    if [[ -n "$TEMP_DATA_DIR" ]] && [[ -d "$TEMP_DATA_DIR" ]]; then
        rm -rf "$TEMP_DATA_DIR"
        print_info "Cleaned up: $TEMP_DATA_DIR"
        TEMP_DATA_DIR=""
    fi
}

# --- Health check ---

check_server_health() {
    local port="${1:-$SERVER_PORT}"
    local url="http://localhost:$port/health"
    if command -v curl &>/dev/null; then
        curl -sf "$url" >/dev/null 2>&1
    elif command -v wget &>/dev/null; then
        wget -q -O /dev/null "$url" 2>/dev/null
    else
        # Fallback to TCP check
        wait_for_port "$port" 1
    fi
}

# --- Trap for cleanup ---

setup_server_trap() {
    trap 'stop_server' EXIT INT TERM
}
