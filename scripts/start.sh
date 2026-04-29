#!/bin/bash
#
# LoomQ Startup Script (Linux/macOS)
# Version: 0.7.0
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default configuration
JAR_FILE="loomq-server/target/loomq-server-0.8.0-SNAPSHOT.jar"
JVM_XMS="${JVM_XMS:-2g}"
JVM_XMX="${JVM_XMX:-2g}"
JVM_GC="${JVM_GC:-ZGC}"
JVM_GC_PAUSE="${JVM_GC_PAUSE:-10}"
LOOMQ_PORT="${LOOMQ_PORT:-8080}"
LOOMQ_HOST="${LOOMQ_HOST:-0.0.0.0}"

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check Java version
check_java() {
    if ! command -v java &> /dev/null; then
        print_error "Java is not installed or not in PATH"
        exit 1
    fi

    JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
    JAVA_MAJOR=$(echo "$JAVA_VERSION" | awk -F. '{print $1}')

    if [ "$JAVA_MAJOR" -lt 25 ]; then
        print_error "Java 25+ is required, found: $JAVA_VERSION"
        exit 1
    fi

    print_info "Java version: $JAVA_VERSION"
}

# Function to check if JAR exists
check_jar() {
    if [ ! -f "$JAR_FILE" ]; then
        print_error "JAR file not found: $JAR_FILE"
        print_info "Please build the project first: mvn clean package -DskipTests"
        exit 1
    fi
}

# Function to create data directory
setup_directories() {
    DATA_DIR="${LOOMQ_DATA_DIR:-./data/wal}"
    if [ ! -d "$DATA_DIR" ]; then
        print_info "Creating data directory: $DATA_DIR"
        mkdir -p "$DATA_DIR"
    fi
}

# Function to display configuration
show_config() {
    echo ""
    echo "=========================================="
    echo "  LoomQ Configuration"
    echo "=========================================="
    echo "  JAR File:        $JAR_FILE"
    echo "  JVM Heap:        $JVM_XMS / $JVM_XMX"
    echo "  GC Type:         $JVM_GC"
    echo "  Max GC Pause:    ${JVM_GC_PAUSE}ms"
    echo "  Server Host:     $LOOMQ_HOST"
    echo "  Server Port:     $LOOMQ_PORT"
    echo "  Data Directory:  ${LOOMQ_DATA_DIR:-./data/wal}"
    echo "=========================================="
    echo ""
}

# Function to start LoomQ
start_loomq() {
    print_info "Starting LoomQ..."

    # Build JVM arguments
    JVM_ARGS="-Xms${JVM_XMS} -Xmx${JVM_XMX}"
    JVM_ARGS="${JVM_ARGS} -XX:+Use${JVM_GC}"
    JVM_ARGS="${JVM_ARGS} -XX:MaxGCPauseMillis=${JVM_GC_PAUSE}"

    # System properties
    SYS_PROPS="-Dloomq.server.host=${LOOMQ_HOST}"
    SYS_PROPS="${SYS_PROPS} -Dloomq.server.port=${LOOMQ_PORT}"

    # Add extra system properties if provided
    if [ -n "$LOOMQ_EXTRA_PROPS" ]; then
        SYS_PROPS="${SYS_PROPS} ${LOOMQ_EXTRA_PROPS}"
    fi

    # Start the application
    exec java ${JVM_ARGS} ${SYS_PROPS} -jar "${JAR_FILE}"
}

# Function to handle signals
cleanup() {
    print_warn "Received shutdown signal, stopping LoomQ..."
    exit 0
}

trap cleanup SIGTERM SIGINT

# Main execution
main() {
    print_info "LoomQ Startup Script v0.7.0"

    check_java
    check_jar
    setup_directories
    show_config
    start_loomq
}

# Show help
show_help() {
    cat << EOF
LoomQ Startup Script

Usage: $0 [options]

Environment Variables:
  JVM_XMS              Initial heap size (default: 2g)
  JVM_XMX              Maximum heap size (default: 2g)
  JVM_GC               Garbage collector: ZGC, G1GC, ParallelGC (default: ZGC)
  JVM_GC_PAUSE         Max GC pause target in ms (default: 10)
  LOOMQ_PORT           Server port (default: 8080)
  LOOMQ_HOST           Server host (default: 0.0.0.0)
  LOOMQ_DATA_DIR       WAL data directory (default: ./data/wal)
  LOOMQ_EXTRA_PROPS    Additional JVM system properties

Examples:
  # Start with defaults
  ./scripts/start.sh

  # Start with custom heap size
  JVM_XMS=4g JVM_XMX=4g ./scripts/start.sh

  # Start cluster node
  LOOMQ_PORT=8081 LOOMQ_SHARD_INDEX=1 ./scripts/start.sh

Options:
  -h, --help    Show this help message
EOF
}

# Parse arguments
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
    exit 0
fi

main
