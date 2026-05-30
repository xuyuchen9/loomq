#!/usr/bin/env bash
# ============================================================
#  LoomQ Benchmark — UI Helpers
# ============================================================

# ANSI colors
if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[0;33m'
    BLUE='\033[0;34m'
    CYAN='\033[0;36m'
    BOLD='\033[1m'
    DIM='\033[2m'
    RESET='\033[0m'
else
    RED='' GREEN='' YELLOW='' BLUE='' CYAN='' BOLD='' DIM='' RESET=''
fi

# Unicode symbols
SYMBOL_OK="✓"
SYMBOL_FAIL="✗"
SYMBOL_WARN="⚠"
SYMBOL_ARROW="→"
SYMBOL_DOT="●"

print_header() {
    echo ""
    echo -e "${BOLD}${BLUE}============================================================${RESET}"
    echo -e "${BOLD}${BLUE}  $1${RESET}"
    echo -e "${BOLD}${BLUE}============================================================${RESET}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${BOLD}${CYAN}--- $1 ---${RESET}"
    echo ""
}

print_success() {
    echo -e "  ${GREEN}${SYMBOL_OK} $1${RESET}"
}

print_warning() {
    echo -e "  ${YELLOW}${SYMBOL_WARN} $1${RESET}"
}

print_error() {
    echo -e "  ${RED}${SYMBOL_FAIL} $1${RESET}"
}

print_info() {
    echo -e "  ${DIM}$1${RESET}"
}

print_kv() {
    local key="$1"
    local value="$2"
    printf "  ${DIM}%-24s${RESET} %s\n" "$key" "$value"
}
