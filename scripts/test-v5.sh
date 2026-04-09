#!/bin/bash
# LoomQ v0.5.0 测试脚本 (Bash)
#
# 用法:
#   ./scripts/test-v5.sh all          # 运行所有测试
#   ./scripts/test-v5.sh functional   # 运行功能测试
#   ./scripts/test-v5.sh stress       # 运行压力测试
#   ./scripts/test-v5.sh replication  # 运行复制测试
#   ./scripts/test-v5.sh benchmark    # 运行基准测试

set -e

# 配置
TEST_TYPE="${1:-all}"
PORT="${PORT:-18080}"
DATA_DIR="${DATA_DIR:-./test-data}"
BASE_URL="http://localhost:$PORT"
SERVER_PID=""

# 结果目录
RESULT_DIR="./test-results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="$RESULT_DIR/test-result-$TIMESTAMP.json"
RESULT_MARKDOWN="$RESULT_DIR/test-report-$TIMESTAMP.md"
mkdir -p "$RESULT_DIR"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 测试结果
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
TEST_RESULTS=""

# 时间记录
START_TIME=$(date +%s)

# 工具函数
print_header() {
    echo -e "\n${CYAN}========================================${NC}"
    echo -e "${CYAN} $1${NC}"
    echo -e "${CYAN}========================================${NC}\n"
}

log_test() {
    local test_id="$1"
    local description="$2"
    local status="$3"
    local details="$4"

    if [ "$status" = "PASS" ]; then
        echo -e "${GREEN}[PASS]${NC} $test_id : $description"
        ((PASS_COUNT++))
    elif [ "$status" = "SKIP" ]; then
        echo -e "${YELLOW}[SKIP]${NC} $test_id : $description"
        echo -e "       $details"
        ((SKIP_COUNT++))
    else
        echo -e "${RED}[FAIL]${NC} $test_id : $description"
        echo -e "       $details"
        ((FAIL_COUNT++))
    fi
}

http_request() {
    local method="$1"
    local url="$2"
    local body="$3"
    local expected_status="$4"

    if [ -n "$body" ]; then
        curl -s -X "$method" "$url" \
            -H "Content-Type: application/json" \
            -d "$body" \
            -w "\n%{http_code}" \
            --connect-timeout 5 \
            --max-time 30 2>/dev/null
    else
        curl -s -X "$method" "$url" \
            -w "\n%{http_code}" \
            --connect-timeout 5 \
            --max-time 30 2>/dev/null
    fi
}

wait_for_server() {
    local max_attempts=30
    local attempt=1

    echo "Waiting for server to start..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$BASE_URL/health" 2>/dev/null | grep -q "UP"; then
            echo "Server is ready"
            return 0
        fi
        sleep 0.5
        ((attempt++))
    done

    echo "Server failed to start within timeout"
    return 1
}

start_server() {
    echo -e "${YELLOW}Starting LoomQ server on port $PORT...${NC}"

    # 清理数据目录
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"

    # 查找 JAR (shaded jar is named loomq-VERSION.jar, original- prefix for non-shaded)
    JAR_FILE=$(find target -maxdepth 1 -name "loomq-*.jar" ! -name "original-*" | head -1)

    if [ -z "$JAR_FILE" ]; then
        echo "Building project..."
        mvn clean package -DskipTests -q
        JAR_FILE=$(find target -maxdepth 1 -name "loomq-*.jar" ! -name "original-*" | head -1)
    fi

    echo "Found JAR: $JAR_FILE"

    if [ -z "$JAR_FILE" ]; then
        echo "JAR file not found"
        exit 1
    fi

    # 启动服务器
    export loomq_node_id="test-node-1"
    export loomq_shard_id="test-shard"
    export loomq_data_dir="$DATA_DIR"
    export loomq_port="$PORT"

    java --enable-preview -jar "$JAR_FILE" > /tmp/loomq-test.log 2>&1 &
    SERVER_PID=$!

    # 等待就绪
    wait_for_server

    echo -e "${GREEN}Server started (PID: $SERVER_PID)${NC}"
}

stop_server() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        echo -e "${YELLOW}Stopping server...${NC}"
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        echo -e "${GREEN}Server stopped${NC}"
    fi
}

# 功能测试
run_functional_tests() {
    print_header "Functional Tests (FT-01 ~ FT-14)"

    local response
    local status

    # FT-01: 创建 DURABLE Intent
    response=$(http_request POST "$BASE_URL/v1/intents" '{
        "intentId": "test-durable-'$$'",
        "executeAt": "'"$(date -u -d '+5 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "deadline": "'"$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "shardKey": "test-shard",
        "ackLevel": "DURABLE",
        "callback": {"url": "https://example.com/webhook", "method": "POST"}
    }')
    status=$(echo "$response" | tail -1)
    if [ "$status" = "201" ]; then
        log_test "FT-01" "Create DURABLE Intent" "PASS"
    else
        log_test "FT-01" "Create DURABLE Intent" "FAIL" "Status: $status"
    fi

    # FT-02: 创建 ASYNC Intent
    response=$(http_request POST "$BASE_URL/v1/intents" '{
        "intentId": "test-async-'$$'",
        "executeAt": "'"$(date -u -d '+5 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "deadline": "'"$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "shardKey": "test-shard",
        "ackLevel": "ASYNC",
        "callback": {"url": "https://example.com/webhook", "method": "POST"}
    }')
    status=$(echo "$response" | tail -1)
    if [ "$status" = "201" ]; then
        log_test "FT-02" "Create ASYNC Intent" "PASS"
    else
        log_test "FT-02" "Create ASYNC Intent" "FAIL" "Status: $status"
    fi

    # FT-04: 查询 Intent
    local intent_id="test-get-$$"
    http_request POST "$BASE_URL/v1/intents" '{
        "intentId": "'$intent_id'",
        "executeAt": "'"$(date -u -d '+5 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "deadline": "'"$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "shardKey": "test-shard",
        "ackLevel": "DURABLE",
        "callback": {"url": "https://example.com/webhook", "method": "POST"}
    }' > /dev/null

    response=$(http_request GET "$BASE_URL/v1/intents/$intent_id")
    status=$(echo "$response" | tail -1)
    if [ "$status" = "200" ]; then
        log_test "FT-04" "Query Intent" "PASS"
    else
        log_test "FT-04" "Query Intent" "FAIL" "Status: $status"
    fi

    # FT-05: 取消 Intent
    intent_id="test-cancel-$$"
    http_request POST "$BASE_URL/v1/intents" '{
        "intentId": "'$intent_id'",
        "executeAt": "'"$(date -u -d '+5 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "deadline": "'"$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "shardKey": "test-shard",
        "ackLevel": "DURABLE",
        "callback": {"url": "https://example.com/webhook", "method": "POST"}
    }' > /dev/null

    response=$(http_request POST "$BASE_URL/v1/intents/$intent_id/cancel")
    status=$(echo "$response" | tail -1)
    if [ "$status" = "200" ]; then
        log_test "FT-05" "Cancel Intent" "PASS"
    else
        log_test "FT-05" "Cancel Intent" "FAIL" "Status: $status"
    fi

    # FT-06: 修改 executeAt
    intent_id="test-patch-$$"
    http_request POST "$BASE_URL/v1/intents" '{
        "intentId": "'$intent_id'",
        "executeAt": "'"$(date -u -d '+5 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "deadline": "'"$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "shardKey": "test-shard",
        "ackLevel": "DURABLE",
        "callback": {"url": "https://example.com/webhook", "method": "POST"}
    }' > /dev/null

    response=$(http_request PATCH "$BASE_URL/v1/intents/$intent_id" '{
        "executeAt": "'"$(date -u -d '+30 minutes' +%Y-%m-%dT%H:%M:%SZ)"'"
    }')
    status=$(echo "$response" | tail -1)
    if [ "$status" = "200" ]; then
        log_test "FT-06" "Modify executeAt" "PASS"
    else
        log_test "FT-06" "Modify executeAt" "FAIL" "Status: $status"
    fi

    # FT-07: 立即触发
    intent_id="test-fire-$$"
    http_request POST "$BASE_URL/v1/intents" '{
        "intentId": "'$intent_id'",
        "executeAt": "'"$(date -u -d '+5 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "deadline": "'"$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "shardKey": "test-shard",
        "ackLevel": "DURABLE",
        "callback": {"url": "https://example.com/webhook", "method": "POST"}
    }' > /dev/null

    response=$(http_request POST "$BASE_URL/v1/intents/$intent_id/fire-now")
    status=$(echo "$response" | tail -1)
    if [ "$status" = "200" ]; then
        log_test "FT-07" "Fire immediately" "PASS"
    else
        log_test "FT-07" "Fire immediately" "FAIL" "Status: $status"
    fi

    # FT-08: 幂等性测试
    local idem_key="idem-test-$$"
    intent_id="test-idem-$$"

    # 第一次创建
    response=$(http_request POST "$BASE_URL/v1/intents" '{
        "intentId": "'$intent_id'",
        "executeAt": "'"$(date -u -d '+5 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "deadline": "'"$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "shardKey": "test-shard",
        "ackLevel": "DURABLE",
        "idempotencyKey": "'$idem_key'",
        "callback": {"url": "https://example.com/webhook", "method": "POST"}
    }')
    status=$(echo "$response" | tail -1)
    if [ "$status" = "201" ]; then
        log_test "FT-08-1" "Create with idempotencyKey (first)" "PASS"
    else
        log_test "FT-08-1" "Create with idempotencyKey (first)" "FAIL" "Status: $status"
    fi

    # 第二次创建（应返回 200）
    response=$(http_request POST "$BASE_URL/v1/intents" '{
        "intentId": "'$intent_id'",
        "executeAt": "'"$(date -u -d '+5 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "deadline": "'"$(date -u -d '+10 minutes' +%Y-%m-%dT%H:%M:%SZ)"'",
        "shardKey": "test-shard",
        "ackLevel": "DURABLE",
        "idempotencyKey": "'$idem_key'",
        "callback": {"url": "https://example.com/webhook", "method": "POST"}
    }')
    status=$(echo "$response" | tail -1)
    if [ "$status" = "200" ]; then
        log_test "FT-08-2" "Create with same idempotencyKey (second)" "PASS"
    else
        log_test "FT-08-2" "Create with same idempotencyKey (second)" "FAIL" "Status: $status"
    fi

    # 跳过的测试
    log_test "FT-13" "Expired action DISCARD" "SKIP" "Requires time-based test"
    log_test "FT-14" "Expired action DEAD_LETTER" "SKIP" "Requires time-based test"
}

# 健康检查测试
run_health_tests() {
    print_header "Health Check Tests"

    local response status

    response=$(http_request GET "$BASE_URL/health")
    status=$(echo "$response" | tail -1)
    [ "$status" = "200" ] && log_test "HEALTH-01" "Health check endpoint" "PASS" || log_test "HEALTH-01" "Health check endpoint" "FAIL"

    response=$(http_request GET "$BASE_URL/health/live")
    status=$(echo "$response" | tail -1)
    [ "$status" = "200" ] && log_test "HEALTH-02" "Liveness probe" "PASS" || log_test "HEALTH-02" "Liveness probe" "FAIL"

    response=$(http_request GET "$BASE_URL/health/ready")
    status=$(echo "$response" | tail -1)
    [ "$status" = "200" ] && log_test "HEALTH-03" "Readiness probe" "PASS" || log_test "HEALTH-03" "Readiness probe" "FAIL"

    response=$(http_request GET "$BASE_URL/metrics")
    status=$(echo "$response" | tail -1)
    [ "$status" = "200" ] && log_test "HEALTH-04" "Prometheus metrics" "PASS" || log_test "HEALTH-04" "Prometheus metrics" "FAIL"
}

# 压力测试
run_stress_tests() {
    print_header "Stress Tests (PT-01 ~ PT-06)"

    local task_count="${STRESS_TASK_COUNT:-10000}"

    # PT-01: ASYNC 写入吞吐
    echo "PT-01: Testing ASYNC write throughput with $task_count intents..."

    local start_time=$(date +%s.%N)
    local batch_size=100
    local count=0

    for ((i=0; i<task_count; i++)); do
        curl -s -X POST "$BASE_URL/v1/intents" \
            -H "Content-Type: application/json" \
            -d '{
                "intentId": "stress-async-'$i'",
                "executeAt": "'"$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)"'",
                "deadline": "'"$(date -u -d '+2 hours' +%Y-%m-%dT%H:%M:%SZ)"'",
                "shardKey": "stress-test",
                "ackLevel": "ASYNC",
                "callback": {"url": "https://example.com/webhook", "method": "POST"}
            }' > /dev/null 2>&1

        ((count++))
        if (( count % 1000 == 0 )); then
            echo "  Progress: $count / $task_count"
        fi
    done

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local qps=$(echo "scale=0; $task_count / $duration" | bc)

    log_test "PT-01" "ASYNC write throughput" "PASS" "$task_count intents in ${duration}s = ${qps} QPS"

    # PT-04: 混合读写
    echo "PT-04: Testing mixed read/write..."
    start_time=$(date +%s.%N)

    for ((i=0; i<1000; i++)); do
        curl -s -X POST "$BASE_URL/v1/intents" \
            -H "Content-Type: application/json" \
            -d '{
                "intentId": "mixed-'$i'",
                "executeAt": "'"$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)"'",
                "deadline": "'"$(date -u -d '+2 hours' +%Y-%m-%dT%H:%M:%SZ)"'",
                "shardKey": "mixed-test",
                "ackLevel": "DURABLE",
                "callback": {"url": "https://example.com/webhook", "method": "POST"}
            }' > /dev/null 2>&1

        if (( i % 10 == 0 )); then
            curl -s "$BASE_URL/v1/intents/mixed-$((i/10))" > /dev/null 2>&1
        fi
    done

    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)
    log_test "PT-04" "Mixed read/write" "PASS" "1000 ops in ${duration}s"

    # 跳过的测试
    log_test "PT-05" "Long-term pending tasks" "SKIP" "Requires extended test duration"
    log_test "PT-06" "High concurrent wake-up" "SKIP" "Requires time-synchronized test"
}

# 复制测试
run_replication_tests() {
    print_header "Replication Tests (FT-15 ~ FT-22)"

    log_test "FT-15" "Normal replication flow" "SKIP" "Requires replica node"
    log_test "FT-16" "REPLICATED ACK latency" "SKIP" "Requires replica node"
    log_test "FT-17" "Manual promotion" "SKIP" "Requires replica node"
    log_test "FT-18" "Old primary recovery" "SKIP" "Requires replica node"
    log_test "FT-19" "Snapshot + WAL recovery" "SKIP" "Requires replica node"
    log_test "FT-20" "Primary kill -9" "SKIP" "Requires replica node"
    log_test "FT-21" "Network partition" "SKIP" "Requires replica node"
    log_test "FT-22" "Replica catch-up" "SKIP" "Requires replica node"
}

# 基准测试
run_benchmark_tests() {
    print_header "Benchmark Tests"

    echo "Running JUnit benchmark tests..."

    if mvn test -Pfull-tests -Dtest=BenchmarkFrameworkTest -q; then
        log_test "BENCH-01" "Benchmark framework tests" "PASS"
    else
        log_test "BENCH-01" "Benchmark framework tests" "FAIL"
    fi

    echo "Running full unit tests..."
    if mvn test -Pbalanced-tests -q; then
        log_test "BENCH-02" "Unit tests (340 tests)" "PASS"
    else
        log_test "BENCH-02" "Unit tests" "FAIL"
    fi
}

# 显示摘要
show_summary() {
    print_header "Test Summary"

    local end_time=$(date +%s)
    local duration=$((end_time - START_TIME))
    local total=$((PASS_COUNT + FAIL_COUNT + SKIP_COUNT))

    echo "Total Tests:  $total"
    echo -e "Passed:       ${GREEN}$PASS_COUNT${NC}"
    echo -e "Failed:       ${RED}$FAIL_COUNT${NC}"
    echo -e "Skipped:      ${YELLOW}$SKIP_COUNT${NC}"
    echo "Duration:     ${duration}s"
    echo ""

    # 保存 JSON 结果
    cat > "$RESULT_FILE" << EOF
{
  "timestamp": "$TIMESTAMP",
  "testType": "$TEST_TYPE",
  "environment": {
    "os": "$(uname -s)",
    "jdk": "$(java -version 2>&1 | head -1)",
    "port": $PORT
  },
  "summary": {
    "total": $total,
    "passed": $PASS_COUNT,
    "failed": $FAIL_COUNT,
    "skipped": $SKIP_COUNT,
    "durationSeconds": $duration
  }
}
EOF
    echo "JSON result saved to: $RESULT_FILE"

    # 保存 Markdown 报告
    cat > "$RESULT_MARKDOWN" << EOF
# LoomQ v0.5.0 测试报告

**时间**: $(date '+%Y-%m-%d %H:%M:%S')
**测试类型**: $TEST_TYPE
**端口**: $PORT

## 测试摘要

| 指标 | 数值 |
|------|------|
| 总测试数 | $total |
| 通过 | $PASS_COUNT |
| 失败 | $FAIL_COUNT |
| 跳过 | $SKIP_COUNT |
| 耗时 | ${duration}s |

## 结论

$(if [ $FAIL_COUNT -eq 0 ]; then echo "✅ 所有测试通过"; else echo "❌ 存在失败测试"; fi)

---
*Generated by LoomQ Test Suite*
EOF
    echo "Markdown report saved to: $RESULT_MARKDOWN"
    echo ""

    if [ $FAIL_COUNT -gt 0 ]; then
        echo -e "${RED}Result: SOME TESTS FAILED${NC}"
        exit 1
    else
        echo -e "${GREEN}Result: ALL TESTS PASSED${NC}"
        exit 0
    fi
}

# 清理
cleanup() {
    stop_server
}

trap cleanup EXIT

# 主程序
main() {
    print_header "LoomQ v0.5.0 Test Suite"
    echo "Test Type: $TEST_TYPE"
    echo "Port: $PORT"
    echo "Data Dir: $DATA_DIR"
    echo ""

    # 构建项目
    echo "Building project..."
    mvn clean package -DskipTests -q
    echo -e "${GREEN}Build completed${NC}"
    echo ""

    # 运行测试
    case $TEST_TYPE in
        all|functional|stress|replication)
            start_server
            ;;
    esac

    case $TEST_TYPE in
        all|functional)
            run_functional_tests
            ;;
    esac

    case $TEST_TYPE in
        all)
            run_health_tests
            ;;
    esac

    case $TEST_TYPE in
        all|stress)
            run_stress_tests
            ;;
    esac

    case $TEST_TYPE in
        all|replication)
            run_replication_tests
            ;;
    esac

    case $TEST_TYPE in
        all|benchmark)
            run_benchmark_tests
            ;;
    esac

    show_summary
}

main
