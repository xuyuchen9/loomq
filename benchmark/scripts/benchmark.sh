#!/bin/bash
# LoomQ 性能基准测试脚本
# 用法: ./benchmark.sh [选项]
#
# 选项:
#   --internal    仅运行内部组件测试（无需启动服务）
#   --http        仅运行 HTTP 接口测试（需要启动服务）
#   --quick       快速测试模式（减少测试时长）
#   --compare     仅显示历史对比
#   --save        自动保存结果
#   --no-save     预览模式（不保存结果）

set -euo pipefail

QUICK_MODE=false
TEST_MODE="all"
AUTO_SAVE=false
NO_SAVE=false

for arg in "$@"; do
    case $arg in
        --internal) TEST_MODE="internal" ;;
        --http) TEST_MODE="http" ;;
        --quick) QUICK_MODE=true ;;
        --compare)
            echo "历史性能对比:"
            echo ""
            cat benchmark/results/history.csv 2>/dev/null || echo "暂无历史记录"
            exit 0
            ;;
        --save) AUTO_SAVE=true ;;
        --no-save) NO_SAVE=true ;;
        *)
            echo "未知选项: $arg"
            echo "用法: $0 [--internal] [--http] [--quick] [--compare] [--save] [--no-save]"
            exit 1
            ;;
    esac
done

echo ""
echo "+--------------------------------------------------------------+"
echo "|          LoomQ 原子化性能基准测试 v0.7.0                     |"
echo "+--------------------------------------------------------------+"
echo ""
echo "测试模式: $TEST_MODE"
echo "快速模式: $QUICK_MODE"
echo ""

# 编译项目
echo ">>> 编译项目..."
mvn clean compile -q -DskipTests
if [ $? -ne 0 ]; then
    echo "[错误] 编译失败"
    exit 1
fi

# 运行基准测试
echo ">>> 运行基准测试..."
echo ""

(
    cd loomq-server
    if [ "$TEST_MODE" = "internal" ]; then
        mvn exec:java -Dexec.mainClass="com.loomq.benchmark.InternalBenchmark" -Dexec.classpathScope=test -q
    elif [ "$QUICK_MODE" = true ]; then
        mvn exec:java -Dexec.mainClass="com.loomq.benchmark.QuickBenchmark" -Dexec.classpathScope=test -q
    else
        mvn exec:java -Dexec.mainClass="com.loomq.benchmark.ExtremeBenchmark" -Dexec.classpathScope=test -q
    fi
)

echo ""
echo "[成功] 测试完成"
echo ""
echo "报告已保存到: benchmark/results/"
echo ""
echo "提示: 使用 --compare 查看历史对比"
