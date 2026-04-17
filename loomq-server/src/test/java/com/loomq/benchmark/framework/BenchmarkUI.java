package com.loomq.benchmark.framework;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 基准测试 UI 渲染器
 *
 * 设计原则：
 * 1. 人类视角：视觉层次、颜色编码、ASCII图表、关键指标一目了然
 * 2. AI视角：JSON结构化输出、可预测的字段名、完整原始数据
 *
 * @author loomq
 * @since v0.6.2
 */
public class BenchmarkUI {

    // ==================== 颜色定义 (ANSI) ====================

    // 前景色
    public static final String RESET = "\u001B[0m";
    public static final String BOLD = "\u001B[1m";
    public static final String DIM = "\u001B[2m";

    public static final String BLACK = "\u001B[30m";
    public static final String RED = "\u001B[31m";
    public static final String GREEN = "\u001B[32m";
    public static final String YELLOW = "\u001B[33m";
    public static final String BLUE = "\u001B[34m";
    public static final String MAGENTA = "\u001B[35m";
    public static final String CYAN = "\u001B[36m";
    public static final String WHITE = "\u001B[37m";

    // 亮色
    public static final String BRIGHT_RED = "\u001B[91m";
    public static final String BRIGHT_GREEN = "\u001B[92m";
    public static final String BRIGHT_YELLOW = "\u001B[93m";
    public static final String BRIGHT_BLUE = "\u001B[94m";
    public static final String BRIGHT_MAGENTA = "\u001B[95m";
    public static final String BRIGHT_CYAN = "\u001B[96m";

    // 背景色
    public static final String BG_GREEN = "\u001B[42m";
    public static final String BG_RED = "\u001B[41m";
    public static final String BG_YELLOW = "\u001B[43m";

    // Unicode 图标
    public static final String CHECK = "✓";
    public static final String CROSS = "✗";
    public static final String ARROW_RIGHT = "→";
    public static final String ARROW_UP = "↑";
    public static final String ARROW_DOWN = "↓";
    public static final String BULLET = "•";
    public static final String DIAMOND = "◆";
    public static final String SQUARE = "■";
    public static final String LIGHTNING = "⚡";
    public static final String CLOCK = "⏱";
    public static final String CHART = "📊";
    public static final String ROCKET = "🚀";
    public static final String PACKAGE = "📦";
    public static final String GEAR = "⚙";
    public static final String SERVER = "🖥";

    // 线条字符
    public static final String H_LINE = "─";
    public static final String H_DOUBLE = "═";
    public static final String V_LINE = "│";
    public static final String TL_CORNER = "┌";
    public static final String TR_CORNER = "┐";
    public static final String BL_CORNER = "└";
    public static final String BR_CORNER = "┘";
    public static final String T_JOIN = "┬";
    public static final String B_JOIN = "┴";
    public static final String L_JOIN = "├";
    public static final String R_JOIN = "┤";

    private final boolean useColor;
    private final int terminalWidth;

    public BenchmarkUI() {
        this(true, 80);
    }

    public BenchmarkUI(boolean useColor, int width) {
        this.useColor = useColor;
        this.terminalWidth = width;
    }

    // ==================== 横幅和标题 ====================

    /**
     * 打印主横幅
     */
    public void printBanner(String version) {
        String[] banner = {
            "",
            boxLine("═", terminalWidth, MAGENTA),
            centerText("LoomQ Performance Benchmark", terminalWidth, MAGENTA + BOLD),
            centerText("v" + version, terminalWidth, DIM),
            boxLine("═", terminalWidth, MAGENTA),
            ""
        };

        for (String line : banner) {
            println(line);
        }
    }

    /**
     * 打印章节标题
     */
    public void printSectionHeader(String title, String category) {
        println("");
        println(color(CYAN + BOLD, TL_CORNER + H_LINE.repeat(terminalWidth - 2) + TR_CORNER));
        println(color(CYAN + BOLD, V_LINE) + " " + color(YELLOW + BOLD, title) + dim(" [" + category + "]"));
        println(color(CYAN + BOLD, L_JOIN + H_LINE.repeat(terminalWidth - 2) + R_JOIN));
    }

    /**
     * 打印子标题
     */
    public void printSubHeader(String title) {
        println("");
        println(color(BLUE, "  " + DIAMOND + " " + title));
        println(color(DIM, "  " + H_LINE.repeat(40)));
    }

    // ==================== 测试结果展示 ====================

    /**
     * 打印单个测试结果 (紧凑格式)
     */
    public void printTestResult(BenchmarkResult result) {
        String name = result.name();
        String status = result.failureCount() == 0 ? success(CHECK) : warning(CROSS);

        // 主要指标
        String qps = formatQps(result.qps());
        String avg = formatLatency(result.avgLatencyMs());
        String p99 = formatLatency(result.p99LatencyMs());

        // 性能评级
        String grade = getPerformanceGrade(result.qps(), result.p99LatencyMs(), result.category());

        println(String.format("  %s %-35s %s  %s QPS  %s Avg  %s P99  %s",
            status,
            truncate(name, 35),
            grade,
            color(qpsColor(result.qps()), qps),
            color(latencyColor(result.avgLatencyMs()), avg),
            color(latencyColor(result.p99LatencyMs()), p99),
            dim(result.threads() + "t")));
    }

    /**
     * 打印测试结果表格
     */
    public void printResultTable(List<BenchmarkResult> results, String categoryName) {
        if (results == null || results.isEmpty()) return;

        printSectionHeader(categoryName, results.get(0).category());

        // 表头
        String header = String.format("  %-35s  %10s  %8s  %8s  %8s  %6s",
            "Test Name", "QPS", "Avg(ms)", "P99(ms)", "Grade", "Threads");
        println(dim(header));
        println(dim("  " + H_LINE.repeat(85)));

        // 数据行
        for (BenchmarkResult r : results) {
            printTestResult(r);
        }
    }

    // ==================== ASCII 图表 ====================

    /**
     * 打印 QPS 柱状图
     */
    public void printQpsChart(List<BenchmarkResult> results, String title) {
        println("");
        println(color(BOLD, "  " + CHART + " " + title));
        println("");

        double maxQps = results.stream()
            .mapToDouble(BenchmarkResult::qps)
            .max().orElse(1);

        int chartWidth = 40;

        for (BenchmarkResult r : results) {
            if (r.qps() <= 0) continue;

            String name = truncate(r.name(), 20);
            int barLength = (int) (r.qps() / maxQps * chartWidth);
            String bar = "█".repeat(Math.max(1, barLength));

            println(String.format("  %-20s %s%s %s",
                dim(name),
                color(qpsColor(r.qps()), bar),
                RESET,
                formatQps(r.qps())));
        }
    }

    /**
     * 打印延迟分布图
     */
    public void printLatencyDistribution(long[] latencies, String title) {
        if (latencies == null || latencies.length == 0) return;

        println("");
        println(color(BOLD, "  " + CLOCK + " " + title));
        println("");

        // 计算百分位
        long[] sorted = latencies.clone();
        Arrays.sort(sorted);

        double p50 = percentile(sorted, 50);
        double p90 = percentile(sorted, 90);
        double p95 = percentile(sorted, 95);
        double p99 = percentile(sorted, 99);
        double max = sorted[sorted.length - 1];

        // 打印分布
        println("  " + dim("P50") + " " + latencyBar(p50, max) + " " + formatLatency(p50));
        println("  " + dim("P90") + " " + latencyBar(p90, max) + " " + formatLatency(p90));
        println("  " + dim("P95") + " " + latencyBar(p95, max) + " " + formatLatency(p95));
        println("  " + dim("P99") + " " + latencyBar(p99, max) + " " + formatLatency(p99));
    }

    private String latencyBar(double value, double max) {
        int width = 30;
        int filled = max > 0 ? (int) (value / max * width) : 0;
        String bar = "█".repeat(filled) + "░".repeat(width - filled);
        return color(latencyColor(value), bar);
    }

    // ==================== 汇总面板 ====================

    /**
     * 打印汇总面板
     */
    public void printSummaryPanel(List<BenchmarkResult> results, String version, String gitCommit) {
        println("");
        println(color(GREEN + BOLD, TL_CORNER + H_DOUBLE.repeat(50) + TR_CORNER));
        println(color(GREEN + BOLD, V_LINE) + centerText("SUMMARY", 50, BOLD));
        println(color(GREEN + BOLD, V_LINE + H_DOUBLE.repeat(50) + V_LINE));

        // 关键指标
        double maxQps = results.stream()
            .filter(r -> r.qps() > 0)
            .mapToDouble(BenchmarkResult::qps)
            .max().orElse(0);

        double minP99 = results.stream()
            .filter(r -> r.p99LatencyMs() > 0)
            .mapToDouble(BenchmarkResult::p99LatencyMs)
            .min().orElse(0);

        int totalTests = results.size();
        int passedTests = (int) results.stream().filter(r -> r.failureCount() == 0).count();

        println(color(GREEN + BOLD, V_LINE) + "  " + ROCKET + " Peak QPS:    " + color(BRIGHT_GREEN, formatQps(maxQps)));
        println(color(GREEN + BOLD, V_LINE) + "  " + LIGHTNING + " Best P99:    " + color(BRIGHT_CYAN, formatLatency(minP99)));
        println(color(GREEN + BOLD, V_LINE) + "  " + CHECK + " Tests:       " + color(BRIGHT_GREEN, passedTests + "") + dim("/" + totalTests + " passed"));
        println(color(GREEN + BOLD, V_LINE) + "  " + PACKAGE + " Version:     " + dim(version));
        println(color(GREEN + BOLD, V_LINE) + "  " + GEAR + " Git:         " + dim(gitCommit));
        println(color(GREEN + BOLD, BL_CORNER + H_DOUBLE.repeat(50) + BR_CORNER));
    }

    /**
     * 打印瓶颈分析面板
     */
    public void printBottleneckAnalysis(List<BenchmarkResult> results) {
        println("");
        println(color(YELLOW + BOLD, TL_CORNER + H_LINE.repeat(60) + TR_CORNER));
        println(color(YELLOW + BOLD, V_LINE) + " " + color(BOLD, "BOTTLENECK ANALYSIS"));
        println(color(YELLOW + BOLD, L_JOIN + H_LINE.repeat(60) + R_JOIN));

        // 按类别分组
        Map<String, List<BenchmarkResult>> byCategory = new LinkedHashMap<>();
        for (BenchmarkResult r : results) {
            byCategory.computeIfAbsent(r.category(), k -> new ArrayList<>()).add(r);
        }

        // 计算各层性能
        double storageQps = getMaxQps(byCategory.get("storage"), "save");
        double schedulerQps = getMaxQps(byCategory.get("scheduler_add"), "");
        double httpQps = getMaxQps(byCategory.get("http"), "POST");

        // 打印层级图
        printLayerLine("HTTP Layer", httpQps, httpQps > 0 ? storageQps / httpQps : 0);
        printLayerLine("JSON Layer", 0, 0);
        printLayerLine("Storage Layer", storageQps, storageQps > 0 && schedulerQps > 0 ? storageQps / schedulerQps : 0);
        printLayerLine("Scheduler Layer", schedulerQps, 0);

        println(color(YELLOW + BOLD, BL_CORNER + H_LINE.repeat(60) + BR_CORNER));

        // 瓶颈判断
        if (httpQps > 0 && storageQps > 0) {
            double ratio = storageQps / httpQps;
            println("");
            if (ratio > 50) {
                println(warning("  " + ARROW_RIGHT + " HTTP+JSON layer is the bottleneck (") +
                    color(RED, String.format("%.0fx", ratio)) + warning(" overhead)"));
            } else if (ratio > 10) {
                println(info("  " + ARROW_RIGHT + " Protocol overhead: ") + color(YELLOW, String.format("%.0fx", ratio)));
            } else {
                println(success("  " + ARROW_RIGHT + " Well balanced system"));
            }
        }
    }

    private void printLayerLine(String layer, double qps, double ratio) {
        String qpsStr = qps > 0 ? formatQps(qps) : dim("N/A");
        String ratioStr = ratio > 0 ? color(YELLOW, String.format("(%.0fx)", ratio)) : "";
        println(color(YELLOW + BOLD, V_LINE) + String.format("  %-20s %12s %s", layer, qpsStr, ratioStr));
    }

    // ==================== 历史对比 ====================

    /**
     * 打印历史趋势
     */
    public void printHistoryTrend(String testName, double prevQps, double currQps) {
        double change = prevQps > 0 ? (currQps - prevQps) / prevQps * 100 : 0;

        String trend;
        String color;
        if (change > 5) {
            trend = ARROW_UP + ARROW_UP;
            color = GREEN;
        } else if (change < -5) {
            trend = ARROW_DOWN + ARROW_DOWN;
            color = RED;
        } else {
            trend = ARROW_RIGHT;
            color = DIM;
        }

        println(String.format("  %s %-30s: %s %s %s %s",
            color(color, trend),
            truncate(testName, 30),
            dim(formatQps(prevQps)),
            color(color, ARROW_RIGHT),
            formatQps(currQps),
            color(change >= 0 ? GREEN : RED, String.format("%+.1f%%", change))));
    }

    // ==================== 进度和状态 ====================

    /**
     * 打印测试进度
     */
    public void printTestProgress(int current, int total, String testName) {
        int percent = total > 0 ? current * 100 / total : 0;
        String bar = "█".repeat(percent / 5) + "░".repeat(20 - percent / 5);
        print("\r  " + dim("[" + current + "/" + total + "]") + " " + color(CYAN, bar) + " " + testName + "   ");
    }

    /**
     * 打印完成状态
     */
    public void printComplete() {
        println("\r  " + success(CHECK + " Complete") + "                                        ");
    }

    /**
     * 打印错误
     */
    public void printError(String message) {
        println("");
        println(color(BG_RED + WHITE, " " + CROSS + " ERROR ") + " " + color(RED, message));
    }

    /**
     * 打印警告
     */
    public void printWarning(String message) {
        println(color(BG_YELLOW + BLACK, " " + "!" + " WARN ") + " " + color(YELLOW, message));
    }

    /**
     * 打印信息
     */
    public void printInfo(String message) {
        println(info("  " + BULLET + " " + message));
    }

    // ==================== AI 输出 (JSON) ====================

    /**
     * 生成 JSON 报告 (给 AI/CI 使用)
     */
    public String generateJsonReport(List<BenchmarkResult> results, String version, String gitCommit) {
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"meta\": {\n");
        json.append("    \"version\": \"").append(version).append("\",\n");
        json.append("    \"gitCommit\": \"").append(gitCommit).append("\",\n");
        json.append("    \"timestamp\": \"").append(Instant.now()).append("\",\n");
        json.append("    \"totalTests\": ").append(results.size()).append("\n");
        json.append("  },\n");

        // 汇总指标
        json.append("  \"summary\": {\n");
        json.append("    \"peakQps\": ").append(results.stream()
            .filter(r -> r.qps() > 0).mapToDouble(BenchmarkResult::qps).max().orElse(0)).append(",\n");
        json.append("    \"bestP99Ms\": ").append(results.stream()
            .filter(r -> r.p99LatencyMs() > 0).mapToDouble(BenchmarkResult::p99LatencyMs).min().orElse(0)).append(",\n");
        json.append("    \"passedTests\": ").append(results.stream()
            .filter(r -> r.failureCount() == 0).count()).append("\n");
        json.append("  },\n");

        // 按类别分组的结果
        json.append("  \"results\": {\n");
        Map<String, List<BenchmarkResult>> byCategory = new LinkedHashMap<>();
        for (BenchmarkResult r : results) {
            byCategory.computeIfAbsent(r.category(), k -> new ArrayList<>()).add(r);
        }

        int catIndex = 0;
        for (Map.Entry<String, List<BenchmarkResult>> entry : byCategory.entrySet()) {
            json.append("    \"").append(entry.getKey()).append("\": [\n");
            int i = 0;
            for (BenchmarkResult r : entry.getValue()) {
                json.append("      ");
                json.append(resultToJson(r));
                if (i < entry.getValue().size() - 1) json.append(",");
                json.append("\n");
                i++;
            }
            json.append("    ]");
            if (catIndex < byCategory.size() - 1) json.append(",");
            json.append("\n");
            catIndex++;
        }
        json.append("  }\n");
        json.append("}\n");

        return json.toString();
    }

    /**
     * 保存 JSON 报告到文件
     */
    public void saveJsonReport(List<BenchmarkResult> results, String version, String gitCommit, Path path) throws IOException {
        String json = generateJsonReport(results, version, gitCommit);
        Files.writeString(path, json);
    }

    private String resultToJson(BenchmarkResult r) {
        return String.format(
            "{\"name\": \"%s\", \"qps\": %.2f, \"avgMs\": %.3f, \"p50Ms\": %.3f, \"p95Ms\": %.3f, \"p99Ms\": %.3f, \"threads\": %d, \"success\": %d, \"failed\": %d}",
            escapeJson(r.name()), r.qps(), r.avgLatencyMs(), r.p50LatencyMs(),
            r.p95LatencyMs(), r.p99LatencyMs(), r.threads(), r.successCount(), r.failureCount()
        );
    }

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // ==================== 辅助方法 ====================

    private String color(String colorCode, String text) {
        return useColor ? colorCode + text + RESET : text;
    }

    private String success(String text) {
        return color(GREEN, text);
    }

    private String warning(String text) {
        return color(YELLOW, text);
    }

    private String info(String text) {
        return color(BLUE, text);
    }

    private String dim(String text) {
        return color(DIM, text);
    }

    private String bold(String text) {
        return color(BOLD, text);
    }

    private String centerText(String text, int width, String colorCode) {
        int padding = (width - text.length()) / 2;
        String spaces = " ".repeat(Math.max(0, padding));
        return color(colorCode, spaces + text + spaces);
    }

    private String boxLine(String c, int width, String colorCode) {
        return color(colorCode, c.repeat(width));
    }

    private String truncate(String s, int maxLen) {
        return s.length() > maxLen ? s.substring(0, maxLen - 2) + ".." : s;
    }

    private String formatQps(double qps) {
        if (qps >= 1_000_000) return String.format("%.1fM", qps / 1_000_000);
        if (qps >= 1_000) return String.format("%.1fK", qps / 1_000);
        return String.format("%.0f", qps);
    }

    private String formatLatency(double ms) {
        if (ms >= 1000) return String.format("%.1fs", ms / 1000);
        if (ms >= 1) return String.format("%.1fms", ms);
        return String.format("%.2fms", ms);
    }

    private String qpsColor(double qps) {
        if (qps >= 100_000) return BRIGHT_GREEN;
        if (qps >= 10_000) return GREEN;
        if (qps >= 1_000) return CYAN;
        if (qps >= 100) return YELLOW;
        return RED;
    }

    private String latencyColor(double ms) {
        if (ms <= 1) return BRIGHT_GREEN;
        if (ms <= 10) return GREEN;
        if (ms <= 50) return YELLOW;
        if (ms <= 100) return BRIGHT_YELLOW;
        return RED;
    }

    private String getPerformanceGrade(double qps, double p99, String category) {
        // 根据类别和性能给出评级
        double score = 0;

        if (category.contains("storage") || category.contains("scheduler")) {
            if (qps >= 500_000) score += 3;
            else if (qps >= 100_000) score += 2;
            else if (qps >= 10_000) score += 1;
        } else if (category.contains("http")) {
            if (qps >= 50_000) score += 3;
            else if (qps >= 10_000) score += 2;
            else if (qps >= 1_000) score += 1;
        } else if (category.contains("ringbuffer")) {
            if (qps >= 10_000_000) score += 3;
            else if (qps >= 1_000_000) score += 2;
            else if (qps >= 100_000) score += 1;
        } else {
            if (qps >= 100_000) score += 3;
            else if (qps >= 10_000) score += 2;
            else if (qps >= 1_000) score += 1;
        }

        if (p99 > 0 && p99 <= 1) score += 1;
        else if (p99 > 100) score -= 1;

        if (score >= 3) return color(BRIGHT_GREEN, "A");
        if (score >= 2) return color(GREEN, "B");
        if (score >= 1) return color(YELLOW, "C");
        return color(RED, "D");
    }

    private double percentile(long[] sorted, int p) {
        if (sorted.length == 0) return 0;
        int index = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, index)];
    }

    private double getMaxQps(List<BenchmarkResult> results, String filter) {
        if (results == null) return 0;
        return results.stream()
            .filter(r -> filter.isEmpty() || r.name().contains(filter))
            .mapToDouble(BenchmarkResult::qps)
            .max()
            .orElse(0);
    }

    private void println(String text) {
        System.out.println(text);
    }

    private void print(String text) {
        System.out.print(text);
    }
}
