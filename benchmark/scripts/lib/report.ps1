# report.ps1 — Excel + MD 报告生成
# 依赖: Python 3 + xlsxwriter (pip install xlsxwriter)

$Script:ReportDir = Join-Path $PSScriptRoot "..\..\results\reports"
$Script:PythonScript = Join-Path $PSScriptRoot "gen_excel.py"

function New-BenchmarkExcel {
    <#
    .SYNOPSIS
    生成 benchmark Excel 报告 (.xlsx)
    .PARAMETER Data
    解析后的 benchmark 数据 (hashtable)
    .PARAMETER OutputPath
    输出路径 (可选，默认 results/reports/benchmark-report-{timestamp}.xlsx)
    #>
    param(
        [hashtable]$Data,
        [string]$OutputPath
    )

    if (-not $OutputPath) {
        $ts = $Data.Timestamp ?? (Get-Date -Format "yyyyMMdd-HHmmss")
        if (-not (Test-Path $Script:ReportDir)) {
            New-Item -ItemType Directory -Path $Script:ReportDir -Force | Out-Null
        }
        $OutputPath = Join-Path $Script:ReportDir "benchmark-report-$ts.xlsx"
    }

    # 生成 JSON 中间文件供 Python 读取
    $jsonPath = [System.IO.Path]::ChangeExtension($OutputPath, ".json")
    $Data | ConvertTo-Json -Depth 10 | Set-Content -Path $jsonPath -Encoding UTF8

    # 调用 Python 生成 Excel
    $pyScript = $Script:PythonScript
    if (-not (Test-Path $pyScript)) {
        Write-Warning "Excel 生成脚本不存在: $pyScript"
        return $null
    }

    try {
        $result = & python $pyScript $jsonPath $OutputPath 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Excel 生成失败: $result"
            return $null
        }
        Remove-Item -Path $jsonPath -ErrorAction SilentlyContinue
        return $OutputPath
    } catch {
        Write-Warning "Excel 生成异常: $_"
        return $null
    }
}

function New-BenchmarkMarkdown {
    <#
    .SYNOPSIS
    生成 benchmark MD 报告
    .PARAMETER Data
    解析后的 benchmark 数据 (hashtable)
    .PARAMETER OutputPath
    输出路径 (可选)
    #>
    param(
        [hashtable]$Data,
        [string]$OutputPath
    )

    $ts = $Data.Timestamp ?? (Get-Date -Format "yyyyMMdd-HHmmss")
    if (-not $OutputPath) {
        if (-not (Test-Path $Script:ReportDir)) {
            New-Item -ItemType Directory -Path $Script:ReportDir -Force | Out-Null
        }
        $OutputPath = Join-Path $Script:ReportDir "benchmark-report-$ts.md"
    }

    $env_ = $Data.Environment ?? @{}
    $http = $Data.CreatePath?.Http ?? @{}
    $grpc = $Data.CreatePath?.Grpc ?? @{}
    $scheduler = $Data.Scheduler ?? @{}
    $internal = $Data.Internal ?? @{}
    $slo = $Data.SLO ?? @{}

    $sb = [System.Text.StringBuilder]::new()

    # 标题
    [void]$sb.AppendLine("# LoomQ Benchmark Report")
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("**时间**: $($env_.Timestamp ?? $ts)")
    [void]$sb.AppendLine("**Commit**: ``$($env_.Commit ?? 'N/A')`` | **分支**: $($env_.Branch ?? 'N/A')")
    [void]$sb.AppendLine("**Java**: $($env_.JavaVersion ?? 'N/A') | **OS**: $($env_.OS ?? 'N/A') | **CPU**: $($env_.CpuCores ?? 'N/A') cores | **内存**: $($env_.MaxMemory ?? 'N/A')")
    [void]$sb.AppendLine("")

    # 吞吐量对比
    [void]$sb.AppendLine("## 吞吐量对比")
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("| 指标 | HTTP | gRPC |")
    [void]$sb.AppendLine("|------|------|------|")
    $httpPeak = if ($http.PeakQps) { "{0:N0}" -f $http.PeakQps } else { "N/A" }
    $grpcPeak = if ($grpc.PeakQps) { "{0:N0}" -f $grpc.PeakQps } else { "N/A" }
    [void]$sb.AppendLine("| Peak QPS | $httpPeak | $grpcPeak |")
    $httpInflect = $http.InflectionThreads ?? "N/A"
    $grpcInflect = $grpc.InflectionThreads ?? "N/A"
    [void]$sb.AppendLine("| 拐点线程数 | $httpInflect | $grpcInflect |")
    $httpP99 = if ($http.InflectionP99) { "$($http.InflectionP99)ms" } else { "N/A" }
    $grpcP99 = if ($grpc.InflectionP99) { "$($grpc.InflectionP99)ms" } else { "N/A" }
    [void]$sb.AppendLine("| P99 @ 拐点 | $httpP99 | $grpcP99 |")
    $httpFail = if ($null -ne $http.FailRate) { "{0:P2}" -f $http.FailRate } else { "N/A" }
    $grpcFail = if ($null -ne $grpc.FailRate) { "{0:P2}" -f $grpc.FailRate } else { "N/A" }
    [void]$sb.AppendLine("| 失败率 | $httpFail | $grpcFail |")
    [void]$sb.AppendLine("")

    # Create Path 详情
    $httpRows = $http.Rows ?? @()
    $grpcRows = $grpc.Rows ?? @()
    if ($httpRows.Count -gt 0 -or $grpcRows.Count -gt 0) {
        [void]$sb.AppendLine("## Create Path 详情")
        [void]$sb.AppendLine("")
        [void]$sb.AppendLine("| 线程数 | HTTP QPS | HTTP P50 | HTTP P90 | HTTP P99 | gRPC QPS | gRPC P50 | gRPC P90 | gRPC P99 |")
        [void]$sb.AppendLine("|--------|----------|----------|----------|----------|----------|----------|----------|----------|")

        $maxRows = [Math]::Max($httpRows.Count, $grpcRows.Count)
        for ($i = 0; $i -lt $maxRows; $i++) {
            $hr = if ($i -lt $httpRows.Count) { $httpRows[$i] } else { @{} }
            $gr = if ($i -lt $grpcRows.Count) { $grpcRows[$i] } else { @{} }
            $threads = $hr.Threads ?? $gr.Threads ?? "?"
            $hQps = if ($hr.Qps) { "{0:N0}" -f $hr.Qps } else { "-" }
            $hP50 = if ($hr.P50) { $hr.P50 } else { "-" }
            $hP90 = if ($hr.P90) { $hr.P90 } else { "-" }
            $hP99 = if ($hr.P99) { $hr.P99 } else { "-" }
            $gQps = if ($gr.Qps) { "{0:N0}" -f $gr.Qps } else { "-" }
            $gP50 = if ($gr.P50) { $gr.P50 } else { "-" }
            $gP90 = if ($gr.P90) { $gr.P90 } else { "-" }
            $gP99 = if ($gr.P99) { $gr.P99 } else { "-" }
            [void]$sb.AppendLine("| $threads | $hQps | $hP50 | $hP90 | $hP99 | $gQps | $gP50 | $gP90 | $gP99 |")
        }
        [void]$sb.AppendLine("")
    }

    # Scheduler Per-Tier
    $tiers = $scheduler.Tiers ?? @()
    if ($tiers.Count -gt 0) {
        [void]$sb.AppendLine("## 调度精度")
        [void]$sb.AppendLine("")
        [void]$sb.AppendLine("| 档位 | 并发 | QPS | Wake P50 | Wake P95 | Wake P99 | E2E P50 | E2E P95 | E2E P99 | 利用率 | 背压 | 完成率 |")
        [void]$sb.AppendLine("|------|------|-----|----------|----------|----------|---------|---------|---------|--------|------|--------|")
        foreach ($tier in $tiers) {
            $name = $tier.Tier ?? "?"
            $conc = $tier.Concurrency ?? "-"
            $qps = if ($tier.Qps) { "{0:N0}" -f $tier.Qps } else { "-" }
            $wp50 = $tier.WakeP50 ?? "-"
            $wp95 = $tier.WakeP95 ?? "-"
            $wp99 = $tier.WakeP99 ?? "-"
            $ep50 = $tier.E2eP50 ?? "-"
            $ep95 = $tier.E2eP95 ?? "-"
            $ep99 = $tier.E2eP99 ?? "-"
            $util = if ($null -ne $tier.UtilPct) { "{0:P1}" -f ($tier.UtilPct / 100) } else { "-" }
            $bp = if ($null -ne $tier.BackpressurePct) { "{0:P1}" -f ($tier.BackpressurePct / 100) } else { "-" }
            $comp = if ($null -ne $tier.CompletionPct) { "{0:P1}" -f ($tier.CompletionPct / 100) } else { "-" }
            [void]$sb.AppendLine("| $name | $conc | $qps | $wp50 | $wp95 | $wp99 | $ep50 | $ep95 | $ep99 | $util | $bp | $comp |")
        }
        [void]$sb.AppendLine("")
    }

    # 内部组件
    $items = $internal.Items ?? @()
    if ($items.Count -gt 0) {
        [void]$sb.AppendLine("## 内部组件")
        [void]$sb.AppendLine("")
        [void]$sb.AppendLine("| 基准测试 | 指标 | 值 |")
        [void]$sb.AppendLine("|----------|------|-----|")
        foreach ($item in $items) {
            [void]$sb.AppendLine("| $($item.Benchmark) | $($item.Metric) | $($item.Value) |")
        }
        [void]$sb.AppendLine("")
    }

    # SLO 验证
    $sloItems = $slo.Items ?? @()
    if ($sloItems.Count -gt 0) {
        [void]$sb.AppendLine("## SLO 验证")
        [void]$sb.AppendLine("")
        [void]$sb.AppendLine("| 档位 | Wake P95 目标 | Wake P95 实际 | 结果 | Wake P99 目标 | Wake P99 实际 | 结果 | E2E P95 目标 | E2E P95 实际 | 结果 | E2E P99 目标 | E2E P99 实际 | 结果 |")
        [void]$sb.AppendLine("|------|---------------|---------------|------|---------------|---------------|------|--------------|--------------|------|--------------|--------------|------|")
        foreach ($item in $sloItems) {
            $tier = $item.Tier ?? "?"
            $cells = @($tier)
            foreach ($key in @("WakeP95", "WakeP99", "E2eP95", "E2eP99")) {
                $target = $item."${key}Target" ?? "-"
                $actual = $item."${key}Actual" ?? "-"
                $pass = if ($item."${key}Pass") { "PASS" } else { "FAIL" }
                $cells += $target, $actual, $pass
            }
            [void]$sb.AppendLine("| " + ($cells -join " | ") + " |")
        }
        [void]$sb.AppendLine("")
    }

    # 回归对比
    $regression = $Data.Regression
    if ($regression) {
        [void]$sb.AppendLine("## 回归对比（vs 上次运行）")
        [void]$sb.AppendLine("")
        $httpDelta = $regression.HttpQpsDelta
        $grpcDelta = $regression.GrpcQpsDelta
        if ($null -ne $httpDelta) {
            $sign = if ($httpDelta -ge 0) { "+" } else { "" }
            [void]$sb.AppendLine("- HTTP QPS: {0}{1:P1}" -f $sign, $httpDelta
        }
        if ($null -ne $grpcDelta) {
            $sign = if ($grpcDelta -ge 0) { "+" } else { "" }
            [void]$sb.AppendLine("- gRPC QPS: {0}{1:P1}" -f $sign, $grpcDelta
        }
        [void]$sb.AppendLine("")
    }

    $content = $sb.ToString()
    Set-Content -Path $OutputPath -Value $content -Encoding UTF8
    return $OutputPath
}
