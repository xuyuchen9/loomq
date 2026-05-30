# report.ps1 — Excel + MD 报告生成
# 兼容 PowerShell 5.1+
# 依赖: Python 3 + xlsxwriter (pip install xlsxwriter)

$Script:ReportDir = Join-Path $PSScriptRoot "..\..\results\reports"
$Script:PythonScript = Join-Path $PSScriptRoot "gen_excel.py"

function Get-SafeValue {
    param($Value, $Default = $null)
    if ($null -ne $Value) { return $Value }
    if ($null -ne $Default) { return $Default }
    return $null
}

function Format-DeltaLine {
    param([string]$Protocol, [double]$PctVal)
    $sign = '+'
    if ($PctVal -lt 0) { $sign = '' }
    $dash = [char]0x2D
    return "$dash $Protocol QPS: $sign$PctVal%"
}

function New-BenchmarkExcel {
    param(
        [hashtable]$Data,
        [string]$OutputPath
    )

    $ts = Get-SafeValue $Data.Timestamp (Get-Date -Format "yyyyMMdd-HHmmss")
    if (-not $OutputPath) {
        if (-not (Test-Path $Script:ReportDir)) {
            New-Item -ItemType Directory -Path $Script:ReportDir -Force | Out-Null
        }
        $OutputPath = Join-Path $Script:ReportDir "benchmark-report-$ts.xlsx"
    }

    # JSON 写到临时目录，不污染 reports
    $tmpDir = Join-Path $env:TEMP "loomq-bench"
    if (-not (Test-Path $tmpDir)) { New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null }
    $jsonPath = Join-Path $tmpDir "benchmark-$ts.json"
    $Data | ConvertTo-Json -Depth 10 | Set-Content -Path $jsonPath -Encoding UTF8

    $pyScript = $Script:PythonScript
    if (-not (Test-Path $pyScript)) {
        Write-Warning "Excel script not found: $pyScript"
        Remove-Item -Path $jsonPath -ErrorAction SilentlyContinue
        return $null
    }

    try {
        $result = & python $pyScript $jsonPath $OutputPath 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Excel generation failed: $result"
            Remove-Item -Path $jsonPath -ErrorAction SilentlyContinue
            return $null
        }
        Remove-Item -Path $jsonPath -ErrorAction SilentlyContinue
        return $OutputPath
    } catch {
        $errMsg = $_.Exception.Message
        Write-Warning "Excel generation error: $errMsg"
        Write-Warning "Ensure Python 3 + xlsxwriter are installed: pip install xlsxwriter"
        Remove-Item -Path $jsonPath -ErrorAction SilentlyContinue
        return $null
    }
}

function New-BenchmarkMarkdown {
    param(
        [hashtable]$Data,
        [string]$OutputPath
    )

    $ts = Get-SafeValue $Data.Timestamp (Get-Date -Format "yyyyMMdd-HHmmss")
    if (-not $OutputPath) {
        if (-not (Test-Path $Script:ReportDir)) {
            New-Item -ItemType Directory -Path $Script:ReportDir -Force | Out-Null
        }
        $OutputPath = Join-Path $Script:ReportDir "benchmark-report-$ts.md"
    }

    $env_ = if ($Data.Environment) { $Data.Environment } else { @{} }
    $createPath = if ($Data.CreatePath) { $Data.CreatePath } else { @{} }
    $http = if ($createPath.Http) { $createPath.Http } else { @{} }
    $grpc = if ($createPath.Grpc) { $createPath.Grpc } else { @{} }
    $scheduler = if ($Data.Scheduler) { $Data.Scheduler } else { @{} }
    $internal = if ($Data.Internal) { $Data.Internal } else { @{} }
    $slo = if ($Data.SLO) { $Data.SLO } else { @{} }

    $sb = [System.Text.StringBuilder]::new()

    # 标题
    [void]$sb.AppendLine("# LoomQ Benchmark Report")
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("**时间**: $(Get-SafeValue $env_.Timestamp $ts)")
    [void]$sb.AppendLine("**Commit**: ``$(Get-SafeValue $env_.Commit 'N/A')`` | **分支**: $(Get-SafeValue $env_.Branch 'N/A')")
    [void]$sb.AppendLine("**Java**: $(Get-SafeValue $env_.JavaVersion 'N/A') | **OS**: $(Get-SafeValue $env_.OS 'N/A') | **CPU**: $(Get-SafeValue $env_.CpuCores 'N/A') cores | **内存**: $(Get-SafeValue $env_.MaxMemory 'N/A')")
    [void]$sb.AppendLine("")

    # 吞吐量对比
    [void]$sb.AppendLine("## 吞吐量对比")
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("| 指标 | HTTP | gRPC |")
    [void]$sb.AppendLine("|------|------|------|")
    $httpPeak = if ($http.PeakQps) { "{0:N0}" -f $http.PeakQps } else { "N/A" }
    $grpcPeak = if ($grpc.PeakQps) { "{0:N0}" -f $grpc.PeakQps } else { "N/A" }
    [void]$sb.AppendLine("| Peak QPS | $httpPeak | $grpcPeak |")
    $httpInflect = Get-SafeValue $http.InflectionThreads "N/A"
    $grpcInflect = Get-SafeValue $grpc.InflectionThreads "N/A"
    [void]$sb.AppendLine("| 拐点线程数 | $httpInflect | $grpcInflect |")
    $httpP99 = if ($http.InflectionP99) { "$($http.InflectionP99)ms" } else { "N/A" }
    $grpcP99 = if ($grpc.InflectionP99) { "$($grpc.InflectionP99)ms" } else { "N/A" }
    [void]$sb.AppendLine("| P99 @ 拐点 | $httpP99 | $grpcP99 |")
    $httpFail = if ($null -ne $http.FailRate) { "{0:P2}" -f $http.FailRate } else { "N/A" }
    $grpcFail = if ($null -ne $grpc.FailRate) { "{0:P2}" -f $grpc.FailRate } else { "N/A" }
    [void]$sb.AppendLine("| 失败率 | $httpFail | $grpcFail |")
    [void]$sb.AppendLine("")

    # Create Path 详情
    $httpRows = if ($http.Rows) { $http.Rows } else { @() }
    $grpcRows = if ($grpc.Rows) { $grpc.Rows } else { @() }
    if ($httpRows.Count -gt 0 -or $grpcRows.Count -gt 0) {
        [void]$sb.AppendLine("## Create Path 详情")
        [void]$sb.AppendLine("")
        [void]$sb.AppendLine("| 线程数 | HTTP QPS | HTTP P50 | HTTP P90 | HTTP P99 | gRPC QPS | gRPC P50 | gRPC P90 | gRPC P99 |")
        [void]$sb.AppendLine("|--------|----------|----------|----------|----------|----------|----------|----------|----------|")

        $maxRows = [Math]::Max($httpRows.Count, $grpcRows.Count)
        for ($i = 0; $i -lt $maxRows; $i++) {
            $hr = if ($i -lt $httpRows.Count) { $httpRows[$i] } else { @{} }
            $gr = if ($i -lt $grpcRows.Count) { $grpcRows[$i] } else { @{} }
            $threads = Get-SafeValue $hr.Threads (Get-SafeValue $gr.Threads "?")
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
    $tiers = if ($scheduler.Tiers) { $scheduler.Tiers } else { @() }
    if ($tiers.Count -gt 0) {
        [void]$sb.AppendLine("## 调度精度")
        [void]$sb.AppendLine("")
        [void]$sb.AppendLine("| 档位 | 并发 | QPS | Wake P50 | Wake P95 | Wake P99 | E2E P50 | E2E P95 | E2E P99 | 利用率 | 背压 | 完成率 |")
        [void]$sb.AppendLine("|------|------|-----|----------|----------|----------|---------|---------|---------|--------|------|--------|")
        foreach ($tier in $tiers) {
            $name = Get-SafeValue $tier.Tier "?"
            $conc = Get-SafeValue $tier.Concurrency "-"
            $qps = if ($tier.Qps) { "{0:N0}" -f $tier.Qps } else { "-" }
            $wp50 = Get-SafeValue $tier.WakeP50 "-"
            $wp95 = Get-SafeValue $tier.WakeP95 "-"
            $wp99 = Get-SafeValue $tier.WakeP99 "-"
            $ep50 = Get-SafeValue $tier.E2eP50 "-"
            $ep95 = Get-SafeValue $tier.E2eP95 "-"
            $ep99 = Get-SafeValue $tier.E2eP99 "-"
            $util = if ($null -ne $tier.UtilPct) { "{0:P1}" -f ($tier.UtilPct / 100) } else { "-" }
            $bp = if ($null -ne $tier.BackpressurePct) { "{0:P1}" -f ($tier.BackpressurePct / 100) } else { "-" }
            $comp = if ($null -ne $tier.CompletionPct) { "{0:P1}" -f ($tier.CompletionPct / 100) } else { "-" }
            [void]$sb.AppendLine("| $name | $conc | $qps | $wp50 | $wp95 | $wp99 | $ep50 | $ep95 | $ep99 | $util | $bp | $comp |")
        }
        [void]$sb.AppendLine("")
    }

    # 内部组件
    $items = if ($internal.Items) { $internal.Items } else { @() }
    if ($items.Count -gt 0) {
        [void]$sb.AppendLine("## 内部组件")
        [void]$sb.AppendLine("")
        [void]$sb.AppendLine("| 基准测试 | 指标 | 值 |")
        [void]$sb.AppendLine("|----------|------|-----|")
        foreach ($item in $items) {
            [void]$sb.AppendLine("| $(Get-SafeValue $item.Benchmark '-') | $(Get-SafeValue $item.Metric '-') | $(Get-SafeValue $item.Value '-') |")
        }
        [void]$sb.AppendLine("")
    }

    # SLO 验证
    $sloItems = if ($slo.Items) { $slo.Items } else { @() }
    if ($sloItems.Count -gt 0) {
        [void]$sb.AppendLine("## SLO 验证")
        [void]$sb.AppendLine("")
        [void]$sb.AppendLine("| 档位 | Wake P95 目标 | Wake P95 实际 | 结果 | Wake P99 目标 | Wake P99 实际 | 结果 | E2E P95 目标 | E2E P95 实际 | 结果 | E2E P99 目标 | E2E P99 实际 | 结果 |")
        [void]$sb.AppendLine("|------|---------------|---------------|------|---------------|---------------|------|--------------|--------------|------|--------------|--------------|------|")
        foreach ($item in $sloItems) {
            $tierName = Get-SafeValue $item.Tier "?"
            $cells = @($tierName)
            foreach ($key in @("WakeP95", "WakeP99", "E2eP95", "E2eP99")) {
                $target = Get-SafeValue $item."${key}Target" "-"
                $actual = Get-SafeValue $item."${key}Actual" "-"
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
        $regTitle = [char]0x23 + [char]0x23 + [char]0x20 + '回归对比' + [char]0xFF08 + 'vs 上次运行' + [char]0xFF09
        [void]$sb.AppendLine($regTitle)
        [void]$sb.AppendLine("")
        $httpDelta = $regression.HttpQpsDelta
        $grpcDelta = $regression.GrpcQpsDelta
        if ($null -ne $httpDelta) {
            $pctVal = [math]::Round($httpDelta * 100, 1)
            $hLine = Format-DeltaLine -Protocol HTTP -PctVal $pctVal
            [void]$sb.AppendLine($hLine)
        }
        if ($null -ne $grpcDelta) {
            $pctVal = [math]::Round($grpcDelta * 100, 1)
            $gLine = Format-DeltaLine -Protocol gRPC -PctVal $pctVal
            [void]$sb.AppendLine($gLine)
        }
        [void]$sb.AppendLine("")
    }

    $content = $sb.ToString()
    [System.IO.File]::WriteAllText($OutputPath, $content, [System.Text.UTF8Encoding]::new($true))
    return $OutputPath
}
