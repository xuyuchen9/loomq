# lib/output.ps1 — LoomQ Benchmark Output Formatting
# Summary builder, CSV history writer, JSON report generator.

function Save-BenchmarkSummary {
    param([string[]]$Lines)

    $SummaryDir = Join-Path $ProjectRoot "benchmark\results\reports"
    New-Item -ItemType Directory -Force -Path $SummaryDir | Out-Null

    $Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $SummaryFile = Join-Path $SummaryDir ("summary-" + $Timestamp + ".txt")
    $LatestFile = Join-Path $SummaryDir "latest-summary.txt"

    Set-Content -Path $SummaryFile -Value $Lines -Encoding UTF8
    Set-Content -Path $LatestFile -Value $Lines -Encoding UTF8

    return $SummaryFile
}

function Write-BenchmarkHistoryCsv {
    param(
        $InternalResult,
        $HttpResult,
        $GrpcResult,
        $SchedulerResult,
        $ParsedScheduler
    )

    $HistoryFile = Join-Path $ProjectRoot "benchmark\results\history.csv"
    $Timestamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ss"
    $Commit = Get-GitCommit
    $Branch = try { git rev-parse --abbrev-ref HEAD 2>$null } catch { "unknown" }

    # --- Unified schema with HTTP/gRPC columns ---
    $Tiers = @("ULTRA","FAST","HIGH","STANDARD","ECONOMY")
    $HeaderCols = @("timestamp","commit","branch","mode","java_version","os_name")
    $HeaderCols += "internal_qps"
    $HeaderCols += "http_peak_qps","http_best_p99_ms","http_worst_p99_ms","http_fail_rate"
    $HeaderCols += "grpc_peak_qps","grpc_best_p99_ms","grpc_worst_p99_ms","grpc_fail_rate"
    foreach ($t in $Tiers) {
        $HeaderCols += "${t}_qps","${t}_p95_ms","${t}_p99_ms","${t}_e2e_p95_ms","${t}_e2e_p99_ms","${t}_util_pct","${t}_backpressure"
    }
    $HeaderCols += "completion_rate","total_qps","global_p95_total_ms","vt_reduction_pct","cohort_wake_events"
    $ExpectedHeader = $HeaderCols -join ","

    # Header consistency: append new columns if schema changed (preserve old data)
    $NeedsNewHeader = $true
    if (Test-Path $HistoryFile) {
        $FirstLine = (Get-Content $HistoryFile -TotalCount 1 -ErrorAction SilentlyContinue)
        if ($FirstLine -eq $ExpectedHeader) {
            $NeedsNewHeader = $false
        } elseif (-not [string]::IsNullOrWhiteSpace($FirstLine)) {
            # Schema changed: rewrite header, pad existing rows with empty columns
            $OldCols = $FirstLine.Split(",")
            $NewColCount = $HeaderCols.Count
            $OldColCount = $OldCols.Count
            $PadCount = $NewColCount - $OldColCount
            if ($PadCount -gt 0) {
                $Lines = Get-Content $HistoryFile -Encoding UTF8
                $Lines[0] = $ExpectedHeader
                for ($i = 1; $i -lt $Lines.Count; $i++) {
                    if (-not [string]::IsNullOrWhiteSpace($Lines[$i])) {
                        $Lines[$i] += "," * $PadCount
                    }
                }
                [System.IO.File]::WriteAllLines($HistoryFile, $Lines, [System.Text.UTF8Encoding]::new($false))
                Write-Info "CSV schema expanded: added $PadCount new columns, preserved $($OldColCount)-column history"
                $NeedsNewHeader = $false
            }
        }
    }
    if ($NeedsNewHeader) {
        [System.IO.File]::WriteAllLines($HistoryFile, @($ExpectedHeader), [System.Text.UTF8Encoding]::new($false))
    }

    # Use pre-parsed scheduler data (single-parse model)
    $ParsedLatency = if ($ParsedScheduler) { $ParsedScheduler.Latency } else { @{} }
    $ParsedE2E = if ($ParsedScheduler) { $ParsedScheduler.E2E } else { @{} }
    $ParsedSemaphore = if ($ParsedScheduler) { $ParsedScheduler.Semaphore } else { @{} }
    $ParsedEnv = if ($ParsedScheduler) { $ParsedScheduler.Env } else { @{} }
    $ParsedGlobal = if ($ParsedScheduler) { $ParsedScheduler.Global } else { @{} }
    $ParsedCohort = if ($ParsedScheduler) { $ParsedScheduler.Cohort } else { @{} }
    $ParsedOptimization = if ($ParsedScheduler) { $ParsedScheduler.Optimization } else { @{} }
    $ParsedSysQps = if ($ParsedScheduler) { $ParsedScheduler.SysQps } else { @{} }
    $SchedulerRows = if ($ParsedScheduler) { $ParsedScheduler.Rows } else { @() }

    $Mode = if ($Quick) { "quick" } else { "full" }
    $JavaVersion = if ($ParsedEnv.Count -gt 0) { $ParsedEnv["java_version"] } else { "" }
    $OsName = if ($ParsedEnv.Count -gt 0) { $ParsedEnv["os_name"] } else { "" }

    # Build row values
    $Vals = @($Timestamp, $Commit, $Branch, $Mode, $JavaVersion, $OsName)

    # Internal
    $Vals += if ($InternalResult) { Get-ResultDouble $InternalResult.Data "direct_store_qps" -Default "" } else { "" }

    # HTTP
    $Vals += if ($HttpResult) { Get-ResultDouble $HttpResult.Data "peak_qps" -Default "" } else { "" }
    $Vals += if ($HttpResult) { Get-ResultDouble $HttpResult.Data "best_p99_ms" -Default "" } else { "" }
    $Vals += if ($HttpResult) { Get-ResultDouble $HttpResult.Data "worst_p99_ms" -Default "" } else { "" }
    $Vals += if ($HttpResult) { Get-ResultDouble $HttpResult.Data "fail_rate" -Default "" } else { "" }

    # gRPC
    $Vals += if ($GrpcResult) { Get-ResultDouble $GrpcResult.Data "peak_qps" -Default "" } else { "" }
    $Vals += if ($GrpcResult) { Get-ResultDouble $GrpcResult.Data "best_p99_ms" -Default "" } else { "" }
    $Vals += if ($GrpcResult) { Get-ResultDouble $GrpcResult.Data "worst_p99_ms" -Default "" } else { "" }
    $Vals += if ($GrpcResult) { Get-ResultDouble $GrpcResult.Data "fail_rate" -Default "" } else { "" }

    # Per-tier scheduler data (wakeup latency stored in us, CSV columns are ms)
    foreach ($t in $Tiers) {
        $row  = $SchedulerRows | Where-Object { $_["tier"] -eq $t } | Select-Object -First 1
        $lat  = $ParsedLatency[$t]
        $e2e  = $ParsedE2E[$t]
        $sem  = $ParsedSemaphore[$t]

        $Vals += if ($row) { $row["qps"] } else { "" }
        $Vals += if ($lat) { [math]::Round((Get-ResultDouble $lat "p95") / 1000) } else { "" }
        $Vals += if ($lat) { [math]::Round((Get-ResultDouble $lat "p99") / 1000) } else { "" }
        $Vals += if ($e2e) { $e2e["p95"] } else { "" }
        $Vals += if ($e2e) { $e2e["p99"] } else { "" }
        $Vals += if ($sem) { $sem["utilization_pct"] } else { "" }
        $Vals += if ($row) { $row["backpressure"] } else { "" }
    }

    $Vals += if ($SchedulerResult) { Get-ResultDouble $SchedulerResult.Data "completion_rate" -Default "" } else { "" }
    $Vals += if ($ParsedSysQps.Count -gt 0) { $ParsedSysQps["total_qps"] } else { "" }
    $Vals += if ($ParsedGlobal.Count -gt 0) { $ParsedGlobal["p95_total"] } else { "" }
    $Vals += if ($ParsedOptimization.Count -gt 0) { $ParsedOptimization["vt_reduction_pct"] } else { "" }
    $Vals += if ($ParsedCohort.Count -gt 0) { $ParsedCohort["wake_events"] } else { "" }

    $RowLine = $Vals -join ","
    [System.IO.File]::AppendAllText($HistoryFile, $RowLine + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))

    Write-Info "History appended to: $HistoryFile"
}

function Write-BenchmarkJson {
    param(
        $InternalResult,
        $HttpResult,
        $GrpcResult,
        $SchedulerResult
    )

    $JsonDir = Join-Path $ProjectRoot "benchmark\results\reports"
    New-Item -ItemType Directory -Force -Path $JsonDir | Out-Null
    $Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $JsonFile = Join-Path $JsonDir ("benchmark-" + $Timestamp + ".json")

    $JsonBranch = try { (git rev-parse --abbrev-ref HEAD 2>$null).Trim() } catch { "unknown" }

    $Output = [ordered]@{
        timestamp = (Get-Date -Format "yyyy-MM-ddTHH:mm:ss")
        commit = Get-GitCommit
        branch = $JsonBranch
        scenarios = @{}
    }

    if ($SchedulerResult) {
        $Tiers = @()
        $SchedulerRows = Get-BenchmarkRowData -Rows $SchedulerResult.Rows
        foreach ($Row in $SchedulerRows) {
            $Tiers += [ordered]@{
                tier = [string]$Row["tier"]
                qps = Get-ResultDouble $Row "qps"
                avg_latency_ms = Get-ResultDouble $Row "avg_latency_ms"
                concurrency = Get-ResultInt $Row "concurrency"
                backpressure = Get-ResultInt $Row "backpressure"
            }
        }
        $Output.scenarios["scheduler"] = [ordered]@{
            peak_tier = [string]$SchedulerResult.Data["peak_tier"]
            peak_qps = Get-ResultDouble $SchedulerResult.Data "peak_qps"
            completion_rate = Get-ResultDouble $SchedulerResult.Data "completion_rate"
            tiers = $Tiers
        }
    }

    if ($HttpResult) {
        $Output.scenarios["http"] = [ordered]@{
            peak_qps = Get-ResultDouble $HttpResult.Data "peak_qps"
            best_p99_ms = Get-ResultDouble $HttpResult.Data "best_p99_ms"
            worst_p99_ms = Get-ResultDouble $HttpResult.Data "worst_p99_ms"
            fail_rate = Get-ResultDouble $HttpResult.Data "fail_rate"
        }
    }

    if ($GrpcResult) {
        $Output.scenarios["grpc"] = [ordered]@{
            peak_qps = Get-ResultDouble $GrpcResult.Data "peak_qps"
            best_p99_ms = Get-ResultDouble $GrpcResult.Data "best_p99_ms"
            worst_p99_ms = Get-ResultDouble $GrpcResult.Data "worst_p99_ms"
            fail_rate = Get-ResultDouble $GrpcResult.Data "fail_rate"
        }
    }

    if ($InternalResult) {
        $Output.scenarios["internal"] = [ordered]@{
            qps = Get-ResultDouble $InternalResult.Data "direct_store_qps"
            threads = Get-ResultInt $InternalResult.Data "direct_store_threads"
            bytes_per_intent = Get-ResultDouble $InternalResult.Data "memory_bytes_per_intent"
        }
    }

    $Output | ConvertTo-Json -Depth 4 | Out-File -FilePath $JsonFile -Encoding UTF8
    Write-Info "JSON report: $JsonFile"
    return $JsonFile
}

function Build-BenchmarkSummary {
    param(
        [string]$ProjectRoot,
        [string]$GitCommit,
        [string]$GitVersion,
        [string]$JavaHome,
        [bool]$Quick,
        $InternalResult,
        $HttpResult,
        $GrpcResult,
        $SchedulerResult,
        $ParsedScheduler
    )

    $Lines = New-Object System.Collections.Generic.List[string]
    $Lines.Add("LoomQ Benchmark Report")
    $Lines.Add("Project root: $ProjectRoot")
    $Lines.Add("Git: $GitCommit ($GitVersion)")
    $Lines.Add("Java: $JavaHome")
    $Lines.Add("Mode: " + $(if ($Quick) { "QUICK" } else { "FULL" }))
    $Lines.Add("")

    $InternalQps = 0.0
    $HttpQps = 0.0
    $HttpBestP99 = 0.0
    $HttpWorstP99 = 0.0
    $SchedulerPeakQps = 0.0
    $SchedulerWorstQps = 0.0
    $SchedulerCompletion = 0.0
    $SchedulerBestEfficiency = 0.0
    $SchedulerWorstEfficiency = 0.0
    $HttpQpsCvPct = 0.0
    $SchedulerQpsCvPct = 0.0
    $SchedulerBackpressureTotal = 0
    $SchedulerClampedTiers = 0

    if ($InternalResult) {
        $InternalQps = Get-ResultDouble $InternalResult.Data "direct_store_qps"
        $Threads = Get-ResultInt $InternalResult.Data "direct_store_threads"
        $Duration = Get-ResultInt $InternalResult.Data "direct_store_duration_sec"
        $MemDelta = Get-ResultInt $InternalResult.Data "memory_delta_bytes"
        $BytesPerIntent = Get-ResultDouble $InternalResult.Data "memory_bytes_per_intent"

        $Lines.Add("1) Process upper bound (direct store)")
        $Lines.Add(("   direct store: {0:n0} intents/s @ {1} threads for {2}s" -f $InternalQps, $Threads, $Duration))
        $Lines.Add(("   retained memory: {0:n2} MB, {1:n0} bytes/intent" -f ($MemDelta / 1MB), $BytesPerIntent))
        $Lines.Add(("   scenario runtime: {0:n1} s" -f $InternalResult.DurationSec))
        $Lines.Add("   NOTE: storage-side ceiling only; HTTP/scheduler overhead excluded.")
        $Lines.Add("")
    }

    if ($HttpResult) {
        $HttpQps = Get-ResultDouble $HttpResult.Data "peak_qps"
        $HttpThreads = Get-ResultInt $HttpResult.Data "best_threads"
        $HttpBestP99 = Get-ResultDouble $HttpResult.Data "best_p99_ms"
        $HttpBestAvg = Get-ResultDouble $HttpResult.Data "best_avg_ms"
        $HttpWorstP99 = Get-ResultDouble $HttpResult.Data "worst_p99_ms"
        $HttpFailRate = Get-ResultDouble $HttpResult.Data "fail_rate"
        $HttpRows = Get-BenchmarkRowData -Rows $HttpResult.Rows
        $HttpQpsValues = @()
        foreach ($Row in $HttpRows) {
            $HttpQpsValues += Get-ResultDouble $Row "qps"
        }
        $HttpStats = Get-DoubleSeriesStats -Values ([double[]]$HttpQpsValues)
        $HttpQpsCvPct = $HttpStats.CvPct

        $Lines.Add("2) HTTP create path")
        $Lines.Add(("   peak: {0:n0} intents/s @ {1} threads" -f $HttpQps, $HttpThreads))
        $Lines.Add(("   latency: best p99 {0:n1} ms, best avg {1:n2} ms, worst p99 {2:n1} ms, fail rate {3:n2}%" -f $HttpBestP99, $HttpBestAvg, $HttpWorstP99, $HttpFailRate))
        $Lines.Add(("   sample points: {0}, qps mean {1:n1}, stdev {2:n1}, cv {3:n1}%" -f $HttpStats.Count, $HttpStats.Mean, $HttpStats.StdDev, $HttpStats.CvPct))
        $Lines.Add(("   scenario runtime: {0:n1} s" -f $HttpResult.DurationSec))
        $Lines.Add("   shortboard: this is the HTTP/JSON/Netty ceiling, not the store ceiling.")
        $Lines.Add("")
    }

    if ($GrpcResult) {
        $GrpcQps = Get-ResultDouble $GrpcResult.Data "peak_qps"
        $GrpcThreads = Get-ResultInt $GrpcResult.Data "best_threads"
        $GrpcBestP99 = Get-ResultDouble $GrpcResult.Data "best_p99_ms"
        $GrpcBestAvg = Get-ResultDouble $GrpcResult.Data "best_avg_ms"
        $GrpcWorstP99 = Get-ResultDouble $GrpcResult.Data "worst_p99_ms"
        $GrpcFailRate = Get-ResultDouble $GrpcResult.Data "fail_rate"
        $GrpcRows = Get-BenchmarkRowData -Rows $GrpcResult.Rows
        $GrpcQpsValues = @()
        foreach ($Row in $GrpcRows) {
            $GrpcQpsValues += Get-ResultDouble $Row "qps"
        }
        $GrpcStats = Get-DoubleSeriesStats -Values ([double[]]$GrpcQpsValues)

        $Lines.Add("2b) gRPC create path")
        $Lines.Add(("   peak: {0:n0} intents/s @ {1} threads" -f $GrpcQps, $GrpcThreads))
        $Lines.Add(("   latency: best p99 {0:n1} ms, best avg {1:n2} ms, worst p99 {2:n1} ms, fail rate {3:n2}%" -f $GrpcBestP99, $GrpcBestAvg, $GrpcWorstP99, $GrpcFailRate))
        $Lines.Add(("   sample points: {0}, qps mean {1:n1}, stdev {2:n1}, cv {3:n1}%" -f $GrpcStats.Count, $GrpcStats.Mean, $GrpcStats.StdDev, $GrpcStats.CvPct))
        $Lines.Add(("   scenario runtime: {0:n1} s" -f $GrpcResult.DurationSec))
        $Lines.Add("   shortboard: this is the gRPC/Protobuf ceiling, not the store ceiling.")
        if ($HttpQps -gt 0 -and $GrpcQps -gt 0) {
            $GrpcVsHttp = $GrpcQps / $HttpQps
            $Lines.Add(("   gRPC vs HTTP: {0:n1}x throughput" -f $GrpcVsHttp))
        }
        $Lines.Add("")
    }

    if ($SchedulerResult) {
        $SchedulerPeakQps = Get-ResultDouble $SchedulerResult.Data "peak_qps"
        $SchedulerPeakTier = [string]$SchedulerResult.Data["peak_tier"]
        $SchedulerWorstQps = Get-ResultDouble $SchedulerResult.Data "worst_qps"
        $SchedulerWorstTier = [string]$SchedulerResult.Data["worst_tier"]
        $SchedulerBestEfficiency = Get-ResultDouble $SchedulerResult.Data "best_efficiency"
        $SchedulerBestEfficiencyTier = [string]$SchedulerResult.Data["best_efficiency_tier"]
        $SchedulerWorstEfficiency = Get-ResultDouble $SchedulerResult.Data "worst_efficiency"
        $SchedulerWorstEfficiencyTier = [string]$SchedulerResult.Data["worst_efficiency_tier"]
        $SchedulerCompletion = Get-ResultDouble $SchedulerResult.Data "completion_rate"
        $SchedulerTotalReceived = Get-ResultInt $SchedulerResult.Data "total_received"
        $SchedulerTotalExpected = Get-ResultInt $SchedulerResult.Data "total_expected"
        $SchedulerWaitMs = Get-ResultInt $SchedulerResult.Data "total_wait_ms"
        $SchedulerClampedTiers = Get-ResultInt $SchedulerResult.Data "efficiency_clamped_tiers"
        $SchedulerRows = Get-BenchmarkRowData -Rows $SchedulerResult.Rows
        $SchedulerQpsValues = @()
        foreach ($Row in $SchedulerRows) {
            $SchedulerQpsValues += Get-ResultDouble $Row "qps"
            $SchedulerBackpressureTotal += Get-ResultInt $Row "backpressure"
        }
        $SchedulerStats = Get-DoubleSeriesStats -Values ([double[]]$SchedulerQpsValues)
        $SchedulerQpsCvPct = $SchedulerStats.CvPct

        $Lines.Add("3) Scheduler trigger path")
        $Lines.Add(("   peak tier: {0} @ {1:n1} QPS" -f $SchedulerPeakTier, $SchedulerPeakQps))
        $Lines.Add(("   worst tier: {0} @ {1:n1} QPS" -f $SchedulerWorstTier, $SchedulerWorstQps))
        $Lines.Add(("   efficiency span: best {0} ({1:n1}%), worst {2} ({3:n1}%), completion {4:n1}% ({5}/{6}), wait {7} ms" -f
            $SchedulerBestEfficiencyTier,
            $SchedulerBestEfficiency,
            $SchedulerWorstEfficiencyTier,
            $SchedulerWorstEfficiency,
            $SchedulerCompletion,
            $SchedulerTotalReceived,
            $SchedulerTotalExpected,
            $SchedulerWaitMs))
        $Lines.Add(("   sample points: {0} tiers, qps mean {1:n1}, stdev {2:n1}, cv {3:n1}%, backpressure {4}" -f
            $SchedulerStats.Count,
            $SchedulerStats.Mean,
            $SchedulerStats.StdDev,
            $SchedulerStats.CvPct,
            $SchedulerBackpressureTotal))
        if ($SchedulerClampedTiers -gt 0) {
            $Lines.Add(("   note: efficiency for {0} tier(s) exceeded theoretical estimate and was capped to 100%." -f $SchedulerClampedTiers))
        }
        $Lines.Add(("   scenario runtime: {0:n1} s" -f $SchedulerResult.DurationSec))
        $Lines.Add("   shortboard: this is the real调度 + callback + webhook path, where tier differences and downstream latency show up.")
        $Lines.Add("")

        # Layer 1: Scheduling precision SLO (wakeup latency in microseconds)
        $SloSpecs = @{
            "ULTRA"    = @{ p95 = 15000;  p99 = 25000  }
            "FAST"     = @{ p95 = 60000;  p99 = 90000  }
            "HIGH"     = @{ p95 = 120000; p99 = 180000 }
            "STANDARD" = @{ p95 = 550000; p99 = 800000 }
            "ECONOMY"  = @{ p95 = 1100000; p99 = 1500000 }
        }
        $ParsedLatency = if ($ParsedScheduler) { $ParsedScheduler.Latency } else { @{} }
        $Lines.Add("4) Scheduling Precision SLO (wakeup latency: executeAt -> consumer dequeue)")
        foreach ($Row in $SchedulerRows) {
            $TierName = [string]$Row["tier"]
            $TierQps = Get-ResultDouble $Row "qps"
            $Lat = $ParsedLatency[$TierName]
            $P95Actual = if ($Lat) { Get-ResultDouble $Lat "p95" } else { 0 }
            $P99Actual = if ($Lat) { Get-ResultDouble $Lat "p99" } else { 0 }
            $Slo = $SloSpecs[$TierName.ToUpper()]
            if ($null -ne $Slo) {
                $P95Limit = $Slo["p95"]
                $P99Limit = $Slo["p99"]
                $P95Pass = $P95Actual -gt 0 -and $P95Actual -le $P95Limit
                $P99Pass = $P99Actual -gt 0 -and $P99Actual -le $P99Limit
                $VerdictP95 = if ($P95Pass) { "${C.Green}PASS${C.Reset}" } else { "${C.Red}FAIL${C.Reset}" }
                $VerdictP99 = if ($P99Pass) { "${C.Green}PASS${C.Reset}" } else { "${C.Red}FAIL${C.Reset}" }
                $Lines.Add(("   {0,-10} QPS={1,6:n0}  p95={2,8:n0}us  p99={3,8:n0}us  p95<={4,7}us {5}  p99<={6,7}us {7}" -f
                    $TierName, $TierQps, $P95Actual, $P99Actual, $P95Limit, $VerdictP95, $P99Limit, $VerdictP99))
            } else {
                $Lines.Add(("   {0,-10} QPS={1,6:n0}  p95={2,8:n0}us  p99={3,8:n0}us  (no SLO defined)" -f
                    $TierName, $TierQps, $P95Actual, $P99Actual))
            }
        }
        $Lines.Add("")

        # Layer 2: E2E latency (display only, not SLO - dominated by queue depth under load)
        $ParsedE2E = if ($ParsedScheduler) { $ParsedScheduler.E2E } else { @{} }
        $Lines.Add("5) E2E Latency (executeAt -> webhook received, queue depth impact)")
        foreach ($Row in $SchedulerRows) {
            $TierName = [string]$Row["tier"]
            $TierQps = Get-ResultDouble $Row "qps"
            $E2E = $ParsedE2E[$TierName]
            $P95Actual = if ($E2E) { Get-ResultDouble $E2E "p95" } else { 0 }
            $P99Actual = if ($E2E) { Get-ResultDouble $E2E "p99" } else { 0 }
            $Lines.Add(("   {0,-10} QPS={1,6:n0}  e2e_p95={2,6:n0}ms  e2e_p99={3,6:n0}ms" -f
                $TierName, $TierQps, $P95Actual, $P99Actual))
        }
        $Lines.Add("")
    }

    $Lines.Add("Derived observations")
    if ($InternalQps -gt 0 -and $HttpQps -gt 0) {
        $HttpLoss = $InternalQps / $HttpQps
        $Lines.Add(("   - HTTP path is about {0:n1}x slower than the in-process upper bound." -f $HttpLoss))
    }

    if ($HttpBestP99 -gt 0 -and $HttpWorstP99 -gt 0) {
        $Lines.Add(("   - HTTP p99 spreads by {0:n1} ms across concurrency points." -f ($HttpWorstP99 - $HttpBestP99)))
    }
    if ($HttpQpsCvPct -gt 0) {
        $Lines.Add(("   - HTTP throughput variation across sampled points: CV {0:n1}%." -f $HttpQpsCvPct))
    }

    if ($SchedulerPeakQps -gt 0 -and $SchedulerWorstQps -gt 0) {
        $Lines.Add(("   - Scheduler real-trigger throughput spreads by {0:n1}x between best and worst tiers." -f ($SchedulerPeakQps / $SchedulerWorstQps)))
    }
    if ($SchedulerQpsCvPct -gt 0) {
        $Lines.Add(("   - Scheduler tier throughput variation across sampled points: CV {0:n1}%." -f $SchedulerQpsCvPct))
    }

    if ($SchedulerBestEfficiency -gt 0 -and $SchedulerWorstEfficiency -gt 0) {
        $Lines.Add(("   - Scheduler efficiency ranges from {0:n1}% to {1:n1}% of theoretical ceiling." -f $SchedulerWorstEfficiency, $SchedulerBestEfficiency))
    }
    if ($SchedulerBackpressureTotal -gt 0) {
        $Lines.Add(("   - Scheduler recorded {0} backpressure event(s) during this run." -f $SchedulerBackpressureTotal))
    }

    if ($SchedulerCompletion -gt 0 -and $SchedulerCompletion -lt 100) {
        $Lines.Add("   - Completion rate did not reach 100%; real trigger path still has drop or timeout risk.")
    }

    # Append regression comparison (if history data available)
    try {
        $Regression = Build-RegressionComparison -ProjectRoot $ProjectRoot
        if ($Regression -and $Regression.Count -gt 0) {
            $Lines.Add("")
            $Lines.AddRange($Regression)
        }
    } catch {
        # regression comparison failure should not break main report
    }

    # Translate English output to Chinese for the saved report file
    $Translated = ConvertTo-ChineseReport $Lines.ToArray()

    return $Translated
}

function ConvertTo-ChineseReport {
    <#
    .SYNOPSIS
        Translate key English terms in benchmark report lines to Chinese.
        Reads translations from benchmark/i18n/report-zh.json to avoid PS1 encoding issues.
        Falls back to original English if translation file not found.
    #>
    param([string[]]$Lines)

    $TranslationFile = Join-Path $PSScriptRoot "i18n\report-zh.json"
    if (-not (Test-Path $TranslationFile)) {
        # No translation file; just strip ANSI codes and return
        $Result = New-Object System.Collections.Generic.List[string]
        foreach ($Line in $Lines) {
            $Result.Add(($Line -replace '\x1b\[[0-9;]*m', ''))
        }
        return $Result.ToArray()
    }

    $Map = Get-Content $TranslationFile -Encoding UTF8 -Raw | ConvertFrom-Json

    $Output = New-Object System.Collections.Generic.List[string]
    foreach ($Line in $Lines) {
        $Translated = $Line
        foreach ($Prop in $Map.PSObject.Properties) {
            $Translated = $Translated.Replace($Prop.Name, $Prop.Value)
        }
        # Strip ANSI escape codes for file output
        $Translated = $Translated -replace '\x1b\[[0-9;]*m', ''
        $Output.Add($Translated)
    }
    return $Output.ToArray()
}

function Build-RegressionComparison {
    <#
    .SYNOPSIS
        Read history.csv last 2 runs, generate regression comparison table.
    .OUTPUTS
        string[] comparison report lines
    #>
    param([string]$ProjectRoot)

    $HistoryFile = Join-Path $ProjectRoot "benchmark\results\history.csv"
    if (-not (Test-Path $HistoryFile)) {
        return @("History file not found: $HistoryFile", "Run a benchmark first.")
    }

    $Lines = Get-Content $HistoryFile -Encoding UTF8 | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    if ($Lines.Count -lt 3) {
        return @("Not enough history (need >= 2 runs), current: $($Lines.Count - 1) records.")
    }

    $Header = $Lines[0].Split(",")
    $Current = $Lines[-1].Split(",")
    $Previous = $Lines[-2].Split(",")

    $ColIndex = @{}
    for ($i = 0; $i -lt $Header.Count; $i++) {
        $ColIndex[$Header[$i]] = $i
    }

    function Get-ColValue {
        param($Row, $ColName, $Default = 0.0)
        if ($ColIndex.ContainsKey($ColName) -and $ColIndex[$ColName] -lt $Row.Count) {
            $v = $Row[$ColIndex[$ColName]]
            if ([string]::IsNullOrWhiteSpace($v)) { return $Default }
            try { return [double]$v } catch { return $Default }
        }
        return $Default
    }

    function Format-Delta {
        param($Old, $New, $Unit, [switch]$LowerIsBetter)
        if ($Old -eq 0 -and $New -eq 0) { return "   -" }
        if ($Old -eq 0) { return "$New $Unit (new)" }
        $Pct = ($New - $Old) / $Old * 100
        $Arrow = if ($Pct -gt 0) { "+" } else { "" }
        $Symbol = if ($LowerIsBetter) {
            if ($Pct -gt 10) { "[!!]" } elseif ($Pct -lt -10) { "[OK]" } else { "[-]" }
        } else {
            if ($Pct -gt 10) { "[OK]" } elseif ($Pct -lt -10) { "[!!]" } else { "[-]" }
        }
        return "$Old->$New $Unit ($Arrow$([math]::Round($Pct,1))%) $Symbol"
    }

    $PrevMode = Get-ColValue $Previous "mode"
    $CurrMode = Get-ColValue $Current "mode"
    $PrevTs = if ($ColIndex.ContainsKey("timestamp")) { $Previous[$ColIndex["timestamp"]] } else { "?" }
    $CurrTs = if ($ColIndex.ContainsKey("timestamp")) { $Current[$ColIndex["timestamp"]] } else { "?" }

    $Sep = "+----------+---------------------------------------------------------------------------+"
    $Output = New-Object System.Collections.Generic.List[string]
    $Output.Add("")
    $Output.Add("=== Regression Comparison ===")
    $Output.Add("  Prev: $PrevTs ($PrevMode)")
    $Output.Add("  Curr: $CurrTs ($CurrMode)")
    $Output.Add($Sep)
    $Output.Add(("| {0,-8} | {1,-73} |" -f "Metric", "Delta"))
    $Output.Add($Sep)

    $PrevTotal = Get-ColValue $Previous "total_qps"
    $CurrTotal = Get-ColValue $Current "total_qps"
    if ($PrevTotal -gt 0 -or $CurrTotal -gt 0) {
        $Output.Add(("| {0,-8} | {1,-73} |" -f "TotalQPS", (Format-Delta $PrevTotal $CurrTotal "QPS")))
    }

    $PrevComp = Get-ColValue $Previous "completion_rate"
    $CurrComp = Get-ColValue $Current "completion_rate"
    if ($PrevComp -gt 0 -or $CurrComp -gt 0) {
        $Output.Add(("| {0,-8} | {1,-73} |" -f "ComplRate", (Format-Delta $PrevComp $CurrComp "%")))
    }

    $Tiers = @("ULTRA","FAST","HIGH","STANDARD","ECONOMY")
    $HasRegression = $false
    $RegressionTiers = @()

    foreach ($t in $Tiers) {
        $PrevQps = Get-ColValue $Previous "${t}_qps"
        $CurrQps = Get-ColValue $Current "${t}_qps"
        $PrevP99 = Get-ColValue $Previous "${t}_p99_ms"
        $CurrP99 = Get-ColValue $Current "${t}_p99_ms"
        $PrevE2E = Get-ColValue $Previous "${t}_e2e_p99_ms"
        $CurrE2E = Get-ColValue $Current "${t}_e2e_p99_ms"

        if ($PrevQps -eq 0 -and $CurrQps -eq 0) { continue }

        $QpsDelta = if ($PrevQps -gt 0) { ($CurrQps - $PrevQps) / $PrevQps * 100 } else { 0 }
        $E2EDelta = if ($PrevE2E -gt 0) { ($CurrE2E - $PrevE2E) / $PrevE2E * 100 } else { 0 }

        if ($QpsDelta -lt -10 -or $E2EDelta -gt 20) {
            $HasRegression = $true
            $RegressionTiers += $t
        }

        $Output.Add(("| {0,-8} | QPS: {1,-40} |" -f $t, (Format-Delta $PrevQps $CurrQps "QPS")))
        $Output.Add(("| {0,-8} | p99:   {1,-40} |" -f "", (Format-Delta $PrevP99 $CurrP99 "ms" -LowerIsBetter)))
        $Output.Add(("| {0,-8} | e2e99: {1,-40} |" -f "", (Format-Delta $PrevE2E $CurrE2E "ms" -LowerIsBetter)))
    }

    $Output.Add($Sep)

    if ($HasRegression) {
        $Output.Add("  [!!] REGRESSION: $($RegressionTiers -join ', ') -- QPS dropped >10% or E2E-p99 grew >20%")
    } else {
        $Output.Add("  [OK] All tiers stable, no significant regression.")
    }

    return $Output.ToArray()
}
