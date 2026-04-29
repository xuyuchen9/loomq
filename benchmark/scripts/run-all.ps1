#!/usr/bin/env pwsh
<#
.SYNOPSIS
    LoomQ 一键全量测试 + 落库脚本

.DESCRIPTION
    运行全部测试（快测 + 慢测 + 集成测试 + 压测）并将结果持久化到 benchmark/results/。

.PARAMETER Quick
    快速模式：跳过大压测，只跑单元/集成测试 + benchmark smoke

.PARAMETER SkipBenchmark
    跳过所有压测，只跑单元/集成测试

.PARAMETER NoSave
    预览模式：不写入 CSV/JSON
#>

param(
    [switch]$Quick,
    [switch]$SkipBenchmark,
    [switch]$NoSave
)

$ErrorActionPreference = "Continue"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# ============================================================
#  Path resolution
# ============================================================

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)
$ResultsDir = Join-Path $ProjectRoot "benchmark\results"
$ReportsDir = Join-Path $ResultsDir "reports"
$LogsDir = Join-Path $ResultsDir "logs"

New-Item -ItemType Directory -Force -Path $ReportsDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$DateIso = Get-Date -Format "yyyy-MM-ddTHH:mm:ss"
$Commit = try { (git -C $ProjectRoot rev-parse --short HEAD 2>$null).Trim() } catch { "unknown" }
$Branch = try { (git -C $ProjectRoot rev-parse --abbrev-ref HEAD 2>$null).Trim() } catch { "unknown" }

$AllLog = Join-Path $LogsDir "full-suite-$Timestamp.log"

# ============================================================
#  Helpers
# ============================================================

function Write-Banner {
    $w = 64
    $l = "=" * $w
    Write-Host ""
    Write-Host $l -ForegroundColor Magenta
    Write-Host ("  LoomQ Full Test Suite" + (" " * ($w - 24))) -ForegroundColor Magenta
    Write-Host ("  $DateIso  |  $Commit  |  $Branch") -ForegroundColor DarkGray
    Write-Host $l -ForegroundColor Magenta
    Write-Host ""
}

function Write-Step {
    param([string]$Text)
    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $Text" -ForegroundColor Cyan
}

function Write-Pass {
    param([string]$Text)
    Write-Host "  PASS  $Text" -ForegroundColor Green
}

function Write-Fail {
    param([string]$Text)
    Write-Host "  FAIL  $Text" -ForegroundColor Red
}

function Invoke-Mvn {
    param([string]$Goal, [string]$Profile = "", [string]$Module = "", [string]$Test = "")
    $args = @($Goal)
    if ($Profile) { $args += "-P$Profile" }
    if ($Module)  { $args += "-pl"; $args += $Module; $args += "-am" }
    if ($Test)    { $args += "-Dtest=$Test"; $args += "-Dsurefire.failIfNoSpecifiedTests=false" }
    $args += "-B"; $args += "-ntp"
    $args += "--file"; $args += (Join-Path $ProjectRoot "pom.xml")

    $exitCode = 0
    $output = & mvn @args | ForEach-Object {
        $line = $_
        Add-Content -Path $AllLog -Value $line
        # echo non-noise lines
        if ($line -match 'Tests run:|BUILD|ERROR|RESULT') {
            Write-Host "  $line" -ForegroundColor DarkGray
        }
        $line
    }
    $exitCode = $LASTEXITCODE
    return @{ ExitCode = $exitCode; Output = $output }
}

function Invoke-MvnExec {
    param([string]$MainClass, [string]$ExecArgs = "")
    $serverDir = Join-Path $ProjectRoot "loomq-server"
    $mvnArgs = @("exec:java", "-Dexec.mainClass=$MainClass", "-Dexec.classpathScope=test")
    if ($ExecArgs) { $mvnArgs += $ExecArgs }
    $mvnArgs += "-B"; $mvnArgs += "-ntp"

    $prevLocation = Get-Location
    $exitCode = 0
    try {
        Set-Location $serverDir
        # Ensure test classes are compiled (especially after MetricsCollector changes in core)
        $output = & mvn test-compile -am -B -ntp -q | ForEach-Object {
            Add-Content -Path $AllLog -Value $_
            $_
        }
        $output = & mvn @mvnArgs | ForEach-Object {
            $line = $_
            Add-Content -Path $AllLog -Value $line
            if ($line -match 'Tests run:|BUILD|ERROR|RESULT') {
                Write-Host "  $line" -ForegroundColor DarkGray
            }
            $line
        }
        $exitCode = $LASTEXITCODE
    } finally {
        Set-Location $prevLocation
    }
    return @{ ExitCode = $exitCode; Output = $output }
}

function Parse-TestCount {
    param($Output)
    $last = ($Output | Select-String "Tests run:" | Select-Object -Last 1)
    if ($null -eq $last) { return @{ Total = 0; Failed = 0; Errors = 0 } }
    $text = $last.ToString()
    if ($text -match 'Tests run:\s*(\d+).*Failures:\s*(\d+).*Errors:\s*(\d+)') {
        return @{ Total = [int]$Matches[1]; Failed = [int]$Matches[2]; Errors = [int]$Matches[3] }
    }
    return @{ Total = 0; Failed = 0; Errors = 0 }
}

# ============================================================
#  Main
# ============================================================

Write-Banner

$Results = @{
    timestamp = $DateIso
    commit    = $Commit
    branch    = $Branch
    mode      = if ($Quick) { "quick" } else { "full" }
    phases    = @{}
}

# ---- Phase 1: Fast Unit Tests ----
Write-Step "Phase 1/4: Fast unit tests"
$r = Invoke-Mvn -Goal test -Profile fast-tests -Module loomq-core
$coreFast = Parse-TestCount $r.Output
if ($r.ExitCode -eq 0) { Write-Pass "loomq-core fast tests: $($coreFast.Total) run, 0 failures" }
else { Write-Fail "loomq-core fast tests: $($coreFast.Failed) failures"; exit 1 }

$r = Invoke-Mvn -Goal test -Profile fast-tests -Module loomq-server
$serverFast = Parse-TestCount $r.Output
if ($r.ExitCode -eq 0) { Write-Pass "loomq-server fast tests: $($serverFast.Total) run, 0 failures" }
else { Write-Fail "loomq-server fast tests: $($serverFast.Failed) failures"; exit 1 }

$Results.phases["fast-tests"] = @{
    core   = $coreFast
    server = $serverFast
}

# ---- Phase 2: Slow Tests ----
Write-Step "Phase 2/4: Slow tests (scheduler dispatch lifecycle)"
$r = Invoke-Mvn -Goal test -Profile slow-tests
$slow = Parse-TestCount $r.Output
if ($r.ExitCode -eq 0) { Write-Pass "slow tests: $($slow.Total) run, 0 failures" }
else { Write-Fail "slow tests: $($slow.Failed) failures"; exit 1 }

$Results.phases["slow-tests"] = $slow

# ---- Phase 3: Integration Tests ----
Write-Step "Phase 3/4: Integration tests"
$r = Invoke-Mvn -Goal test -Profile integration-tests `
    -Test "!com.loomq.scheduler.PrecisionTierIntegrationTest"
$integ = Parse-TestCount $r.Output
if ($r.ExitCode -eq 0) { Write-Pass "integration tests: $($integ.Total) run, 0 failures" }
else { Write-Fail "integration tests: $($integ.Failed) failures"; exit 1 }

$Results.phases["integration-tests"] = $integ

# ---- Phase 4: Benchmark ----
if (-not $SkipBenchmark) {
    Write-Step "Phase 4/4: Installing updated jars + benchmark"
    # Install to local m2 so exec:java picks up MetricsCollector/LatencySnapshot changes
    & mvn install -DskipTests -B -ntp -q | ForEach-Object { Add-Content -Path $AllLog -Value $_ }

    if ($Quick) {
        $r = Invoke-MvnExec -MainClass "com.loomq.scheduler.SchedulerTriggerBenchmarkWithMockServer" -ExecArgs "-Dloomq.benchmark.quick=true"
    } else {
        Write-Step "Phase 4/4: Full benchmark (100k intents, ~2min)"
        $r = Invoke-MvnExec -MainClass "com.loomq.scheduler.SchedulerTriggerBenchmarkWithMockServer"
    }
    $bm = Parse-TestCount $r.Output
    if ($r.ExitCode -eq 0) { Write-Pass "benchmark: $($bm.Total) scenarios run, 0 failures" }
    else { Write-Fail "benchmark: $($bm.Failed) failures" }

    # Parse ALL marker types from benchmark output
    $bmRows = @()
    $bmLatency = @{}
    $bmSemaphore = @{}
    $bmQueue = @{}
    $bmLifecycle = @{}
    $bmProfile = @{}
    $bmGlobal = @{}
    $bmSystem = @()
    $bmEnv = @{}
    $bmParams = @()
    $bmCohort = @{}
    $bmWalMode = @{}
    $bmOptimization = @{}
    $bmE2ELatency = @{}
    $bmSchedPrecision = @{}
    $bmBatch = @{}
    $bmTrace = @{}
    $bmWalCrash = @{}
    $bmTraceTier = @{}
    $bmIntentTraces = @()
    $bmBorrow = @{}
    $bmConfig = @{}
    $bmSysQps = @{}
    $bmRtt = @{}

    foreach ($line in $r.Output) {
        if ($line -match '^(RESULT_[A-Z0-9_]+)\|(.+)') {
            $markerType = $Matches[1]
            $fields = $Matches[2]
            $row = @{}
            foreach ($pair in $fields.Split('|')) {
                $kv = $pair.Split('=', 2)
                if ($kv.Count -eq 2) { $row[$kv[0]] = $kv[1] }
            }
            switch ($markerType) {
                'RESULT_ROW'      { $bmRows += $row }
                'RESULT_LATENCY'  { $bmLatency[$row["tier"]] = $row }
                'RESULT_SEMAPHORE' { $bmSemaphore[$row["tier"]] = $row }
                'RESULT_QUEUE'    { $bmQueue[$row["tier"]] = $row }
                'RESULT_LIFECYCLE' { $bmLifecycle[$row["tier"]] = $row }
                'RESULT_PROFILE'  { $bmProfile[$row["tier"]] = $row }
                'RESULT_ENV'    { $bmEnv = $row }
                'RESULT_PARAMS' { $bmParams += $row }
                'RESULT_GLOBAL_LATENCY' { $bmGlobal = $row }
                'RESULT_SYSTEM'   { $bmSystem += $row }
                'RESULT_COHORT'   { $bmCohort = $row }
                'RESULT_WAL_MODE' { $bmWalMode[$row["tier"]] = $row }
                'RESULT_OPTIMIZATION' { $bmOptimization = $row }
                'RESULT_E2E_LATENCY'  { $bmE2ELatency[$row["tier"]] = $row }
                'RESULT_SCHED_PRECISION' { $bmSchedPrecision[$row["tier"]] = $row }
                'RESULT_BATCH'       { $bmBatch = $row }
                'RESULT_TRACE'       { $bmTrace = $row }
                'RESULT_TRACE_TIER'  { $bmTraceTier[$row["tier"]] = $row }
                'RESULT_BORROW'      { $bmBorrow = $row }
                'RESULT_CONFIG'      { $bmConfig = $row }
                'RESULT_SYSTEM_QPS'  { $bmSysQps = $row }
                'RESULT_RTT'         { $bmRtt[$row["tier"]] = $row }
                'RESULT_INTENT_TRACE' { $bmIntentTraces += $row }
                'RESULT_WAL_CRASH'   { $bmWalCrash[$row["mode"]] = $row }
            }
        }
    }

    $Results.phases["benchmark"] = @{
        tests        = $bm
        rows         = $bmRows
        latency      = $bmLatency
        semaphore    = $bmSemaphore
        queue        = $bmQueue
        lifecycle    = $bmLifecycle
        profile      = $bmProfile
        global       = $bmGlobal
        system       = $bmSystem
        env          = $bmEnv
        params       = $bmParams
        cohort       = $bmCohort
        wal_mode     = $bmWalMode
        optimization = $bmOptimization
        e2e_latency  = $bmE2ELatency
        sched_precision = $bmSchedPrecision
        batch        = $bmBatch
        trace        = $bmTrace
        trace_tier   = $bmTraceTier
        intent_traces = $bmIntentTraces
        borrow       = $bmBorrow
        config       = $bmConfig
        system_qps   = $bmSysQps
        rtt          = $bmRtt
        wal_crash    = $bmWalCrash
    }
} else {
    Write-Step "Phase 4/4: Benchmark (skipped)"
}

# ============================================================
#  Persist Results
# ============================================================

if (-not $NoSave) {
    # --- CSV (wide format: one row per run, per-tier columns) ---
    $CsvFile = Join-Path $ResultsDir "history.csv"
    $csvExists = Test-Path $CsvFile
    $bmData = $Results.phases["benchmark"]

    # Build header with all per-tier columns
    $tiers = @("ULTRA","FAST","HIGH","STANDARD","ECONOMY")
    $cols = @("timestamp","commit","branch","mode","total_tests","total_failed")
    foreach ($t in $tiers) {
        $cols += "${t}_qps", "${t}_p50_ms", "${t}_p95_ms", "${t}_p99_ms",
                 "${t}_mean_ms", "${t}_max_ms", "${t}_samples",
                 "${t}_util_pct", "${t}_active_dispatch",
                 "${t}_queue_size", "${t}_queue_offer_failed", "${t}_queue_retry",
                 "${t}_acked", "${t}_dead_letter", "${t}_expired",
                 "${t}_wal_mode",
                 "${t}_e2e_p50_ms", "${t}_e2e_p95_ms", "${t}_e2e_p99_ms"
    }
    $cols += "global_p95_trigger","global_p95_wake","global_p95_webhook","global_p95_total"
    $cols += "cohort_registered","cohort_flushed","cohort_wake_events","cohort_active","cohort_pending"
    $cols += "vt_reduction_pct","vts_saved"
    $cols += "batch_total","batch_intents","batch_avg_latency_ms","batch_max_size"
    $cols += "trace_sched_p50","trace_sched_p95","trace_deliver_p50","trace_deliver_p95","trace_e2e_p50","trace_e2e_p95"

    if (-not $csvExists) {
        ($cols -join ",") | Out-File -FilePath $CsvFile -Encoding UTF8
    }

    $totalTests = $coreFast.Total + $serverFast.Total + $slow.Total + $integ.Total
    $totalFailed = $coreFast.Failed + $serverFast.Failed + $slow.Failed + $integ.Failed
    $mode = if ($Quick) { "quick" } else { "full" }

    # Build row values
    $vals = @($DateIso, $Commit, $Branch, $mode, $totalTests, $totalFailed)
    foreach ($t in $tiers) {
        $lat = if ($bmData -and $bmData["latency"])  { $bmData["latency"][$t] }  else { $null }
        $sem = if ($bmData -and $bmData["semaphore"]) { $bmData["semaphore"][$t] } else { $null }
        $que = if ($bmData -and $bmData["queue"])     { $bmData["queue"][$t] }     else { $null }
        $lif = if ($bmData -and $bmData["lifecycle"]) { $bmData["lifecycle"][$t] } else { $null }
        $row = if ($bmData -and $bmData["rows"])      { $bmData["rows"] | Where-Object { $_["tier"] -eq $t } | Select-Object -First 1 } else { $null }

        $vals += if ($row) { $row["qps"] } else { "" }
        $vals += if ($lat) { $lat["p50"] } else { "" }
        $vals += if ($lat) { $lat["p95"] } else { "" }
        $vals += if ($lat) { $lat["p99"] } else { "" }
        $vals += if ($lat) { $lat["mean"] } else { "" }
        $vals += if ($lat) { $lat["max"] } else { "" }
        $vals += if ($lat) { $lat["samples"] } else { "" }
        $vals += if ($sem) { $sem["utilization_pct"] } else { "" }
        $vals += if ($sem) { $sem["active"] } else { "" }
        $vals += if ($que) { $que["size"] } else { "" }
        $vals += if ($que) { $que["offer_failed"] } else { "" }
        $vals += if ($que) { $que["retry"] } else { "" }
        $vals += if ($lif) { $lif["acked"] } else { "" }
        $vals += if ($lif) { $lif["dead_letter"] } else { "" }
        $vals += if ($lif) { $lif["expired"] } else { "" }
        $vals += if ($p)    { $p["wal_tier_mode"] } else { "" }
        $e2e = if ($bmData -and $bmData["e2e_latency"]) { $bmData["e2e_latency"][$t] } else { $null }
        $vals += if ($e2e)  { $e2e["p50"] } else { "" }
        $vals += if ($e2e)  { $e2e["p95"] } else { "" }
        $vals += if ($e2e)  { $e2e["p99"] } else { "" }
    }
    $global = if ($bmData -and $bmData["global"]) { $bmData["global"] } else { $null }
    $vals += if ($global) { $global["p95_trigger"] } else { "" }
    $vals += if ($global) { $global["p95_wake"] } else { "" }
    $vals += if ($global) { $global["p95_webhook"] } else { "" }
    $vals += if ($global) { $global["p95_total"] } else { "" }

    $cohort = if ($bmData -and $bmData["cohort"]) { $bmData["cohort"] } else { $null }
    $vals += if ($cohort) { $cohort["total_registered"] } else { "" }
    $vals += if ($cohort) { $cohort["total_flushed"] } else { "" }
    $vals += if ($cohort) { $cohort["wake_events"] } else { "" }
    $vals += if ($cohort) { $cohort["active_cohorts"] } else { "" }
    $vals += if ($cohort) { $cohort["pending_intents"] } else { "" }

    $opt = if ($bmData -and $bmData["optimization"]) { $bmData["optimization"] } else { $null }
    $vals += if ($opt) { $opt["vt_reduction_pct"] } else { "" }
    $vals += if ($opt) { $opt["vts_saved"] } else { "" }

    $batch = if ($bmData -and $bmData["batch"]) { $bmData["batch"] } else { $null }
    $vals += if ($batch) { $batch["total_batches"] } else { "" }
    $vals += if ($batch) { $batch["total_intents_batched"] } else { "" }
    $vals += if ($batch) { $batch["avg_batch_latency_ms"] } else { "" }
    $vals += if ($batch) { $batch["max_batch_size"] } else { "" }

    $trace = if ($bmData -and $bmData["trace"]) { $bmData["trace"] } else { $null }
    $vals += if ($trace) { $trace["sched_p50"] } else { "" }
    $vals += if ($trace) { $trace["sched_p95"] } else { "" }
    $vals += if ($trace) { $trace["deliver_p50"] } else { "" }
    $vals += if ($trace) { $trace["deliver_p95"] } else { "" }
    $vals += if ($trace) { $trace["e2e_p50"] } else { "" }
    $vals += if ($trace) { $trace["e2e_p95"] } else { "" }

    ($vals -join ",") | Out-File -FilePath $CsvFile -Encoding UTF8 -Append

    # --- JSON ---
    $JsonFile = Join-Path $ReportsDir "full-suite-$Timestamp.json"
    $Results | ConvertTo-Json -Depth 6 | Out-File -FilePath $JsonFile -Encoding UTF8

    # --- TXT Summary (rich) ---
    $SummaryFile = Join-Path $ReportsDir "full-suite-$Timestamp.txt"
    $lines = @()
    $lines += "=" * 64
    $lines += "  LoomQ Benchmark Report"
    $lines += "=" * 64
    $lines += "Date       : $DateIso"
    $lines += "Commit     : $Commit ($Branch)"

    $env = if ($bmData) { $bmData["env"] } else { $null }
    if ($env -and $env.Count -gt 0) {
        $lines += "Java       : $($env['java_version']) ($($env['java_vendor'])) | OS: $($env['os_name']) $($env['os_arch'])"
        $lines += "CPU Cores  : $($env['cpu_cores']) | Max Heap: $($env['max_heap_mb']) MB"
        $lines += "Mode       : $($env['mode']) | Intents/tier: $($bmData['params'][0]['intents_per_tier']) | Max wait: $($bmData['params'][0]['max_wait_ms'])ms"
    }
    $lines += ""

    # Test counts
    $lines += "Phase          Tests   Failed  Errors"
    $lines += "----------     -----   ------  ------"
    $lines += ("{0,-14}  {1,5}   {2,5}   {3,5}" -f "fast (core)",   $coreFast.Total, $coreFast.Failed, $coreFast.Errors)
    $lines += ("{0,-14}  {1,5}   {2,5}   {3,5}" -f "fast (server)", $serverFast.Total, $serverFast.Failed, $serverFast.Errors)
    $lines += ("{0,-14}  {1,5}   {2,5}   {3,5}" -f "slow",          $slow.Total, $slow.Failed, $slow.Errors)
    $lines += ("{0,-14}  {1,5}   {2,5}   {3,5}" -f "integration",   $integ.Total, $integ.Failed, $integ.Errors)
    $lines += ("{0,-14}  {1,5}   {2,5}   {3,5}" -f "TOTAL",         $totalTests, $totalFailed, 0)
    $lines += ""

    if ($bmData -and $bmData["rows"] -and $bmData["rows"].Count -gt 0) {
        # Tier Configuration table (from RESULT_PROFILE markers)
        if ($bmData["profile"] -and $bmData["profile"].Count -gt 0) {
            $lines += "--- Tier Configuration ---"
            $lines += ("{0,-10} {1,8} {2,8} {3,8} {4,8} {5,8} {6,8}" -f
                "Tier", "WindowMs", "MaxConc", "BatchSz", "BatchWMs", "Consumers", "QueueCap")
            $lines += ("{0,-10} {1,8} {2,8} {3,8} {4,8} {5,8} {6,8}" -f
                "----------", "--------", "-------", "-------", "--------", "---------", "--------")
            foreach ($t in $tiers) {
                $p = $bmData["profile"][$t]
                if ($p) {
                    $lines += ("{0,-10} {1,8} {2,8} {3,8} {4,8} {5,8} {6,8}" -f
                        $t, $p["precision_window_ms"], $p["max_concurrency"], $p["batch_size"],
                        $p["batch_window_ms"], $p["consumer_count"], $p["dispatch_queue_capacity"])
                }
            }
            $lines += ""
        }

        # Configuration Summary (optimization features)
        $cfg = $bmData["config"]
        if ($cfg -and $cfg.Count -gt 0) {
            $lines += "--- Optimization Configuration ---"
            $lines += "  Chameleon mode:   $($cfg['chameleon_mode']) (batch POST: $($cfg['batch_post']))"
            $lines += "  Arrow borrowing:  enabled (100ms timeout, auto-return)"
            $lines += "  WAL mode:         ULTRA/FAST=ASYNC, HIGH=BATCH_DEFERRED, STANDARD/ECONOMY=DURABLE"
            $lines += ""
        }

        # Mock delays
        $mockLine = $bmData["params"] | Where-Object { $_["mock_delay_ULTRA"] } | Select-Object -First 1
        if ($mockLine) {
            $lines += "Mock HTTP delays : ULTRA=$($mockLine['mock_delay_ULTRA'])ms FAST=$($mockLine['mock_delay_FAST'])ms HIGH=$($mockLine['mock_delay_HIGH'])ms STANDARD=$($mockLine['mock_delay_STANDARD'])ms ECONOMY=$($mockLine['mock_delay_ECONOMY'])ms"
            $lines += ""
        }

        # System QPS
        $sysQps = $bmData["system_qps"]
        if ($sysQps -and $sysQps.Count -gt 0 -and $sysQps["total_qps"]) {
            $lines += "--- System Throughput ---"
            $lines += "  Total QPS:     $($sysQps['total_qps']) (stable, all tiers combined)"
            $lines += "  Total received: $($sysQps['total_received'])"
            $lines += ""
        }

        # Per-Tier Results table
        $lines += "--- Per-Tier Results ---"
        $lines += ("{0,-10} {1,5} {2,5} {3,7} {4,5} {5,5} {6,5} {7,5} {8,5} {9,5} {10,5} {11,5} {12,6} {13,5} {14,5} {15,4}" -f
            "Tier", "Total", "Recv", "QPS", "p50", "p75", "p90", "p95", "p99", "p999", "max", "mean", "Util%", "Queue", "BP", "Life")
        $lines += ("{0,-10} {1,5} {2,5} {3,7} {4,5} {5,5} {6,5} {7,5} {8,5} {9,5} {10,5} {11,5} {12,6} {13,5} {14,5} {15,-20}" -f
            "----------", "-----", "-----", "------", "---", "---", "---", "---", "---", "----", "---", "----", "-----", "-----", "--", "----")
        foreach ($t in $tiers) {
            $row  = $bmData["rows"] | Where-Object { $_["tier"] -eq $t } | Select-Object -First 1
            $lat  = $bmData["latency"][$t]
            $sem  = $bmData["semaphore"][$t]
            $que  = $bmData["queue"][$t]
            $lif  = $bmData["lifecycle"][$t]
            $p    = $bmData["profile"][$t]
            if (-not $row) { continue }

            $total  = if ($lif) { $lif["total"] } else { $row["received"] }
            $qps    = $row["qps"]
            $p50    = if ($lat) { $lat["p50"] } else { "-" }
            $p75    = if ($lat) { $lat["p75"] } else { "-" }
            $p90    = if ($lat) { $lat["p90"] } else { "-" }
            $p95    = if ($lat) { $lat["p95"] } else { "-" }
            $p99    = if ($lat) { $lat["p99"] } else { "-" }
            $p999   = if ($lat) { $lat["p999"] } else { "-" }
            $lmax   = if ($lat) { $lat["max"] } else { "-" }
            $lmean  = if ($lat) { $lat["mean"] } else { "-" }
            $util   = if ($sem) { "$($sem['utilization_pct'])%" } else { "-" }
            $qsz    = if ($que) { $que["size"] } else { "-" }
            $bp     = $row["backpressure"]
            $acked  = if ($lif) { $lif["acked"] } else { "-" }
            $dl     = if ($lif) { $lif["dead_letter"] } else { "0" }
            $lc     = "ACK:$acked DL:$dl"

            $lines += ("{0,-10} {1,5} {2,5} {3,7} {4,5} {5,5} {6,5} {7,5} {8,5} {9,5} {10,5} {11,5} {12,6} {13,5} {14,5} {15,-20}" -f
                $t, $total, $row["received"], $qps, $p50, $p75, $p90, $p95, $p99, $p999, $lmax, $lmean, $util, $qsz, $bp, $lc)
        }
        $lines += ""

        # System QPS
        $sysQps = $bmData["system_qps"]
        if (-not $sysQps) {
            # Fallback: compute from log
            $sysQpsLine = $r.Output | Where-Object { $_ -match 'RESULT_SYSTEM_QPS' } | Select-Object -First 1
            if ($sysQpsLine -match 'total_qps=([0-9.]+)') { $sysQps = @{total_qps=$Matches[1]} }
        }

        # SLO Validation
        $lines += "--- SLO Validation (E2E latency: executeAt -> webhook received) ---"
        $lines += "Note: E2E latency = real customer-perceived time. BatchWindowMs contributes directly."
        $slo = @{
            "ULTRA"    = @{p95=50;  p99=100}
            "FAST"     = @{p95=150; p99=250}
            "HIGH"     = @{p95=600; p99=1000}
            "STANDARD" = @{p95=1500; p99=2200}
            "ECONOMY"  = @{p95=3500; p99=5000}
        }
        $lines += ("{0,-10} {1,13} {2,13} {3,8} {4,13} {5,13} {6,8}" -f
            "Tier", "p95 (actual)", "p95 (limit)", "Verdict", "p99 (actual)", "p99 (limit)", "Verdict")
        $lines += ("{0,-10} {1,13} {2,13} {3,8} {4,13} {5,13} {6,8}" -f
            "----------", "------------", "-----------", "-------", "------------", "-----------", "-------")
        foreach ($t in $tiers) {
            $e2e = $bmData["e2e_latency"][$t]
            $limits = $slo[$t]
            if ($e2e -and $limits) {
                $p95ok = [int]$e2e["p95"] -le $limits["p95"]
                $p99ok = [int]$e2e["p99"] -le $limits["p99"]
                $lines += ("{0,-10} {1,13} {2,13} {3,8} {4,13} {5,13} {6,8}" -f
                    $t, "$($e2e['p95'])ms", "$($limits['p95'])ms", $(if ($p95ok) { "PASS" } else { "FAIL" }),
                    "$($e2e['p99'])ms", "$($limits['p99'])ms", $(if ($p99ok) { "PASS" } else { "FAIL" }))
            }
        }
        $lines += ""

        # System Resources
        if ($bmData["system"] -and $bmData["system"].Count -gt 0) {
            $cpuVals = @($bmData["system"] | ForEach-Object { [double]$_.cpu })
            $heapVals = @($bmData["system"] | ForEach-Object { [int]$_.heap_mb })
            $avgCpu = if ($cpuVals.Count -gt 0) { [math]::Round(($cpuVals | Measure-Object -Average).Average * 100, 1) } else { 0 }
            $maxCpu = if ($cpuVals.Count -gt 0) { [math]::Round(($cpuVals | Measure-Object -Maximum).Maximum * 100, 1) } else { 0 }
            $maxHeap = if ($heapVals.Count -gt 0) { ($heapVals | Measure-Object -Maximum).Maximum } else { 0 }
            $lines += "--- System Resources ---"
            $lines += "  Avg CPU: ${avgCpu}% | Peak CPU: ${maxCpu}% | Peak Heap: ${maxHeap} MB"
            $lines += ""
        }

        # E2E Latency Table (executeAt -> webhook received)
        $lines += "--- End-to-End Latency (executeAt -> webhook received) ---"
        $lines += ("{0,-10} {1,8} {2,8} {3,8} {4,8} {5,8} {6}" -f
            "Tier", "p50", "p95", "p99", "max", "mean", "Samples")
        $lines += ("{0,-10} {1,8} {2,8} {3,8} {4,8} {5,8} {6}" -f
            "----------", "--------", "--------", "--------", "--------", "--------", "-------")
        foreach ($t in $tiers) {
            $e2e = $bmData["e2e_latency"][$t]
            if ($e2e) {
                $lines += ("{0,-10} {1,8} {2,8} {3,8} {4,8} {5,8} {6}" -f
                    $t, "$($e2e['p50'])ms", "$($e2e['p95'])ms", "$($e2e['p99'])ms",
                    "$($e2e['max'])ms", "$($e2e['mean'])ms", $e2e["samples"])
            }
        }
        $lines += ""

        # DeepSeek V4 Optimization Impact
        $cohort = $bmData["cohort"]
        $opt = $bmData["optimization"]
        if ($cohort -and $cohort.Count -gt 0) {
            $lines += "--- DeepSeek V4 Optimization Impact ---"
            $lines += ""
            $lines += "CSA Cohort Consolidation (per-intent VT sleep -> batched cohort wake):"
            $lines += "  Intents via cohort : $($cohort['total_registered']) (old: 1 VT each = $($cohort['total_registered']) VTs)"
            $lines += "  Cohort wake events : $($cohort['wake_events']) (platform daemon, not FJP)"
            if ($opt) {
                $lines += "  VT reduction       : $($opt['vt_reduction_pct'])% ($($opt['vts_saved']) VTs eliminated)"
            }
            $lines += "  Active cohorts     : $($cohort['active_cohorts']) ($($cohort['pending_intents']) intents pending)"
            $lines += ""
            $lines += "Tier-Differentiated WAL (FP4 QAT-inspired - right persistence per tier):"
            $lines += ("{0,-10} {1,-18} {2}" -f "Tier", "WAL Mode", "Trade-off")
            $lines += ("{0,-10} {1,-18} {2}" -f "----------", "------------------", "--------")
            foreach ($t in $tiers) {
                $wm = $bmData["wal_mode"][$t]
                if ($wm) {
                    $tradeoff = switch ($wm["wal_mode"]) {
                        "ASYNC"          { "low latency, crash-lose win < flush interval" }
                        "BATCH_DEFERRED" { "balanced: periodic batch fsync ~50ms" }
                        "DURABLE"        { "strongest durability, fsync per write" }
                        default          { "-" }
                    }
                    $lines += ("{0,-10} {1,-18} {2}" -f $t, $wm["wal_mode"], $tradeoff)
                }
            }
            $lines += ""

            # Arrow cross-tier borrowing
            $borrow = $bmData["borrow"]
            if ($borrow -and $borrow.Count -gt 0) {
                $lines += "--- Arrow Cross-Tier Slot Borrowing ---"
                $lines += "  Own direct:    $($borrow['own_direct']) (tryAcquire succeeded immediately)"
                $lines += "  Own blocking:  $($borrow['own_blocking']) (fell back to blocking acquire)"
                $lines += "  Borrowed:      $($borrow['borrowed']) (from lower-priority idle tiers)"
                $lines += "  Borrow rate:   $($borrow['borrow_rate'])% of all acquires"
                $lines += ""
            }

            # Batch Delivery Impact
            $batch = $bmData["batch"]
            if ($batch -and $batch.Count -gt 0) {
                $lines += "--- Batch Delivery Impact ---"
                $lines += "  Total batches:     $($batch['total_batches']) (for $($batch['total_intents_batched']) intents)"
                $lines += "  Avg batch latency: $($batch['avg_batch_latency_ms']) ms"
                $lines += "  Max batch size:    $($batch['max_batch_size'])"
                $reduction = if ([int]$batch['total_batches'] -gt 0) { [math]::Round([int]$batch['total_intents_batched'] / [int]$batch['total_batches'], 1) } else { 0 }
                $lines += "  HTTP reduction:    $($batch['total_intents_batched']) -> $($batch['total_batches']) requests (${reduction}x fewer)"
                $lines += ""
            }

            # Health Summary
            $lines += "--- Health Summary ---"
            $cohortData = $bmData["cohort"]
            $lines += "  Scheduler:     running (cohorts: $($cohortData['active_cohorts']), pending: $($cohortData['pending_intents']))"
            $totalBP = 0
            foreach ($t in $tiers) {
                $r = $bmData["rows"] | Where-Object { $_["tier"] -eq $t } | Select-Object -First 1
                if ($r) { $totalBP += [int]$r["backpressure"] }
            }
            $lines += "  Backpressure:  $totalBP total events"
            if ($bmData["system"] -and $bmData["system"].Count -gt 0) {
                $lastSys = $bmData["system"][-1]
                $lines += "  Heap:          $($lastSys['heap_mb']) MB"
            }
            $lines += "  WAL health:    not exercised (benchmark bypasses WAL — see WalTierCrashRecoveryTest)"
            $lines += ""

            # Pipeline Trace (p50/p95, all tiers combined)
            $trace = $bmData["trace"]
            if ($trace -and $trace.Count -gt 0) {
                $lines += "--- Pipeline Trace (p50/p95, all tiers combined) ---"
                $lines += "  Scheduler precision (executeAt->dequeue): p50=$($trace['sched_p50'])ms  p95=$($trace['sched_p95'])ms"
                $lines += "  Delivery time    (dequeue->webhook recv): p50=$($trace['deliver_p50'])ms  p95=$($trace['deliver_p95'])ms"
                $lines += "  Total E2E        (executeAt->webhook recv): p50=$($trace['e2e_p50'])ms  p95=$($trace['e2e_p95'])ms"
                $lines += "  Total intents traced: $($trace['total_intents'])"
                $lines += ""

                # Per-tier breakdown
                $lines += "  Per-Tier Trace Breakdown:"
                $lines += ("  {0,-10} {1,8} {2,8} {3,8} {4,8}" -f "Tier", "schedP50", "schedP95", "e2eP50", "e2eP95")
                foreach ($t in $tiers) {
                    $tt = $bmData["trace_tier"][$t]
                    if ($tt) {
                        $lines += ("  {0,-10} {1,8} {2,8} {3,8} {4,8}" -f $t, "$($tt['sched_p50'])ms", "$($tt['sched_p95'])ms", "$($tt['e2e_p50'])ms", "$($tt['e2e_p95'])ms")
                    }
                }
                $lines += ""
            }

            # WAL crash-recovery results (from WalMultiProcessCrashTest)
            if ($bmData["wal_crash"] -and $bmData["wal_crash"].Count -gt 0) {
                $lines += "--- WAL Crash-Recovery (multi-process true crash) ---"
                foreach ($mode in @("DURABLE", "ASYNC")) {
                    $wc = $bmData["wal_crash"][$mode]
                    if ($wc) {
                        $lines += "  $mode : written=$($wc['written'])  recovered=$($wc['recovered'])  lost=$($wc['lost'])  loss_pct=$($wc['loss_pct'])%"
                    }
                }
                $lines += ""
            }
        }
    }
    $lines | Out-File -FilePath $SummaryFile -Encoding UTF8

    Write-Host ""
    Write-Host "==============================================================" -ForegroundColor Green
    Write-Host "  Results persisted" -ForegroundColor Green
    Write-Host "==============================================================" -ForegroundColor Green
    Write-Host "  CSV  : $CsvFile" -ForegroundColor Gray
    Write-Host "  JSON : $JsonFile" -ForegroundColor Gray
    Write-Host "  TXT  : $SummaryFile" -ForegroundColor Gray
    Write-Host "  Log  : $AllLog" -ForegroundColor Gray
    Write-Host ""
}

# ============================================================
#  Final Verdict
# ============================================================

if ($totalFailed -eq 0) {
    Write-Host "  ALL TESTS PASSED  ($totalTests total)" -ForegroundColor Green
    Write-Host ""
    exit 0
} else {
    Write-Host "  $totalFailed TEST(S) FAILED!" -ForegroundColor Red
    Write-Host ""
    exit 1
}
