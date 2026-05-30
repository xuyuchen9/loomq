<#
.SYNOPSIS
    LoomQ 性能基准测试

.DESCRIPTION
    运行 LoomQ 全链路基准测试，生成 Excel + MD 报告。

.PARAMETER Quick
    快速模式（减少测试时长和线程梯度）

.PARAMETER Stress
    包含压力测试（多线程梯度扫描）

.PARAMETER StressOnly
    仅运行压力测试，指定场景: http / grpc

.PARAMETER Scenario
    指定运行场景: internal / create / scheduler / all（默认 all）

.PARAMETER NoCompile
    跳过编译步骤

.PARAMETER Compare
    仅显示上次运行的报告对比

.PARAMETER VerboseOutput
    显示完整场景日志

.EXAMPLE
    .\benchmark.ps1                    # 全量测试
    .\benchmark.ps1 -Quick             # 快速测试
    .\benchmark.ps1 -Stress            # 全量 + 压力测试
    .\benchmark.ps1 -Stress -StressOnly grpc   # 仅 gRPC 压力测试
    .\benchmark.ps1 -Scenario internal # 仅内部组件测试
    .\benchmark.ps1 -Compare           # 查看上次报告
#>

param(
    [switch]$Quick,
    [switch]$Stress,
    [string]$StressOnly = "",
    [string]$Scenario = "all",
    [string]$GrpcTier = "",
    [switch]$NoCompile,
    [switch]$Compare,
    [switch]$VerboseOutput,
    [switch]$Help,
    [switch]$KeepOpen,
    [switch]$NoPause
)

# ============================================================
#  Load modules
# ============================================================

. $PSScriptRoot\lib\ui.ps1
. $PSScriptRoot\lib\util.ps1
. $PSScriptRoot\lib\server.ps1
. $PSScriptRoot\lib\report.ps1

# ============================================================
#  Error handler
# ============================================================

trap {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "  FATAL ERROR" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Yellow
    Write-Host $_.ScriptStackTrace -ForegroundColor Gray
    Exit-BenchmarkScript -ExitCode 1
}

# ============================================================
#  Configuration
# ============================================================

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$ScenarioTimeouts = @{
    "internal"  = @{ Quick = 120; Full = 240 }
    "wal"       = @{ Quick = 60;  Full = 120 }
    "storage"   = @{ Quick = 60;  Full = 120 }
    "http"      = @{ Quick = 180; Full = 420 }
    "grpc"      = @{ Quick = 180; Full = 420 }
    "scheduler" = @{ Quick = 180; Full = 420 }
}

function Get-ScenarioTimeout {
    param([string]$Name, [bool]$IsQuick)
    $t = $ScenarioTimeouts[$Name]
    if ($null -eq $t) { return 300 }
    if ($IsQuick) { return $t.Quick } else { return $t.Full }
}

# ============================================================
#  Invoke-BenchmarkScenario (orchestration core)
# ============================================================

function Invoke-BenchmarkScenario {
    param(
        [string]$Name,
        [string]$ServerModuleDir,
        [string]$MainClass,
        [string]$ExecArgs = $null,
        [string]$BenchmarkBaseUrl = $null,
        [string]$BenchmarkGrpcHost = $null,
        [int]$BenchmarkGrpcPort = 0,
        [int]$TimeoutSec = 300,
        [switch]$Quick,
        [switch]$VerboseOutput,
        [string[]]$ExtraJvmProperties = @()
    )

    Write-Header $Name

    $MvnArgs = @(
        "exec:java",
        "-Dexec.mainClass=$MainClass",
        "-Dexec.classpathScope=test",
        "-Dmaven.repo.local=$BenchmarkMavenRepo",
        "-q"
    )
    if ($Quick) { $MvnArgs += "-Dloomq.benchmark.quick=true" }
    if (-not [string]::IsNullOrWhiteSpace($BenchmarkBaseUrl)) {
        $MvnArgs += "-Dloomq.benchmark.baseUrl=$BenchmarkBaseUrl"
    }
    if (-not [string]::IsNullOrWhiteSpace($BenchmarkGrpcHost)) {
        $MvnArgs += "-Dloomq.benchmark.grpc.host=$BenchmarkGrpcHost"
    }
    if ($BenchmarkGrpcPort -gt 0) {
        $MvnArgs += "-Dloomq.benchmark.grpc.port=$BenchmarkGrpcPort"
    }
    if (-not [string]::IsNullOrWhiteSpace($ExecArgs)) {
        $MvnArgs += "-Dexec.args=$ExecArgs"
    }
    foreach ($prop in $ExtraJvmProperties) {
        if (-not [string]::IsNullOrWhiteSpace($prop)) { $MvnArgs += "-D$prop" }
    }

    $LogDir = Join-Path $ProjectRoot "benchmark\results\logs"
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    $SafeClass = ($MainClass -replace '[^A-Za-z0-9_.-]', '_')
    $Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $StdOutLog = Join-Path $LogDir ("scenario-$SafeClass-$Stamp.out.log")
    $StdErrLog = Join-Path $LogDir ("scenario-$SafeClass-$Stamp.err.log")

    $Process = $null
    $TimedOut = $false
    $StartTime = Get-Date
    $NextPulse = $StartTime.AddSeconds(10)
    try {
        $Process = Start-Process -FilePath "mvn" -ArgumentList $MvnArgs `
            -WorkingDirectory $ServerModuleDir -PassThru -WindowStyle Hidden `
            -RedirectStandardOutput $StdOutLog -RedirectStandardError $StdErrLog

        while (-not $Process.HasExited) {
            $Now = Get-Date
            $Elapsed = [int]($Now - $StartTime).TotalSeconds
            if ($Now -ge $NextPulse) {
                Write-Info "$Name running... ${Elapsed}s / ${TimeoutSec}s"
                $NextPulse = $Now.AddSeconds(10)
            }
            if ($Elapsed -ge $TimeoutSec) {
                $TimedOut = $true
                Stop-ProcessTree -RootPid $Process.Id
                break
            }
            Start-Sleep -Milliseconds 250
        }
    } catch { throw }

    if ($null -ne $Process -and -not $TimedOut) {
        try { $Process.WaitForExit() } catch {}
    }

    $StdOutLines = if (Test-Path $StdOutLog) { Get-Content $StdOutLog -Encoding UTF8 } else { @() }
    $StdErrLines = if (Test-Path $StdErrLog) { Get-Content $StdErrLog -Encoding UTF8 } else { @() }
    $Lines = New-Object System.Collections.Generic.List[string]
    foreach ($Line in $StdOutLines) { [void]$Lines.Add($Line) }
    foreach ($Line in $StdErrLines) { [void]$Lines.Add("[stderr] $Line") }

    foreach ($Line in $Lines) {
        if ($Line -match '^RESULT\|') {
            Write-Host $Line -ForegroundColor Green
        } elseif ($Line -match '^RESULT_ROW\|') {
            Write-Host $Line -ForegroundColor DarkGray
        } elseif ($VerboseOutput -or $Line -match '^╔|^╚|^║|^===|^---|^###') {
            Write-Host $Line
        }
    }

    $ExitCode = -1
    $HasMarker = ($Lines | Where-Object { $_ -like 'RESULT|*' }).Count -gt 0
    if ($TimedOut) {
        $ExitCode = -1
    } elseif ($HasMarker) {
        $ExitCode = 0
    } elseif ($null -ne $Process) {
        try { $Process.Refresh() } catch {}
        if ($null -ne $Process.ExitCode) { $ExitCode = [int]$Process.ExitCode }
    }
    if ($TimedOut) {
        $TailOut = $StdOutLines | Select-Object -Last 40
        $TailErr = $StdErrLines | Select-Object -Last 40
        throw "Scenario '$Name' timed out after ${TimeoutSec}s.`n--- stdout tail ---`n$($TailOut -join [Environment]::NewLine)`n--- stderr tail ---`n$($TailErr -join [Environment]::NewLine)"
    }

    $Marker = $Lines | Where-Object { $_ -like 'RESULT|*' } | Select-Object -Last 1
    $Rows = @($Lines | Where-Object { $_ -like 'RESULT_ROW|*' })
    $DurationSec = [math]::Round(((Get-Date) - $StartTime).TotalSeconds, 1)

    return [pscustomobject]@{
        Name = $Name
        MainClass = $MainClass
        ExitCode = $ExitCode
        TimedOut = $TimedOut
        DurationSec = $DurationSec
        StdOutLog = $StdOutLog
        StdErrLog = $StdErrLog
        Lines = $Lines.ToArray()
        Marker = $Marker
        Rows = $Rows
        Data = ConvertFrom-BenchmarkMarker $Marker
    }
}

# ============================================================
#  RESULT marker → JSON data model
# ============================================================

function ConvertTo-ReportData {
    param(
        $InternalResult, $WalResult, $StorageResult,
        $HttpResult, $GrpcResult, $SchedulerResult,
        $HttpStressResult, $GrpcStressResult,
        [string]$GitCommit, [string]$GitVersion, [string]$JavaVersion
    )

    $ts = Get-Date -Format "yyyyMMdd-HHmmss"
    $branch = try { (git rev-parse --abbrev-ref HEAD 2>$null).Trim() } catch { "unknown" }
    $cpuCores = [System.Environment]::ProcessorCount
    $maxMem = [math]::Round((Get-CimInstance Win32_OperatingSystem -ErrorAction SilentlyContinue).TotalVisibleMemorySize / 1MB, 0)

    $envData = @{
        timestamp   = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        commit      = $GitCommit
        branch      = $branch
        java_version = $JavaVersion
        os          = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
        cpu_cores   = $cpuCores
        max_memory  = "${maxMem}GB"
    }

    # ── Create Path ──
    # ProtocolBenchmark RESULT_ROW|threads=N|qps=N|avg_ms=N|p50_ms=N|p90_ms=N|p99_ms=N|success=N|fail=N
    $cpRows = @()
    $httpRowMap = @{}
    $grpcRowMap = @{}

    if ($HttpResult -and $HttpResult.Rows) {
        foreach ($r in $HttpResult.Rows) {
            $d = ConvertFrom-BenchmarkMarker $r
            $t = Get-ResultInt $d "threads"
            $httpRowMap[$t] = @{
                qps = Get-ResultDouble $d "qps"
                p50 = Get-ResultDouble $d "p50_ms"
                p90 = Get-ResultDouble $d "p90_ms"
                p99 = Get-ResultDouble $d "p99_ms"
            }
        }
    }
    if ($GrpcResult -and $GrpcResult.Rows) {
        foreach ($r in $GrpcResult.Rows) {
            $d = ConvertFrom-BenchmarkMarker $r
            $t = Get-ResultInt $d "threads"
            $grpcRowMap[$t] = @{
                qps = Get-ResultDouble $d "qps"
                p50 = Get-ResultDouble $d "p50_ms"
                p90 = Get-ResultDouble $d "p90_ms"
                p99 = Get-ResultDouble $d "p99_ms"
            }
        }
    }

    $allThreads = @($httpRowMap.Keys + $grpcRowMap.Keys) | Sort-Object -Unique
    foreach ($t in $allThreads) {
        $hr = $httpRowMap[$t]
        $gr = $grpcRowMap[$t]
        $cpRows += @{
            threads  = $t
            http_qps = if ($hr) { $hr.qps } else { $null }
            http_p50 = if ($hr) { $hr.p50 } else { $null }
            http_p90 = if ($hr) { $hr.p90 } else { $null }
            http_p99 = if ($hr) { $hr.p99 } else { $null }
            grpc_qps = if ($gr) { $gr.qps } else { $null }
            grpc_p50 = if ($gr) { $gr.p50 } else { $null }
            grpc_p90 = if ($gr) { $gr.p90 } else { $null }
            grpc_p99 = if ($gr) { $gr.p99 } else { $null }
        }
    }

    # ── Summary ──
    $httpPeak = if ($HttpResult) { Get-ResultDouble $HttpResult.Data "peak_qps" } else { $null }
    $grpcPeak = if ($GrpcResult) { Get-ResultDouble $GrpcResult.Data "peak_qps" } else { $null }
    $httpFail = if ($HttpResult) { Get-ResultDouble $HttpResult.Data "fail_rate" } else { $null }
    $grpcFail = if ($GrpcResult) { Get-ResultDouble $GrpcResult.Data "fail_rate" } else { $null }
    $memPerIntent = if ($InternalResult -and $InternalResult.Data) { Get-ResultDouble $InternalResult.Data "memory_bytes_per_intent" } else { $null }

    $summaryData = @{
        http_peak_qps          = $httpPeak
        grpc_peak_qps          = $grpcPeak
        http_inflection_threads = if ($HttpStressResult) { Get-ResultInt $HttpStressResult.Data "inflection_threads" } else { $null }
        grpc_inflection_threads = if ($GrpcStressResult) { Get-ResultInt $GrpcStressResult.Data "inflection_threads" } else { $null }
        http_inflection_p99    = if ($HttpStressResult) { Get-ResultDouble $HttpStressResult.Data "best_p99_ms" } else { $null }
        grpc_inflection_p99    = if ($GrpcStressResult) { Get-ResultDouble $GrpcStressResult.Data "best_p99_ms" } else { $null }
        http_fail_rate         = $httpFail
        grpc_fail_rate         = $grpcFail
        memory_per_intent      = if ($memPerIntent) { "{0:N0}B" -f $memPerIntent } else { $null }
        slo_pass_http          = $null
        slo_pass_grpc          = $null
    }

    # ── Scheduler tiers ──
    # 从 SchedulerResult.Lines 中直接按 tier 提取 RESULT_ROW + RESULT_LATENCY + RESULT_E2E_LATENCY
    $tierData = @()
    if ($SchedulerResult -and $SchedulerResult.Lines) {
        foreach ($tierName in @("ULTRA", "FAST", "HIGH", "STANDARD", "ECONOMY")) {
            # RESULT_ROW|tier=ULTRA|concurrency=200|qps=14025|...
            $rowLine = $SchedulerResult.Lines | Where-Object { $_ -match "^RESULT_ROW\|.*tier=$tierName\|" } | Select-Object -First 1
            if (-not $rowLine) { continue }
            $rowD = ConvertFrom-BenchmarkMarker $rowLine

            # RESULT_LATENCY|tier=ULTRA|type=wakeup_us|p50=5000|p95=100000|p99=100000|...
            $latLine = $SchedulerResult.Lines | Where-Object { $_ -match "^RESULT_LATENCY\|.*tier=$tierName\|" } | Select-Object -First 1
            $latD = if ($latLine) { ConvertFrom-BenchmarkMarker $latLine } else { @{} }

            # RESULT_E2E_LATENCY|tier=ULTRA|p50=1005|p95=1375|p99=1407|...
            $e2eLine = $SchedulerResult.Lines | Where-Object { $_ -match "^RESULT_E2E_LATENCY\|.*tier=$tierName\|" } | Select-Object -First 1
            $e2eD = if ($e2eLine) { ConvertFrom-BenchmarkMarker $e2eLine } else { @{} }

            # RESULT_SEMAPHORE|tier=ULTRA|utilization_pct=0.0
            $semLine = $SchedulerResult.Lines | Where-Object { $_ -match "^RESULT_SEMAPHORE\|.*tier=$tierName\|" } | Select-Object -First 1
            $semD = if ($semLine) { ConvertFrom-BenchmarkMarker $semLine } else { @{} }

            # RESULT_LIFECYCLE|tier=ULTRA|total=20000|acked=20000
            $lcLine = $SchedulerResult.Lines | Where-Object { $_ -match "^RESULT_LIFECYCLE\|.*tier=$tierName\|" } | Select-Object -First 1
            $lcD = if ($lcLine) { ConvertFrom-BenchmarkMarker $lcLine } else { @{} }

            $total = Get-ResultDouble $lcD "total"
            $acked = Get-ResultDouble $lcD "acked"
            $compRate = if ($total -gt 0) { [math]::Round($acked / $total * 100, 1) } else { 0 }

            $tierData += @{
                tier             = $tierName
                concurrency      = Get-ResultInt $rowD "concurrency"
                qps              = Get-ResultDouble $rowD "qps"
                wake_p50         = Get-ResultDouble $latD "p50"
                wake_p95         = Get-ResultDouble $latD "p95"
                wake_p99         = Get-ResultDouble $latD "p99"
                e2e_p50          = Get-ResultDouble $e2eD "p50"
                e2e_p95          = Get-ResultDouble $e2eD "p95"
                e2e_p99          = Get-ResultDouble $e2eD "p99"
                util_pct         = Get-ResultDouble $semD "utilization_pct"
                backpressure_pct = Get-ResultDouble $rowD "backpressure"
                completion_pct   = $compRate
            }
        }
    }

    # ── Internal ──
    $internalItems = @()
    if ($InternalResult -and $InternalResult.Data) {
        $id = $InternalResult.Data
        $internalItems += @{ benchmark = "IntentStore"; metric = "Write QPS"; value = (Get-ResultDouble $id "direct_store_qps") }
        $internalItems += @{ benchmark = "Memory"; metric = "bytes/intent"; value = (Get-ResultDouble $id "memory_bytes_per_intent") }
    }
    # Cold-swap: 从 RESULT_COLD_SWAP marker 解析
    if ($InternalResult -and $InternalResult.Lines) {
        $coldLine = $InternalResult.Lines | Where-Object { $_ -match '^RESULT_COLD_SWAP\|' } | Select-Object -First 1
        if ($coldLine) {
            $coldD = ConvertFrom-BenchmarkMarker $coldLine
            $internalItems += @{ benchmark = "Cold-Swap"; metric = "Savings%"; value = (Get-ResultDouble $coldD "saved_pct") }
        }
    }
    # WAL: RESULT|wal_throughput|mode=DURABLE|throughput=N
    if ($WalResult -and $WalResult.Lines) {
        $walLines = $WalResult.Lines | Where-Object { $_ -match '^RESULT\|wal_throughput\|' }
        foreach ($wl in $walLines) {
            $wd = ConvertFrom-BenchmarkMarker $wl
            $mode = Get-ResultString $wd "mode"
            $tp = Get-ResultDouble $wd "throughput"
            $internalItems += @{ benchmark = "WAL $mode"; metric = "ops/s"; value = $tp }
        }
    }
    # Storage: RESULT|storage|type=concurrent|save_ms=N
    if ($StorageResult -and $StorageResult.Lines) {
        $stoLines = $StorageResult.Lines | Where-Object { $_ -match '^RESULT\|storage\|' }
        foreach ($sl in $stoLines) {
            $sd = ConvertFrom-BenchmarkMarker $sl
            $stype = Get-ResultString $sd "type"
            $saveMs = Get-ResultDouble $sd "save_ms"
            $label = if ($stype -eq "concurrent") { "Storage InMem" } else { "Storage RocksDB" }
            $internalItems += @{ benchmark = $label; metric = "save_ms"; value = $saveMs }
        }
    }

    # ── SLO ──
    $sloItems = @()
    $configPath = Join-Path $ProjectRoot "benchmark\config.json"
    $sloConfig = @{}
    if (Test-Path $configPath) {
        $sloConfig = (Get-Content $configPath -Raw | ConvertFrom-Json).slo
    }
    foreach ($tierEntry in $tierData) {
        $tierName = $tierEntry.tier
        $sloDef = $sloConfig.$tierName
        if (-not $sloDef) { continue }
        $item = @{ tier = $tierName }
        # config.json uses: p95_wakeup_us, p99_wakeup_us, p95_e2e_ms, p99_e2e_ms
        # tierData uses:     wake_p95,      wake_p99,      e2e_p95,      e2e_p99
        foreach ($entry in @(
            @{ key = "wake_p95"; configKey = "p95_wakeup_us" },
            @{ key = "wake_p99"; configKey = "p99_wakeup_us" },
            @{ key = "e2e_p95";  configKey = "p95_e2e_ms"   },
            @{ key = "e2e_p99";  configKey = "p99_e2e_ms"   }
        )) {
            $key = $entry.key
            $target = $sloDef.$($entry.configKey)
            $actual = $tierEntry.$key
            if ($key -like "e2e_*") {
                $item["${key}_target"] = if ($target) { "{0}ms" -f $target } else { "-" }
                $item["${key}_actual"] = if ($actual) { "{0}ms" -f ([math]::Round($actual, 1)) } else { "-" }
            } else {
                $item["${key}_target"] = if ($target) { "{0}ms" -f ([math]::Round($target / 1000, 1)) } else { "-" }
                $item["${key}_actual"] = if ($actual) { "{0}ms" -f ([math]::Round($actual / 1000, 1)) } else { "-" }
            }
            $item["${key}_pass"] = if ($target -and $actual) { $actual -le $target } else { $null }
        }
        $sloItems += $item
    }

    # ── Regression ──
    $regression = $null
    $lastJson = Get-ChildItem -Path (Join-Path $ProjectRoot "benchmark\results\reports") -Filter "benchmark-report-*.json" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($lastJson) {
        $lastData = Get-Content $lastJson.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
        $lastHttpPeak = $lastData.summary.http_peak_qps
        $lastGrpcPeak = $lastData.summary.grpc_peak_qps
        if ($lastHttpPeak -and $httpPeak) {
            $regression = @{
                http_qps_delta = ($httpPeak - $lastHttpPeak) / $lastHttpPeak
                grpc_qps_delta = if ($lastGrpcPeak -and $grpcPeak) { ($grpcPeak - $lastGrpcPeak) / $lastGrpcPeak } else { $null }
            }
        }
    }

    return @{
        timestamp  = $ts
        environment = $envData
        summary    = $summaryData
        create_path = @{ rows = $cpRows }
        scheduler  = @{ tiers = $tierData }
        internal   = @{ items = $internalItems }
        slo        = @{ items = $sloItems }
        regression = $regression
    }
}

# ============================================================
#  Main program
# ============================================================

$script:PauseOnExit = (-not $NoPause) -and ($KeepOpen -or (Test-LaunchedFromExplorer))

if ($Help) {
    $v = Get-GitVersion
    Write-Host @"
LoomQ Performance Benchmark $v

Usage: .\benchmark.ps1 [Options]

Options:
  -Quick              Quick test mode (reduced duration/thread tiers)
  -Stress             Include stress test (multi-tier pressure sweep)
  -StressOnly <name>  Only run stress: http / grpc
  -Scenario <name>    Run specific scenario: internal / create / scheduler / all
  -GrpcTier <tier>    gRPC precision tier: ULTRA/FAST/HIGH/STANDARD/ECONOMY
  -NoCompile          Skip compilation
  -Compare            Show latest report comparison
  -VerboseOutput      Show full scenario logs
  -Help               Show this help

Output:
  Excel report: benchmark/results/reports/benchmark-report-{timestamp}.xlsx
  MD report:    benchmark/results/reports/benchmark-report-{timestamp}.md

Examples:
  .\benchmark.ps1                         # Full test suite
  .\benchmark.ps1 -Quick                  # Quick validation
  .\benchmark.ps1 -Stress                 # Full + stress sweep
  .\benchmark.ps1 -Stress -StressOnly grpc # gRPC stress only
  .\benchmark.ps1 -Scenario internal      # Internal components only
"@
    Exit-BenchmarkScript -ExitCode 0
}

# Banner
$v = Get-GitVersion
Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Magenta
Write-Host "|          LoomQ Performance Benchmark $v                  |" -ForegroundColor Magenta
Write-Host "+============================================================+" -ForegroundColor Magenta
Write-Host ""

# Find project root
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $ScriptDir)

if (-not (Test-Path "$ProjectRoot\pom.xml")) {
    Write-Error "Cannot find pom.xml. Run from project root."
}
Set-Location $ProjectRoot
Write-Info "Project root: $ProjectRoot"
$BenchmarkMavenRepo = Join-Path $ProjectRoot "benchmark\results\m2repo"
New-Item -ItemType Directory -Force -Path $BenchmarkMavenRepo | Out-Null

# Environment check
Write-Header "Environment Check"
$JavaHome = Test-Java
$PreviousErrorActionPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
try {
    $JavaVersion = & "$JavaHome\bin\java.exe" -version 2>&1 | Select-Object -First 1
} finally {
    $ErrorActionPreference = $PreviousErrorActionPreference
}
Write-Info "Java: $JavaHome"
Write-Host "   Version: $JavaVersion" -ForegroundColor Gray

$MavenCmd = Get-Command mvn -ErrorAction SilentlyContinue
if (-not $MavenCmd) {
    Write-Error "Maven not found! Install and add to PATH."
}

$GitCommit = Get-GitCommit
$GitVersion = Get-GitVersion
Write-Host "   Git: $GitCommit ($GitVersion)" -ForegroundColor Gray
Write-Host "   CPU: $([System.Environment]::ProcessorCount) cores" -ForegroundColor Gray

# Compare mode
if ($Compare) {
    Write-Header "Latest Report"
    $ReportDir = Join-Path $ProjectRoot "benchmark\results\reports"
    $LatestMd = Get-ChildItem -Path $ReportDir -Filter "benchmark-report-*.md" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($LatestMd) {
        Get-Content $LatestMd.FullName -Encoding UTF8 | ForEach-Object { Write-Host $_ }
    } else {
        Write-Warning "No benchmark report found."
    }
    Exit-BenchmarkScript -ExitCode 0
}

# Compile
if (-not $NoCompile) {
    Write-Header "Compiling Project"
    $CompileStart = Get-Date
    Write-Host "Running: mvn compile test-compile -q" -ForegroundColor Gray
    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $CompileOutput = & mvn compile test-compile "-Dmaven.repo.local=$BenchmarkMavenRepo" -q 2>&1
    } finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Compilation failed!"
        $CompileOutput | Select-Object -Last 30 | ForEach-Object { Write-Host "  $_" }
    }
    $CompileTime = [math]::Round(((Get-Date) - $CompileStart).TotalSeconds, 1)
    Write-Success "Compilation completed ($CompileTime seconds)"
}

Ensure-BenchmarkRepo -ProjectRoot $ProjectRoot -RepoPath $BenchmarkMavenRepo -ForceRefresh:(-not $NoCompile)

Write-Header "Running Benchmark Scenarios"
Stop-OrphanBenchmarkServers -ProjectRoot $ProjectRoot

$ServerModuleDir = Join-Path $ProjectRoot "loomq-server"
$TempServerHandle = $null
$TempDataDir = $null
$IsQuick = [bool]$Quick

# Scenario filtering
$RunInternal = $Scenario -eq "all" -or $Scenario -eq "internal"
$RunCreate = ($Scenario -eq "all" -or $Scenario -eq "create") -and -not ($StressOnly -and $StressOnly -ne "http" -and $StressOnly -ne "grpc")
$RunScheduler = ($Scenario -eq "all" -or $Scenario -eq "scheduler") -and [string]::IsNullOrWhiteSpace($StressOnly)

$InternalResult = $null; $WalResult = $null; $StorageResult = $null
$HttpResult = $null; $GrpcResult = $null; $SchedulerResult = $null
$HttpStressResult = $null; $GrpcStressResult = $null

try {
    # ── Phase 1: Internal (no server) ──
    if ($RunInternal) {
        $InternalResult = Invoke-BenchmarkScenario `
            -Name "1) IntentStore & Memory" `
            -ServerModuleDir $ServerModuleDir `
            -MainClass "com.loomq.benchmark.InternalBenchmark" `
            -TimeoutSec (Get-ScenarioTimeout "internal" $IsQuick) `
            -Quick:$Quick -VerboseOutput:$VerboseOutput

        if ($InternalResult.ExitCode -ne 0) {
            throw "Internal benchmark failed (exit $($InternalResult.ExitCode))"
        }

        $WalResult = Invoke-BenchmarkScenario `
            -Name "1b) WAL Write Throughput" `
            -ServerModuleDir $ServerModuleDir `
            -MainClass "com.loomq.benchmark.WalThroughputBenchmark" `
            -TimeoutSec (Get-ScenarioTimeout "wal" $IsQuick) `
            -Quick:$Quick -VerboseOutput:$VerboseOutput

        $StorageResult = Invoke-BenchmarkScenario `
            -Name "1c) Storage Engine Comparison" `
            -ServerModuleDir $ServerModuleDir `
            -MainClass "com.loomq.benchmark.StorageBenchmark" `
            -TimeoutSec (Get-ScenarioTimeout "storage" $IsQuick) `
            -Quick:$Quick -VerboseOutput:$VerboseOutput
    }

    # ── Phase 2: Server-based tests ──
    if ($RunCreate -or $RunScheduler -or $Stress) {
        Write-Info "Cooling down (30s) to stabilize CPU thermals..."
        Start-Sleep -Seconds 30

        $ServerJarPath = Ensure-ServerJar -ProjectRoot $ProjectRoot -ServerModuleDir $ServerModuleDir -AllowBuild:(-not $NoCompile)
        $TempServerPort = Get-FreePort
        $TempGrpcPort = Get-FreePort
        $TempDataDir = Join-Path $ProjectRoot ("benchmark\results\runtime\data-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
        New-Item -ItemType Directory -Force -Path $TempDataDir | Out-Null
        $TempNodeId = "bench-" + (Get-Date -Format "yyyyMMddHHmmss")

        Write-Info "Starting server on http://127.0.0.1:$TempServerPort (gRPC: $TempGrpcPort)"

        $TempServerHandle = Start-BenchmarkServer `
            -ProjectRoot $ProjectRoot -JavaHome $JavaHome `
            -ServerJarPath $ServerJarPath -Port $TempServerPort `
            -GrpcPort $TempGrpcPort -DataDir $TempDataDir -NodeId $TempNodeId

        try {
            # HTTP Create Path
            if ($RunCreate) {
                $HttpResult = Invoke-BenchmarkScenario `
                    -Name "2) HTTP Create Path" `
                    -ServerModuleDir $ServerModuleDir `
                    -MainClass "com.loomq.benchmark.HttpVirtualThreadBenchmark" `
                    -BenchmarkBaseUrl $TempServerHandle.BaseUrl `
                    -TimeoutSec (Get-ScenarioTimeout "http" $IsQuick) `
                    -Quick:$Quick -VerboseOutput:$VerboseOutput

                if ($HttpResult.ExitCode -ne 0) {
                    throw "HTTP benchmark failed (exit $($HttpResult.ExitCode))"
                }

                Write-Info "Cooling down (15s)..."
                Start-Sleep -Seconds 15
            }

            # gRPC Create Path
            if ($RunCreate) {
                $GrpcExtraProps = @()
                if (-not [string]::IsNullOrWhiteSpace($GrpcTier)) {
                    $GrpcExtraProps += "loomq.benchmark.grpc.tier=$GrpcTier"
                }

                $GrpcResult = Invoke-BenchmarkScenario `
                    -Name "2b) gRPC Create Path" `
                    -ServerModuleDir $ServerModuleDir `
                    -MainClass "com.loomq.benchmark.GrpcVirtualThreadBenchmark" `
                    -BenchmarkGrpcHost "127.0.0.1" `
                    -BenchmarkGrpcPort $TempGrpcPort `
                    -TimeoutSec (Get-ScenarioTimeout "grpc" $IsQuick) `
                    -Quick:$Quick -VerboseOutput:$VerboseOutput `
                    -ExtraJvmProperties $GrpcExtraProps

                if ($GrpcResult.ExitCode -ne 0) {
                    throw "gRPC benchmark failed (exit $($GrpcResult.ExitCode))"
                }
            }

            # Scheduler Trigger
            if ($RunScheduler) {
                Write-Info "Cooling down (15s)..."
                Start-Sleep -Seconds 15

                $SchedulerResult = Invoke-BenchmarkScenario `
                    -Name "3) Scheduler Trigger" `
                    -ServerModuleDir $ServerModuleDir `
                    -MainClass "com.loomq.scheduler.SchedulerTriggerBenchmarkWithMockServer" `
                    -TimeoutSec (Get-ScenarioTimeout "scheduler" $IsQuick) `
                    -Quick:$Quick -VerboseOutput:$VerboseOutput

                if ($SchedulerResult.ExitCode -ne 0) {
                    throw "Scheduler benchmark failed (exit $($SchedulerResult.ExitCode))"
                }
            }

            # ── Stress test phase ──
            if ($Stress) {
                $StressExtraProps = @(
                    "loomq.benchmark.stress=true"
                    "loomq.benchmark.duration_sec=30"
                    "loomq.benchmark.cooldown_ms=5000"
                )
                if (-not [string]::IsNullOrWhiteSpace($GrpcTier)) {
                    $StressExtraProps += "loomq.benchmark.grpc.tier=$GrpcTier"
                }
                $StressTimeout = 600
                $RunHttpStress = [string]::IsNullOrWhiteSpace($StressOnly) -or $StressOnly -eq "http"
                $RunGrpcStress = [string]::IsNullOrWhiteSpace($StressOnly) -or $StressOnly -eq "grpc"

                Write-Host ""
                Write-Host "+============================================================+" -ForegroundColor Yellow
                Write-Host "|  STRESS MODE                                               |" -ForegroundColor Yellow
                Write-Host "+============================================================+" -ForegroundColor Yellow
                Write-Host ""

                Write-Info "Cooling down (30s) before stress phase..."
                Start-Sleep -Seconds 30

                if ($RunHttpStress) {
                    $HttpStressResult = Invoke-BenchmarkScenario `
                        -Name "2c) HTTP Stress" `
                        -ServerModuleDir $ServerModuleDir `
                        -MainClass "com.loomq.benchmark.HttpVirtualThreadBenchmark" `
                        -BenchmarkBaseUrl $TempServerHandle.BaseUrl `
                        -TimeoutSec $StressTimeout `
                        -VerboseOutput:$VerboseOutput `
                        -ExtraJvmProperties $StressExtraProps

                    Write-Info "Cooling down (15s)..."
                    Start-Sleep -Seconds 15
                }

                if ($RunGrpcStress) {
                    $GrpcStressResult = Invoke-BenchmarkScenario `
                        -Name "2d) gRPC Stress" `
                        -ServerModuleDir $ServerModuleDir `
                        -MainClass "com.loomq.benchmark.GrpcVirtualThreadBenchmark" `
                        -BenchmarkGrpcHost "127.0.0.1" `
                        -BenchmarkGrpcPort $TempGrpcPort `
                        -TimeoutSec $StressTimeout `
                        -VerboseOutput:$VerboseOutput `
                        -ExtraJvmProperties $StressExtraProps
                }
            }
        } finally {
            Stop-BenchmarkServer -ServerHandle $TempServerHandle
        }
    }

    # ── Generate reports ──
    Write-Header "Generating Reports"

    $ReportData = ConvertTo-ReportData `
        -InternalResult $InternalResult -WalResult $WalResult -StorageResult $StorageResult `
        -HttpResult $HttpResult -GrpcResult $GrpcResult -SchedulerResult $SchedulerResult `
        -HttpStressResult $HttpStressResult -GrpcStressResult $GrpcStressResult `
        -GitCommit $GitCommit -GitVersion $GitVersion -JavaVersion $JavaVersion

    $ReportDir = Join-Path $ProjectRoot "benchmark\results\reports"
    New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null

    # Generate Excel (JSON 中间文件写到临时目录)
    $ExcelPath = New-BenchmarkExcel -Data $ReportData
    if ($ExcelPath) {
        Write-Success "Excel: $ExcelPath"
    } else {
        Write-Warning "Excel generation failed (is Python + xlsxwriter installed?)"
    }

    # Generate Markdown
    $MdPath = New-BenchmarkMarkdown -Data $ReportData
    if ($MdPath) {
        Write-Success "Markdown: $MdPath"
    }

    # Save JSON for regression comparison (在 reports 目录)
    $JsonPath = Join-Path $ReportDir "benchmark-report-$($ReportData.timestamp).json"
    [System.IO.File]::WriteAllText($JsonPath, ($ReportData | ConvertTo-Json -Depth 10), [System.Text.UTF8Encoding]::new($true))
    Write-Info "JSON (regression): $JsonPath"

    # Rotate old reports (keep latest N)
    $KeepRecent = 10
    $configPath = Join-Path $ProjectRoot "benchmark\config.json"
    if (Test-Path $configPath) {
        $rc = (Get-Content $configPath -Raw | ConvertFrom-Json).rotation
        if ($rc -and $rc.keep_recent) { $KeepRecent = [int]$rc.keep_recent }
    }
    foreach ($ext in @("*.json", "*.md", "*.xlsx")) {
        $oldFiles = Get-ChildItem -Path $ReportDir -Filter $ext -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending | Select-Object -Skip $KeepRecent
        foreach ($f in $oldFiles) {
            Remove-Item $f.FullName -ErrorAction SilentlyContinue
        }
    }

    # Print summary
    Write-Host ""
    Write-Header "Summary"
    $s = $ReportData.summary
    if ($s.http_peak_qps) {
        Write-Host ("   HTTP Peak QPS:      {0:N0}" -f $s.http_peak_qps) -ForegroundColor Cyan
    }
    if ($s.grpc_peak_qps) {
        Write-Host ("   gRPC Peak QPS:      {0:N0}" -f $s.grpc_peak_qps) -ForegroundColor Cyan
    }
    if ($ReportData.regression) {
        $r = $ReportData.regression
        if ($r.http_qps_delta) {
            $sign = if ($r.http_qps_delta -ge 0) { "+" } else { "" }
            Write-Host ("   HTTP vs last:       {0}{1:P1}" -f $sign, $r.http_qps_delta) -ForegroundColor $(if ($r.http_qps_delta -ge 0) { "Green" } else { "Red" })
        }
        if ($r.grpc_qps_delta) {
            $sign = if ($r.grpc_qps_delta -ge 0) { "+" } else { "" }
            Write-Host ("   gRPC vs last:       {0}{1:P1}" -f $sign, $r.grpc_qps_delta) -ForegroundColor $(if ($r.grpc_qps_delta -ge 0) { "Green" } else { "Red" })
        }
    }

    # Stress summary
    if ($HttpStressResult -or $GrpcStressResult) {
        Write-Host ""
        Write-Header "Stress Results"
        foreach ($sr in @(@{Label="HTTP"; R=$HttpStressResult}, @{Label="gRPC"; R=$GrpcStressResult})) {
            if ($sr.R) {
                $d = $sr.R.Data
                $peak = Get-ResultDouble $d "peak_qps"
                $inflect = Get-ResultInt $d "inflection_threads"
                Write-Host ("   {0,-8} peak={1:N0} QPS  inflection={2} threads" -f $sr.Label, $peak, $inflect)
            }
        }
    }

    Write-Host ""
    Write-Host "Tip: use -Compare to show the latest saved report." -ForegroundColor Gray
    Exit-BenchmarkScript -ExitCode 0

} catch {
    Write-Error "Benchmark run failed!"
    throw
} finally {
    if ($TempServerHandle) {
        Stop-BenchmarkServer -ServerHandle $TempServerHandle
    }
    if ($TempDataDir -and (Test-Path $TempDataDir)) {
        try { Remove-Item -Path $TempDataDir -Recurse -Force -ErrorAction SilentlyContinue } catch {}
    }
}
