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

    # Environment
    $envData = @{
        timestamp   = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        commit      = $GitCommit
        branch      = $branch
        java_version = $JavaVersion
        os          = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
        cpu_cores   = $cpuCores
        max_memory  = "${maxMem}GB"
    }

    # Create Path rows
    $cpRows = @()
    $httpRowsData = @()
    $grpcRowsData = @()

    if ($HttpResult -and $HttpResult.Rows) {
        foreach ($r in $HttpResult.Rows) {
            $d = ConvertFrom-BenchmarkMarker $r
            $httpRowsData += @{
                threads = Get-ResultInt $d "threads"
                qps     = Get-ResultDouble $d "qps"
                p50     = Get-ResultDouble $d "p50"
                p90     = Get-ResultDouble $d "p90"
                p99     = Get-ResultDouble $d "p99"
            }
        }
    }
    if ($GrpcResult -and $GrpcResult.Rows) {
        foreach ($r in $GrpcResult.Rows) {
            $d = ConvertFrom-BenchmarkMarker $r
            $grpcRowsData += @{
                threads = Get-ResultInt $d "threads"
                qps     = Get-ResultDouble $d "qps"
                p50     = Get-ResultDouble $d "p50"
                p90     = Get-ResultDouble $d "p90"
                p99     = Get-ResultDouble $d "p99"
            }
        }
    }

    $maxRows = [Math]::Max($httpRowsData.Count, $grpcRowsData.Count)
    for ($i = 0; $i -lt $maxRows; $i++) {
        $hr = if ($i -lt $httpRowsData.Count) { $httpRowsData[$i] } else { @{} }
        $gr = if ($i -lt $grpcRowsData.Count) { $grpcRowsData[$i] } else { @{} }
        $cpRows += @{
            threads  = if ($null -ne $hr.threads) { $hr.threads } else { $gr.threads }
            http_qps = $hr.qps;  http_p50 = $hr.p50;  http_p90 = $hr.p90;  http_p99 = $hr.p99
            grpc_qps = $gr.qps;  grpc_p50 = $gr.p50;  grpc_p90 = $gr.p90;  grpc_p99 = $gr.p99
        }
    }

    # Summary
    $httpPeak = if ($HttpResult) { Get-ResultDouble $HttpResult.Data "peak_qps" } else { $null }
    $grpcPeak = if ($GrpcResult) { Get-ResultDouble $GrpcResult.Data "peak_qps" } else { $null }
    $httpInflect = if ($HttpStressResult) { Get-ResultInt $HttpStressResult.Data "inflection_threads" } else { $null }
    $grpcInflect = if ($GrpcStressResult) { Get-ResultInt $GrpcStressResult.Data "inflection_threads" } else { $null }
    $httpInflectP99 = if ($HttpStressResult) { Get-ResultDouble $HttpStressResult.Data "best_p99_ms" } else { $null }
    $grpcInflectP99 = if ($GrpcStressResult) { Get-ResultDouble $GrpcStressResult.Data "best_p99_ms" } else { $null }
    $httpFail = if ($HttpResult) { Get-ResultDouble $HttpResult.Data "fail_rate" } else { $null }
    $grpcFail = if ($GrpcResult) { Get-ResultDouble $GrpcResult.Data "fail_rate" } else { $null }
    $memPerIntent = if ($InternalResult) { Get-ResultDouble $InternalResult.Data "bytes_per_intent" } else { $null }

    $summaryData = @{
        http_peak_qps          = $httpPeak
        grpc_peak_qps          = $grpcPeak
        http_inflection_threads = $httpInflect
        grpc_inflection_threads = $grpcInflect
        http_inflection_p99    = $httpInflectP99
        grpc_inflection_p99    = $grpcInflectP99
        http_fail_rate         = $httpFail
        grpc_fail_rate         = $grpcFail
        memory_per_intent      = if ($memPerIntent) { "{0:N0}B" -f $memPerIntent } else { $null }
        slo_pass_http          = $null
        slo_pass_grpc          = $null
    }

    # Scheduler tiers
    $tierData = @()
    if ($SchedulerResult) {
        $ParsedScheduler = ConvertTo-ParsedBenchmark $SchedulerResult
        foreach ($tierName in @("ULTRA", "FAST", "HIGH", "STANDARD", "ECONOMY")) {
            $tierMarker = $ParsedScheduler.Rows | Where-Object {
                $d = ConvertFrom-BenchmarkMarker $_; (Get-ResultString $d "tier") -eq $tierName
            } | Select-Object -First 1
            if ($tierMarker) {
                $td = ConvertFrom-BenchmarkMarker $tierMarker
                $tierData += @{
                    tier             = $tierName
                    concurrency      = Get-ResultInt $td "concurrency"
                    qps              = Get-ResultDouble $td "qps"
                    wake_p50         = Get-ResultDouble $td "wake_p50_us"
                    wake_p95         = Get-ResultDouble $td "wake_p95_us"
                    wake_p99         = Get-ResultDouble $td "wake_p99_us"
                    e2e_p50          = Get-ResultDouble $td "e2e_p50_ms"
                    e2e_p95          = Get-ResultDouble $td "e2e_p95_ms"
                    e2e_p99          = Get-ResultDouble $td "e2e_p99_ms"
                    util_pct         = Get-ResultDouble $td "semaphore_util_pct"
                    backpressure_pct = Get-ResultDouble $td "backpressure_pct"
                    completion_pct   = Get-ResultDouble $td "completion_rate"
                }
            }
        }
    }

    # Internal
    $internalItems = @()
    if ($InternalResult) {
        $id = $InternalResult.Data
        $internalItems += @{ benchmark = "IntentStore"; metric = "Write QPS"; value = (Get-ResultDouble $id "direct_store_qps") }
        $internalItems += @{ benchmark = "Memory"; metric = "bytes/intent"; value = (Get-ResultDouble $id "bytes_per_intent") }
        $internalItems += @{ benchmark = "Cold-Swap"; metric = "Savings%"; value = (Get-ResultDouble $id "cold_swap_savings_pct") }
    }
    if ($WalResult) {
        $wd = $WalResult.Data
        $internalItems += @{ benchmark = "WAL DURABLE"; metric = "ops/s"; value = (Get-ResultDouble $wd "durable_ops_per_sec") }
        $internalItems += @{ benchmark = "WAL ASYNC"; metric = "ops/s"; value = (Get-ResultDouble $wd "async_ops_per_sec") }
    }
    if ($StorageResult) {
        $sd = $StorageResult.Data
        $internalItems += @{ benchmark = "Storage InMem"; metric = "save_ms"; value = (Get-ResultDouble $sd "concurrent_save_ms") }
        $internalItems += @{ benchmark = "Storage RocksDB"; metric = "save_ms"; value = (Get-ResultDouble $sd "rocksdb_save_ms") }
    }

    # SLO
    $sloItems = @()
    if ($SchedulerResult) {
        $configPath = Join-Path $ProjectRoot "benchmark\config.json"
        $sloConfig = @{}
        if (Test-Path $configPath) {
            $sloConfig = (Get-Content $configPath -Raw | ConvertFrom-Json).slo
        }
        foreach ($tierName in @("ULTRA", "FAST", "HIGH", "STANDARD", "ECONOMY")) {
            $td = $tierData | Where-Object { $_.tier -eq $tierName } | Select-Object -First 1
            if (-not $td) { continue }
            $sloDef = $sloConfig.$tierName
            $item = @{ tier = $tierName }
            foreach ($key in @("wake_p95", "wake_p99", "e2e_p95", "e2e_p99")) {
                $targetKey = "${key}_us"
                if ($key -like "e2e_*") { $targetKey = "${key}_ms" }
                $target = if ($sloDef) { $sloDef.$targetKey } else { $null }
                $actual = $td.$key
                $item["${key}_target"] = if ($target) { "{0}ms" -f ([math]::Round($target / 1000, 1)) } else { "-" }
                $item["${key}_actual"] = if ($actual) { "{0}ms" -f ([math]::Round($actual / 1000, 1)) } else { "-" }
                $item["${key}_pass"] = if ($target -and $actual) { $actual -le $target } else { $null }
            }
            $sloItems += $item
        }
    }

    # Regression (read last Excel summary)
    $regression = $null
    $lastExcel = Get-ChildItem -Path (Join-Path $ProjectRoot "benchmark\results\reports") -Filter "benchmark-report-*.xlsx" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($lastExcel) {
        # Read previous summary JSON if exists
        $lastJson = [System.IO.Path]::ChangeExtension($lastExcel.FullName, ".json")
        if (Test-Path $lastJson) {
            $lastData = Get-Content $lastJson -Raw | ConvertFrom-Json
            $lastHttpPeak = $lastData.summary.http_peak_qps
            $lastGrpcPeak = $lastData.summary.grpc_peak_qps
            if ($lastHttpPeak -and $httpPeak) {
                $regression = @{
                    http_qps_delta = ($httpPeak - $lastHttpPeak) / $lastHttpPeak
                    grpc_qps_delta = if ($lastGrpcPeak -and $grpcPeak) { ($grpcPeak - $lastGrpcPeak) / $lastGrpcPeak } else { $null }
                }
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
        Get-Content $LatestMd.FullName | ForEach-Object { Write-Host $_ }
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

    # Save JSON for regression comparison
    $ReportDir = Join-Path $ProjectRoot "benchmark\results\reports"
    New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null
    $JsonPath = Join-Path $ReportDir "benchmark-report-$($ReportData.timestamp).json"
    $ReportData | ConvertTo-Json -Depth 10 | Set-Content -Path $JsonPath -Encoding UTF8

    # Generate Excel
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
