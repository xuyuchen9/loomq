<#
.SYNOPSIS
    LoomQ Performance Benchmark Test Script

.DESCRIPTION
    Run atomic performance tests for LoomQ delayed intent queue.

.PARAMETER InternalOnly
    Run only internal component tests (no server required)

.PARAMETER Quick
    Quick test mode (reduced test duration)

.PARAMETER Compare
    Show history comparison only

.PARAMETER NoCompile
    Skip compilation step

.PARAMETER Help
    Show help message

.PARAMETER KeepOpen
    Keep the window open after script exits

.PARAMETER NoPause
    Never pause window on exit (even when double-click launched)

.PARAMETER VerboseOutput
    Show full scenario logs instead of condensed output

.PARAMETER Workload
    Workload distribution profile: uniform, prod-typical, burst-ultra, mixed-heavy (default: uniform)
#>

param(
    [switch]$InternalOnly,
    [switch]$Quick,
    [switch]$Compare,
    [switch]$NoCompile,
    [switch]$Help,
    [switch]$KeepOpen,
    [switch]$NoPause,
    [switch]$VerboseOutput,
    [string]$Workload = "uniform",
    [switch]$Stress,
    [string]$StressOnly = "",
    [string]$StressThreads = "500,1000,2000,3000",
    [int]$StressDuration = 30,
    [int]$StressCooldown = 5000,
    [string]$GrpcTier = "",
    [int]$GrpcWorkerThreads = 0,
    [switch]$Profile
)

# ============================================================
#  Load modules
# ============================================================

. $PSScriptRoot\lib\ui.ps1
. $PSScriptRoot\lib\util.ps1
. $PSScriptRoot\lib\server.ps1
. $PSScriptRoot\lib\output.ps1

# ============================================================
#  Top-level error handler
# ============================================================

trap {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "  FATAL ERROR" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    Write-Host ""
    Write-Host $_.Exception.Message -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Stack Trace:" -ForegroundColor Gray
    Write-Host $_.ScriptStackTrace -ForegroundColor Gray
    Write-Host ""
    Exit-BenchmarkScript -ExitCode 1
}

# ============================================================
#  Configuration
# ============================================================

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Scenario timeout configuration (seconds): Quick / Full
$ScenarioTimeouts = @{
    "internal"  = @{ Quick = 120; Full = 240 }
    "wal"       = @{ Quick = 60;  Full = 120 }
    "storage"   = @{ Quick = 60;  Full = 120 }
    "observer"  = @{ Quick = 120; Full = 180 }
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
    if ($Quick) {
        $MvnArgs += "-Dloomq.benchmark.quick=true"
    }
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
        if (-not [string]::IsNullOrWhiteSpace($prop)) {
            $MvnArgs += "-D$prop"
        }
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
        $Process = Start-Process -FilePath "mvn" -ArgumentList $MvnArgs -WorkingDirectory $ServerModuleDir -PassThru -WindowStyle Hidden -RedirectStandardOutput $StdOutLog -RedirectStandardError $StdErrLog

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
    } catch {
        throw
    }

    if ($null -ne $Process -and -not $TimedOut) {
        try { $Process.WaitForExit() } catch {}
    }

    $StdOutLines = if (Test-Path $StdOutLog) { Get-Content $StdOutLog -Encoding UTF8 } else { @() }
    $StdErrLines = if (Test-Path $StdErrLog) { Get-Content $StdErrLog -Encoding UTF8 } else { @() }
    $Lines = New-Object System.Collections.Generic.List[string]
    foreach ($Line in $StdOutLines) { [void]$Lines.Add($Line) }
    foreach ($Line in $StdErrLines) { [void]$Lines.Add("[stderr] $Line") }

    foreach ($Line in $Lines) {
        if ($VerboseOutput) {
            if ($Line -match '^\[stderr\]\s*\[ERROR\]|^\[ERROR\]|Exception|FATAL') {
                Write-Host $Line -ForegroundColor Red
            } elseif ($Line -match '^\[stderr\]\s*WARNING:|^\[WARN\]') {
                Write-Host $Line -ForegroundColor Yellow
            } elseif ($Line -match '^RESULT\|') {
                Write-Host $Line -ForegroundColor Green
            } elseif ($Line -match '^RESULT_ROW\|') {
                Write-Host $Line -ForegroundColor DarkGray
            } else {
                Write-Host $Line
            }
        } else {
            if ($Line -match '^RESULT\|') {
                Write-Host $Line -ForegroundColor Green
            } elseif ($Line -match '^RESULT_ROW\|') {
                Write-Host $Line -ForegroundColor DarkGray
            } elseif ($Line -match '^╔|^╚|^║|^===|^---|^###|^测试配置|^峰值 QPS|^最佳 P99|^最差 P99|^失败率|^解释:') {
                Write-Host $Line -ForegroundColor Cyan
            } elseif ($Line -match '^\[stderr\] WARNING:|^\[stderr\] \[ERROR\]|^\[stderr\] \[INFO\]|^\[ERROR\]|^\[WARN\]|^\[INFO\]') {
                Write-Host $Line
            }
        }
    }

    $ExitCode = -1
    if ($TimedOut) {
        $ExitCode = -1
    } elseif ($null -ne $Process) {
        try { $Process.Refresh() } catch {}
        if ($null -ne $Process.ExitCode) {
            $ExitCode = [int]$Process.ExitCode
        } else {
            $HasErrorLine = ($Lines | Where-Object { $_ -match '^\[ERROR\]|^\[stderr\]\s+\[ERROR\]' } | Select-Object -First 1)
            $ExitCode = if ($null -ne $HasErrorLine) { 1 } else { 0 }
        }
    }
    if ($TimedOut) {
        $TailOut = $StdOutLines | Select-Object -Last 40
        $TailErr = $StdErrLines | Select-Object -Last 40
        throw "Scenario '$Name' timed out after ${TimeoutSec}s.`nstdout: $StdOutLog`nstderr: $StdErrLog`n--- stdout tail ---`n$($TailOut -join [Environment]::NewLine)`n--- stderr tail ---`n$($TailErr -join [Environment]::NewLine)"
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
#  Main program
# ============================================================

$script:PauseOnExit = (-not $NoPause) -and ($KeepOpen -or (Test-LaunchedFromExplorer))
$script:VerboseScenarioOutput = $VerboseOutput -or $script:PauseOnExit

# Show help
if ($Help) {
    $v = Get-GitVersion
    Write-Host @"
LoomQ Performance Benchmark $v

Usage: .\benchmark.ps1 [Options]

Options:
  -InternalOnly       Run only internal tests (no server required)
  -Quick              Quick test mode
  -Compare            Show history comparison only
  -NoCompile          Skip compilation
  -KeepOpen           Keep console window open after finish
  -NoPause            Never pause on exit (for scripted runs)
  -VerboseOutput      Show full scenario logs
  -Workload <name>    Workload distribution: uniform, prod-typical, burst-ultra, mixed-heavy (default: uniform)
  -Stress             Enable stress test mode (multi-tier pressure sweep per scenario)
  -StressOnly <name>  Only run stress test for specified scenario: http, grpc, scheduler
  -StressThreads <n>  Comma-separated thread tiers for stress mode (default: 500,1000,2000,3000)
  -StressDuration <n> Seconds per stress tier (default: 30)
  -StressCooldown <n> Milliseconds between stress tiers (default: 5000)
  -GrpcTier <tier>    gRPC precision tier: ULTRA, FAST, HIGH, STANDARD, ECONOMY (default: STANDARD)
  -Help               Show this help

Note: All results are always persisted (CSV + JSON + TXT). Old reports are
      automatically rotated (10 most recent kept).

Examples:
  .\benchmark.ps1                         # Run full tests
  .\benchmark.ps1 -Stress                 # Full tests + stress sweep on HTTP/gRPC
  .\benchmark.ps1 -Stress -StressOnly grpc # Only gRPC stress test
  .\benchmark.ps1 -InternalOnly           # Internal tests only
  .\benchmark.ps1 -Compare                # View history
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
    Write-Error "Cannot find pom.xml"
    Write-Host ""
    Write-Host "Project root not found: $ProjectRoot" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Please run this script from the project root directory:" -ForegroundColor Yellow
    Write-Host "  .\benchmark\scripts\benchmark.ps1" -ForegroundColor Cyan
    Wait-Exit
}

Set-Location $ProjectRoot
Write-Info "Project root: $ProjectRoot"
$BenchmarkMavenRepo = Join-Path $ProjectRoot "benchmark\results\m2repo"
New-Item -ItemType Directory -Force -Path $BenchmarkMavenRepo | Out-Null

# Check Java
Write-Header "Environment Check"
$JavaHome = Test-Java
 $PreviousErrorActionPreference = $ErrorActionPreference
 $ErrorActionPreference = "Continue"
 try {
     $JavaVersion = & "$JavaHome\bin\java.exe" -version 2>&1 | Select-Object -First 1
 } finally {
     $ErrorActionPreference = $PreviousErrorActionPreference
 }
Write-Info "Using Java: $JavaHome"
Write-Host "   Version: $JavaVersion" -ForegroundColor Gray

# Check Maven
$MavenCmd = Get-Command mvn -ErrorAction SilentlyContinue
if (-not $MavenCmd) {
    Write-Error "Maven not found!"
    Write-Host ""
    Write-Host "Please install Maven and add to PATH" -ForegroundColor Yellow
    Write-Info "Download: https://maven.apache.org/download.cgi"
    Wait-Exit
}

# Git info
$GitCommit = Get-GitCommit
$GitVersion = Get-GitVersion
Write-Host "   Git: $GitCommit ($GitVersion)" -ForegroundColor Gray

# Show history comparison
if ($Compare) {
    Write-Header "Latest Summary"
    $LatestSummary = Get-LatestSummaryFile
    if ($LatestSummary) {
        Write-Info "Summary: $($LatestSummary.FullName)"
        Get-Content $LatestSummary.FullName | ForEach-Object { Write-Host $_ }
    } else {
        Write-Warning "No benchmark summary found yet."
    }

    # Regression comparison
    Write-Host ""
    Write-Header "Regression Comparison"
    $Regression = Build-RegressionComparison -ProjectRoot $ProjectRoot
    foreach ($Line in $Regression) {
        Write-Host $Line
    }

    Exit-BenchmarkScript -ExitCode 0
}

# Compile project
if (-not $NoCompile) {
    Write-Header "Compiling Project"
    $CompileStart = Get-Date

    Write-Host "Running: mvn compile test-compile -Dmaven.repo.local=<benchmark repo> -q" -ForegroundColor Gray

    $PreviousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $CompileOutput = & mvn compile test-compile "-Dmaven.repo.local=$BenchmarkMavenRepo" -q 2>&1
    } finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    $CompileResult = $LASTEXITCODE

    if ($CompileResult -ne 0) {
        Write-Error "Compilation failed!"
        Write-Host ""
        Write-Host "Error output:" -ForegroundColor Yellow
        $CompileOutput | Select-Object -Last 30 | ForEach-Object { Write-Host "  $_" }
        Write-Host ""
        Write-Info "Try running manually: mvn compile test-compile"
        Wait-Exit
    }

    $CompileTime = [math]::Round(((Get-Date) - $CompileStart).TotalSeconds, 1)
    Write-Success "Compilation completed ($CompileTime seconds)"
}

Ensure-BenchmarkRepo `
    -ProjectRoot $ProjectRoot `
    -RepoPath $BenchmarkMavenRepo `
    -ForceRefresh:(-not $NoCompile)

Write-Header "Running Benchmark Scenarios"
Stop-OrphanBenchmarkServers -ProjectRoot $ProjectRoot

$ServerModuleDir = Join-Path $ProjectRoot "loomq-server"
$TempServerHandle = $null
$TempDataDir = $null
$InternalResult = $null
$HttpResult = $null
$GrpcResult = $null
$SchedulerResult = $null

try {
    $IsQuick = [bool]$Quick

    $InternalResult = Invoke-BenchmarkScenario `
        -Name "1) In-process upper bound" `
        -ServerModuleDir $ServerModuleDir `
        -MainClass "com.loomq.benchmark.InternalBenchmark" `
        -TimeoutSec (Get-ScenarioTimeout "internal" $IsQuick) `
        -Quick:$Quick `
        -VerboseOutput:$script:VerboseScenarioOutput

    # ── Stress mode banner ──
    if ($Stress) {
        Write-Host ""
        Write-Host "+============================================================+" -ForegroundColor Yellow
        Write-Host "|  STRESS MODE enabled                                     |" -ForegroundColor Yellow
        Write-Host "|  Threads: $StressThreads  Duration: ${StressDuration}s  Cooldown: ${StressCooldown}ms     |" -ForegroundColor Yellow
        if (-not [string]::IsNullOrWhiteSpace($StressOnly)) {
            Write-Host "|  Only testing: $StressOnly                                        |" -ForegroundColor Yellow
        }
        Write-Host "+============================================================+" -ForegroundColor Yellow
        Write-Host ""
    }

    if ($InternalResult.ExitCode -ne 0) {
        throw "Internal benchmark failed with exit code $($InternalResult.ExitCode)"
    }

    $WalResult = Invoke-BenchmarkScenario `
        -Name "1b) WAL Write Throughput" `
        -ServerModuleDir $ServerModuleDir `
        -MainClass "com.loomq.benchmark.WalThroughputBenchmark" `
        -TimeoutSec (Get-ScenarioTimeout "wal" $IsQuick) `
        -Quick:$Quick `
        -VerboseOutput:$script:VerboseScenarioOutput

    $StorageResult = Invoke-BenchmarkScenario `
        -Name "1c) Storage Engine Comparison" `
        -ServerModuleDir $ServerModuleDir `
        -MainClass "com.loomq.benchmark.StorageBenchmark" `
        -TimeoutSec (Get-ScenarioTimeout "storage" $IsQuick) `
        -Quick:$Quick `
        -VerboseOutput:$script:VerboseScenarioOutput

    $ObserverResult = Invoke-BenchmarkScenario `
        -Name "1d) Observer Overhead" `
        -ServerModuleDir $ServerModuleDir `
        -MainClass "com.loomq.benchmark.ObserverOverheadBenchmark" `
        -TimeoutSec (Get-ScenarioTimeout "observer" $IsQuick) `
        -Quick:$Quick `
        -VerboseOutput:$script:VerboseScenarioOutput

    if (-not $InternalOnly) {
        # Cooldown between internal and server-based benchmarks to mitigate thermal throttling
        Write-Info "Cooling down (30s) to stabilize CPU thermals..."
        Start-Sleep -Seconds 30

        $ServerJarPath = Ensure-ServerJar -ProjectRoot $ProjectRoot -ServerModuleDir $ServerModuleDir -AllowBuild:(-not $NoCompile)
        $TempServerPort = Get-FreePort
        $TempGrpcPort = Get-FreePort
        $TempDataDir = Join-Path $ProjectRoot ("benchmark\results\runtime\data-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
        New-Item -ItemType Directory -Force -Path $TempDataDir | Out-Null
        $TempNodeId = "bench-" + (Get-Date -Format "yyyyMMddHHmmss")

        Write-Info "Starting isolated server on http://127.0.0.1:$TempServerPort (gRPC: $TempGrpcPort)"
        Write-Info "Data dir: $TempDataDir"
        Write-Info "Node id:  $TempNodeId"

        $ServerExtraArgs = @()
        if ($GrpcWorkerThreads -gt 0) {
            $ServerExtraArgs += "-Dgrpc.worker_threads=$GrpcWorkerThreads"
        }

        $JfrFile = $null
        if ($Profile) {
            $ProfileDir = Join-Path $ProjectRoot "benchmark\results\reports"
            New-Item -ItemType Directory -Force -Path $ProfileDir | Out-Null
            $ProfileStamp = Get-Date -Format "yyyyMMdd-HHmmss"
            $JfrFile = Join-Path $ProfileDir "profile-$ProfileStamp.jfr"
            $ServerExtraArgs += "-XX:StartFlightRecording=filename=$JfrFile,duration=600s,settings=profile"
            Write-Info "JFR profiling enabled: $JfrFile"
        }

        $TempServerHandle = Start-BenchmarkServer `
            -ProjectRoot $ProjectRoot `
            -JavaHome $JavaHome `
            -ServerJarPath $ServerJarPath `
            -Port $TempServerPort `
            -GrpcPort $TempGrpcPort `
            -DataDir $TempDataDir `
            -NodeId $TempNodeId `
            -ExtraJvmArgs $ServerExtraArgs

        try {
            $RunHttp = [string]::IsNullOrWhiteSpace($StressOnly) -or $StressOnly -eq "http"
            $RunGrpc = [string]::IsNullOrWhiteSpace($StressOnly) -or $StressOnly -eq "grpc"
            $RunScheduler = [string]::IsNullOrWhiteSpace($StressOnly) -or $StressOnly -eq "scheduler"

            if ($RunHttp) {
                $HttpResult = Invoke-BenchmarkScenario `
                    -Name "2) HTTP create path" `
                    -ServerModuleDir $ServerModuleDir `
                    -MainClass "com.loomq.benchmark.HttpVirtualThreadBenchmark" `
                    -BenchmarkBaseUrl $TempServerHandle.BaseUrl `
                    -TimeoutSec (Get-ScenarioTimeout "http" $IsQuick) `
                    -Quick:$Quick `
                    -VerboseOutput:$script:VerboseScenarioOutput

                if ($HttpResult.ExitCode -ne 0) {
                    throw "HTTP benchmark failed with exit code $($HttpResult.ExitCode)"
                }
            }

            if ($RunGrpc) {
                Write-Info "Cooling down (15s) between HTTP and gRPC scenarios..."
                Start-Sleep -Seconds 15

                $GrpcExtraProps = @()
                if (-not [string]::IsNullOrWhiteSpace($GrpcTier)) {
                    $GrpcExtraProps += "loomq.benchmark.grpc.tier=$GrpcTier"
                }

                $GrpcResult = Invoke-BenchmarkScenario `
                    -Name "2b) gRPC create path" `
                    -ServerModuleDir $ServerModuleDir `
                    -MainClass "com.loomq.benchmark.GrpcVirtualThreadBenchmark" `
                    -BenchmarkGrpcHost "127.0.0.1" `
                    -BenchmarkGrpcPort $TempGrpcPort `
                    -TimeoutSec (Get-ScenarioTimeout "grpc" $IsQuick) `
                    -Quick:$Quick `
                    -VerboseOutput:$script:VerboseScenarioOutput `
                    -ExtraJvmProperties $GrpcExtraProps

                if ($GrpcResult.ExitCode -ne 0) {
                    throw "gRPC benchmark failed with exit code $($GrpcResult.ExitCode)"
                }
            }

            if ($RunScheduler) {
                Write-Info "Cooling down (15s) between gRPC and scheduler scenarios..."
                Start-Sleep -Seconds 15

                $SchedulerResult = Invoke-BenchmarkScenario `
                    -Name "3) Scheduler trigger path" `
                    -ServerModuleDir $ServerModuleDir `
                    -MainClass "com.loomq.scheduler.SchedulerTriggerBenchmarkWithMockServer" `
                    -ExecArgs "-Dloomq.benchmark.workload=$Workload" `
                    -TimeoutSec (Get-ScenarioTimeout "scheduler" $IsQuick) `
                    -Quick:$Quick `
                    -VerboseOutput:$script:VerboseScenarioOutput

                if ($SchedulerResult.ExitCode -ne 0) {
                    throw "Scheduler benchmark failed with exit code $($SchedulerResult.ExitCode)"
                }

                $SchedulerCompletionRate = Get-ResultDouble $SchedulerResult.Data "completion_rate" -Default -1
                if ($SchedulerCompletionRate -lt 1) {
                    throw "Scheduler benchmark completed but produced invalid completion rate ($SchedulerCompletionRate%). Check callback delivery path."
                }
            }

            # ── Stress test phase ──
            $HttpStressResult = $null
            $GrpcStressResult = $null
            if ($Stress) {
                $StressExtraProps = @(
                    "loomq.benchmark.stress=true"
                    "loomq.benchmark.threads=$StressThreads"
                    "loomq.benchmark.duration_sec=$StressDuration"
                    "loomq.benchmark.cooldown_ms=$StressCooldown"
                )
                if (-not [string]::IsNullOrWhiteSpace($GrpcTier)) {
                    $StressExtraProps += "loomq.benchmark.grpc.tier=$GrpcTier"
                }
                $StressTimeout = ($StressThreads.Split(",").Count + 1) * ($StressDuration + 15) + 120
                $RunHttpStress = [string]::IsNullOrWhiteSpace($StressOnly) -or $StressOnly -eq "http"
                $RunGrpcStress = [string]::IsNullOrWhiteSpace($StressOnly) -or $StressOnly -eq "grpc"

                Write-Info "Cooling down (30s) before stress phase..."
                Start-Sleep -Seconds 30

                if ($RunHttpStress) {
                    $HttpStressResult = Invoke-BenchmarkScenario `
                        -Name "2c) HTTP stress test" `
                        -ServerModuleDir $ServerModuleDir `
                        -MainClass "com.loomq.benchmark.HttpVirtualThreadBenchmark" `
                        -BenchmarkBaseUrl $TempServerHandle.BaseUrl `
                        -TimeoutSec $StressTimeout `
                        -VerboseOutput:$script:VerboseScenarioOutput `
                        -ExtraJvmProperties $StressExtraProps

                    Write-Info "Cooling down (15s) between HTTP and gRPC stress..."
                    Start-Sleep -Seconds 15
                }

                if ($RunGrpcStress) {
                    $GrpcStressResult = Invoke-BenchmarkScenario `
                        -Name "2d) gRPC stress test" `
                        -ServerModuleDir $ServerModuleDir `
                        -MainClass "com.loomq.benchmark.GrpcVirtualThreadBenchmark" `
                        -BenchmarkGrpcHost "127.0.0.1" `
                        -BenchmarkGrpcPort $TempGrpcPort `
                        -TimeoutSec $StressTimeout `
                        -VerboseOutput:$script:VerboseScenarioOutput `
                        -ExtraJvmProperties $StressExtraProps
                }
            }
        } finally {
            Stop-BenchmarkServer -ServerHandle $TempServerHandle
        }
    }

    # Parse all results into unified data model
    $ParsedInternal = ConvertTo-ParsedBenchmark $InternalResult
    $ParsedHttp = ConvertTo-ParsedBenchmark $HttpResult
    $ParsedGrpc = ConvertTo-ParsedBenchmark $GrpcResult
    $ParsedScheduler = ConvertTo-ParsedBenchmark $SchedulerResult

    $SummaryLines = Build-BenchmarkSummary `
        -ProjectRoot $ProjectRoot `
        -GitCommit $GitCommit `
        -GitVersion $GitVersion `
        -JavaHome $JavaHome `
        -Quick $IsQuick `
        -InternalResult $InternalResult `
        -HttpResult $HttpResult `
        -GrpcResult $GrpcResult `
        -SchedulerResult $SchedulerResult `
        -ParsedScheduler $ParsedScheduler

    Write-Header "Summary"
    $SummaryLines | ForEach-Object { Write-Host $_ }

    # ── Capacity Analysis (stress mode) ──
    if ($Stress -and ($HttpStressResult -or $GrpcStressResult)) {
        Write-Host ""
        Write-Header "Capacity Analysis"

        function Get-StressInflection {
            param($Result)
            if (-not $Result -or -not $Result.Data) { return $null }
            $InfThreads = Get-ResultInt $Result.Data "inflection_threads"
            $InfQps = Get-ResultDouble $Result.Data "inflection_qps"
            if ($InfThreads -gt 0) {
                return [pscustomobject]@{ Threads = $InfThreads; Qps = $InfQps }
            }
            return $null
        }

        function Get-ScenarioStressSummary {
            param($Result, [string]$Label)
            if (-not $Result) { return }
            $PeakQps = Get-ResultDouble $Result.Data "peak_qps"
            $BestP99 = Get-ResultDouble $Result.Data "best_p99_ms"
            $WorstP99 = Get-ResultDouble $Result.Data "worst_p99_ms"
            $Inf = Get-StressInflection $Result
            $InfStr = if ($Inf) { "$($Inf.Threads) threads ($([string]::Format('{0:N0}', $Inf.Qps)) QPS)" } else { "not found" }
            $Status = if ($WorstP99 -gt 500) { "❌ avalanche" } elseif ($WorstP99 -gt 100) { "⚠️  near SLO" } else { "✅ OK" }
            Write-Host ("   {0,-16} peak={1,8:N0}  best_p99={2,5}ms  worst_p99={3,5}ms  inflection={4,-30} {5}" -f `
                $Label, $PeakQps, $BestP99, $WorstP99, $InfStr, $Status)
        }

        Write-Host ""
        Write-Host ("   {0,-16} {1,-14} {2,-15} {3,-16} {4,-32} {5}" -f "Scenario", "Peak QPS", "Best P99", "Worst P99", "Inflection", "Status") -ForegroundColor Cyan
        Write-Host ("   " + ("-" * 100))
        Get-ScenarioStressSummary $HttpStressResult "HTTP create"
        Get-ScenarioStressSummary $GrpcStressResult "gRPC create"

        # Bottleneck analysis
        $HttpPeak = if ($HttpStressResult) { Get-ResultDouble $HttpStressResult.Data "peak_qps" } else { 0 }
        $GrpcPeak = if ($GrpcStressResult) { Get-ResultDouble $GrpcStressResult.Data "peak_qps" } else { 0 }
        $InternalQps = if ($InternalResult) { Get-ResultDouble $InternalResult.Data "direct_store_qps" } else { 0 }

        Write-Host ""
        if ($InternalQps -gt 0 -and $GrpcPeak -gt 0) {
            $Gap = [math]::Round($InternalQps / $GrpcPeak, 1)
            Write-Host "   Bottleneck: gRPC/HTTP transport layer (${Gap}x below IntentStore ceiling)" -ForegroundColor Yellow
        }

        # Save stress summary
        $StressSummaryLines = New-Object System.Collections.Generic.List[string]
        $StressSummaryLines.Add("")
        $StressSummaryLines.Add("=== Capacity Analysis ===")
        $StressSummaryLines.Add("   Stress threads: $StressThreads")
        $StressSummaryLines.Add("   Stress duration: ${StressDuration}s per tier")
        $StressSummaryLines.Add("")
        if ($HttpStressResult) {
            $PeakH = Get-ResultDouble $HttpStressResult.Data "peak_qps"
            $ThreadsH = Get-ResultInt $HttpStressResult.Data "best_threads"
            $InfH = Get-StressInflection $HttpStressResult
            $StressSummaryLines.Add("   HTTP: peak $([string]::Format('{0:N0}', $PeakH)) QPS @ $ThreadsH threads, inflection: $(if ($InfH) { "$($InfH.Threads) threads" } else { 'not found' })")
        }
        if ($GrpcStressResult) {
            $PeakG = Get-ResultDouble $GrpcStressResult.Data "peak_qps"
            $ThreadsG = Get-ResultInt $GrpcStressResult.Data "best_threads"
            $InfG = Get-StressInflection $GrpcStressResult
            $StressSummaryLines.Add("   gRPC: peak $([string]::Format('{0:N0}', $PeakG)) QPS @ $ThreadsG threads, inflection: $(if ($InfG) { "$($InfG.Threads) threads" } else { 'not found' })")
        }
        if ($InternalQps -gt 0 -and $GrpcPeak -gt 0) {
            $StressSummaryLines.Add("   Bottleneck: transport layer ($([math]::Round($InternalQps / $GrpcPeak, 1))x below IntentStore ceiling)")
        }
        $StressSummaryLines.Add("")

        # Append to existing summary file
        $SummaryLines = @($SummaryLines) + @($StressSummaryLines)
    }

    Remove-OldReports

    $SummaryFile = Save-BenchmarkSummary -Lines $SummaryLines
    Write-Host ""
    Write-Success "Summary saved"
    Write-Info "Report: $SummaryFile"

    Write-BenchmarkHistoryCsv `
        -InternalResult $InternalResult `
        -HttpResult $HttpResult `
        -GrpcResult $GrpcResult `
        -SchedulerResult $SchedulerResult `
        -ParsedScheduler $ParsedScheduler

    Write-BenchmarkJson `
        -InternalResult $InternalResult `
        -HttpResult $HttpResult `
        -GrpcResult $GrpcResult `
        -SchedulerResult $SchedulerResult

    # Save stress test results separately
    if ($Stress -and ($HttpStressResult -or $GrpcStressResult)) {
        $StressJsonDir = Join-Path $ProjectRoot "benchmark\results\reports"
        $StressStamp = Get-Date -Format "yyyyMMdd-HHmmss"
        $StressJsonFile = Join-Path $StressJsonDir "stress-$StressStamp.json"
        $StressJsonBranch = try { (git rev-parse --abbrev-ref HEAD 2>$null).Trim() } catch { "unknown" }

        $StressJsonOutput = [ordered]@{
            timestamp = (Get-Date -Format "yyyy-MM-ddTHH:mm:ss")
            commit = $GitCommit
            branch = $StressJsonBranch
            config = [ordered]@{
                stress_threads = $StressThreads
                stress_duration_sec = $StressDuration
                stress_cooldown_ms = $StressCooldown
                cores = [System.Environment]::ProcessorCount
            }
            scenarios = @{}
        }
        if ($HttpStressResult) {
            $StressJsonOutput.scenarios["http_stress"] = [ordered]@{
                peak_qps = Get-ResultDouble $HttpStressResult.Data "peak_qps"
                best_p99_ms = Get-ResultDouble $HttpStressResult.Data "best_p99_ms"
                worst_p99_ms = Get-ResultDouble $HttpStressResult.Data "worst_p99_ms"
                inflection_threads = Get-ResultInt $HttpStressResult.Data "inflection_threads"
                inflection_qps = Get-ResultDouble $HttpStressResult.Data "inflection_qps"
            }
        }
        if ($GrpcStressResult) {
            $StressJsonOutput.scenarios["grpc_stress"] = [ordered]@{
                peak_qps = Get-ResultDouble $GrpcStressResult.Data "peak_qps"
                best_p99_ms = Get-ResultDouble $GrpcStressResult.Data "best_p99_ms"
                worst_p99_ms = Get-ResultDouble $GrpcStressResult.Data "worst_p99_ms"
                inflection_threads = Get-ResultInt $GrpcStressResult.Data "inflection_threads"
                inflection_qps = Get-ResultDouble $GrpcStressResult.Data "inflection_qps"
            }
        }
        $StressJsonOutput | ConvertTo-Json -Depth 4 | Out-File -FilePath $StressJsonFile -Encoding UTF8
        Write-Info "Stress JSON: $StressJsonFile"

        # Append to stress CSV history
        $StressHistoryFile = Join-Path $ProjectRoot "benchmark\results\stress-history.csv"
        $StressHistoryHeader = "timestamp,commit,branch,scenario,peak_qps,best_p99_ms,worst_p99_ms,inflection_threads,inflection_qps"
        $NeedsStressHeader = -not (Test-Path $StressHistoryFile)
        if (-not $NeedsStressHeader) {
            $FirstLine = Get-Content $StressHistoryFile -TotalCount 1 -ErrorAction SilentlyContinue
            if ($FirstLine -ne $StressHistoryHeader) { $NeedsStressHeader = $true }
        }
        if ($NeedsStressHeader) {
            [System.IO.File]::WriteAllLines($StressHistoryFile, @($StressHistoryHeader), [System.Text.UTF8Encoding]::new($false))
        }
        $StressTs = Get-Date -Format "yyyy-MM-ddTHH:mm:ss"
        if ($HttpStressResult) {
            $InfH = Get-ResultInt $HttpStressResult.Data "inflection_threads"
            $InfQpsH = Get-ResultDouble $HttpStressResult.Data "inflection_qps"
            $RowH = "{0},{1},{2},http,{3},{4},{5},{6},{7}" -f $StressTs, $GitCommit, $StressJsonBranch,
                (Get-ResultDouble $HttpStressResult.Data "peak_qps"),
                (Get-ResultDouble $HttpStressResult.Data "best_p99_ms"),
                (Get-ResultDouble $HttpStressResult.Data "worst_p99_ms"),
                $(if ($InfH -gt 0) { $InfH } else { "" }),
                $(if ($InfQpsH -gt 0) { [string]::Format('{0:F0}', $InfQpsH) } else { "" })
            [System.IO.File]::AppendAllText($StressHistoryFile, $RowH + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))
        }
        if ($GrpcStressResult) {
            $InfG = Get-ResultInt $GrpcStressResult.Data "inflection_threads"
            $InfQpsG = Get-ResultDouble $GrpcStressResult.Data "inflection_qps"
            $RowG = "{0},{1},{2},grpc,{3},{4},{5},{6},{7}" -f $StressTs, $GitCommit, $StressJsonBranch,
                (Get-ResultDouble $GrpcStressResult.Data "peak_qps"),
                (Get-ResultDouble $GrpcStressResult.Data "best_p99_ms"),
                (Get-ResultDouble $GrpcStressResult.Data "worst_p99_ms"),
                $(if ($InfG -gt 0) { $InfG } else { "" }),
                $(if ($InfQpsG -gt 0) { [string]::Format('{0:F0}', $InfQpsG) } else { "" })
            [System.IO.File]::AppendAllText($StressHistoryFile, $RowG + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))
        }
        Write-Info "Stress history: $StressHistoryFile"
    }

    # ── JFR: stop recording and flush before analysis ──
    if ($Profile -and $JfrFile -and $TempServerHandle) {
        $ServerPid = $TempServerHandle.Process.Id
        Write-Info "Stopping JFR recording (PID: $ServerPid)..."
        $PreviousErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & "$JavaHome\bin\jcmd.exe" $ServerPid JFR.stop 2>&1
        } catch {}
        finally {
            $ErrorActionPreference = $PreviousErrorActionPreference
        }
        Start-Sleep -Seconds 2  # Give JFR time to flush to disk
    }

    # ── JFR Profiling Analysis ──
    if ($Profile -and $JfrFile -and (Test-Path $JfrFile)) {
        Write-Host ""
        Write-Header "CPU Profiling (JFR)"

        $JfrSize = (Get-Item $JfrFile).Length / 1MB
        Write-Info "JFR recording: $JfrFile ($([math]::Round($JfrSize, 1)) MB)"

        $JfrOutFile = $JfrFile -replace '\.jfr$', '-samples.txt'
        $PreviousErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & "$JavaHome\bin\jfr.exe" print --events jdk.ExecutionSample --stack-depth 32 $JfrFile 2>$null | Out-File -FilePath $JfrOutFile -Encoding UTF8
        } finally {
            $ErrorActionPreference = $PreviousErrorActionPreference
        }

        if (Test-Path $JfrOutFile) {
            # Parse execution samples and count method occurrences
            $MethodCounts = @{}
            $TotalSamples = 0
            $CurrentMethod = $null
            foreach ($Line in (Get-Content $JfrOutFile -Encoding UTF8)) {
                if ($Line -match '^\s+Method:') {
                    # jfr print format: "  Method: com.example.Foo.bar()"
                    $MethodMatch = [regex]::Match($Line, 'Method:\s+(.+)')
                    if ($MethodMatch.Success) {
                        $CurrentMethod = $MethodMatch.Groups[1].Value.Trim()
                    }
                } elseif ($Line -match '^\s+at\s+(.+?)\(' -and $null -eq $CurrentMethod) {
                    # Alternative: stack trace format "  at com.example.Foo.bar(Foo.java:123)"
                    $StackMatch = [regex]::Match($Line, 'at\s+(.+?)\(')
                    if ($StackMatch.Success) {
                        $CurrentMethod = $StackMatch.Groups[1].Value.Trim()
                    }
                }
                if ($Line -match '^Event\s' -or $Line -match '^\s*$') {
                    if ($CurrentMethod) {
                        if (-not $MethodCounts.ContainsKey($CurrentMethod)) {
                            $MethodCounts[$CurrentMethod] = 0
                        }
                        $MethodCounts[$CurrentMethod]++
                        $TotalSamples++
                        $CurrentMethod = $null
                    }
                }
            }
            # Count last method if file doesn't end with blank line
            if ($CurrentMethod) {
                if (-not $MethodCounts.ContainsKey($CurrentMethod)) {
                    $MethodCounts[$CurrentMethod] = 0
                }
                $MethodCounts[$CurrentMethod]++
                $TotalSamples++
            }

            if ($TotalSamples -gt 0) {
                Write-Host ""
                Write-Host "  Total samples: $TotalSamples" -ForegroundColor Gray
                Write-Host ""
                Write-Host ("  {0,6}  {1,-70}" -f "Pct%", "Method") -ForegroundColor Cyan
                Write-Host ("  " + ("-" * 80))

                $Sorted = $MethodCounts.GetEnumerator() | Sort-Object Value -Descending | Select-Object -First 25
                foreach ($Entry in $Sorted) {
                    $Pct = [math]::Round($Entry.Value / $TotalSamples * 100, 1)
                    $MethodName = $Entry.Key
                    # Shorten long method names
                    if ($MethodName.Length -gt 68) {
                        $MethodName = $MethodName.Substring(0, 65) + "..."
                    }
                    $Color = if ($Pct -gt 15) { "Red" } elseif ($Pct -gt 5) { "Yellow" } else { "White" }
                    Write-Host ("  {0,5}%  {1,-70}" -f $Pct, $MethodName) -ForegroundColor $Color
                }
                Write-Host ""

                # Save hotspot summary
                $HotspotFile = $JfrFile -replace '\.jfr$', '-hotspot.txt'
                $HotspotLines = @("JFR CPU Hotspot Analysis", "Total samples: $TotalSamples", "Recording: $JfrFile", "")
                $HotspotLines += ("{0,6}  {1}" -f "Pct%", "Method")
                $HotspotLines += ("-" * 80)
                foreach ($Entry in $Sorted) {
                    $Pct = [string]::Format('{0:F1}', $Entry.Value / $TotalSamples * 100)
                    $HotspotLines += ("{0,5}%  {1}" -f $Pct, $Entry.Key)
                }
                $HotspotLines | Out-File -FilePath $HotspotFile -Encoding UTF8
                Write-Info "Hotspot report: $HotspotFile"
            } else {
                Write-Warning "No execution samples found in JFR recording. The recording may be too short."
            }
        } else {
            Write-Warning "Failed to parse JFR recording. Check if jfr.exe is in PATH."
        }
    }

    $Latest = Get-LatestSummaryFile
    if ($Latest) {
        Write-Info "Latest: $($Latest.FullName)"
    }

    Write-Host ""
    Write-Host "Tip: use -Compare to show the latest saved summary." -ForegroundColor Gray
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
