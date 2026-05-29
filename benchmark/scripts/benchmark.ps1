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
    [string]$Workload = "uniform"
)

# ============================================================
#  Load modules
# ============================================================

. $PSScriptRoot\benchmark-ui.ps1
. $PSScriptRoot\benchmark-util.ps1
. $PSScriptRoot\benchmark-server.ps1
. $PSScriptRoot\benchmark-output.ps1

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
        [switch]$VerboseOutput
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
  -InternalOnly    Run only internal tests (no server required)
  -Quick           Quick test mode
  -Compare         Show history comparison only
  -NoCompile       Skip compilation
  -KeepOpen        Keep console window open after finish
  -NoPause         Never pause on exit (for scripted runs)
  -VerboseOutput   Show full scenario logs
  -Workload <name> Workload distribution: uniform, prod-typical, burst-ultra, mixed-heavy (default: uniform)
  -Help            Show this help

Note: All results are always persisted (CSV + JSON + TXT). Old reports are
      automatically rotated (10 most recent kept).

Examples:
  .\benchmark.ps1                  # Run full tests
  .\benchmark.ps1 -InternalOnly    # Internal tests only
  .\benchmark.ps1 -Compare         # View history
  .\benchmark.ps1 -KeepOpen        # Keep window open
  .\benchmark.ps1 -VerboseOutput   # Full scenario logs
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

        $TempServerHandle = Start-BenchmarkServer `
            -ProjectRoot $ProjectRoot `
            -JavaHome $JavaHome `
            -ServerJarPath $ServerJarPath `
            -Port $TempServerPort `
            -GrpcPort $TempGrpcPort `
            -DataDir $TempDataDir `
            -NodeId $TempNodeId

        try {
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

            Write-Info "Cooling down (15s) between HTTP and gRPC scenarios..."
            Start-Sleep -Seconds 15

            $GrpcResult = Invoke-BenchmarkScenario `
                -Name "2b) gRPC create path" `
                -ServerModuleDir $ServerModuleDir `
                -MainClass "com.loomq.benchmark.GrpcVirtualThreadBenchmark" `
                -BenchmarkGrpcHost "127.0.0.1" `
                -BenchmarkGrpcPort $TempGrpcPort `
                -TimeoutSec (Get-ScenarioTimeout "grpc" $IsQuick) `
                -Quick:$Quick `
                -VerboseOutput:$script:VerboseScenarioOutput

            if ($GrpcResult.ExitCode -ne 0) {
                throw "gRPC benchmark failed with exit code $($GrpcResult.ExitCode)"
            }

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
