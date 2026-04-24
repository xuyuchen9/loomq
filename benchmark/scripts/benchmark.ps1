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

.PARAMETER Save
    Auto-save results to history

.PARAMETER NoSave
    Preview mode (do not save results)

.PARAMETER Help
    Show help message

.PARAMETER KeepOpen
    Keep the window open after script exits

.PARAMETER NoPause
    Never pause window on exit (even when double-click launched)

.PARAMETER VerboseOutput
    Show full scenario logs instead of condensed output
#>

param(
    [switch]$InternalOnly,
    [switch]$Quick,
    [switch]$Compare,
    [switch]$NoCompile,
    [switch]$Save,
    [switch]$NoSave,
    [switch]$Help,
    [switch]$KeepOpen,
    [switch]$NoPause,
    [switch]$VerboseOutput
)

# ============================================================
#  顶层错误处理 - 防止闪退
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
#  配置
# ============================================================

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# ANSI 转义字符 (PowerShell 5.1 兼容)
$ESC = [char]27

# 颜色定义 - 使用表达式避免解析问题
$C = @{}
$C.Reset   = "$ESC[0m"
$C.Bold    = "$ESC[1m"
$C.Dim     = "$ESC[2m"
$C.Red     = "$ESC[31m"
$C.Green   = "$ESC[32m"
$C.Yellow  = "$ESC[33m"
$C.Blue    = "$ESC[34m"
$C.Magenta = "$ESC[35m"
$C.Cyan    = "$ESC[36m"
$C.White   = "$ESC[37m"
$C.BrightRed    = "$ESC[91m"
$C.BrightGreen  = "$ESC[92m"
$C.BrightYellow = "$ESC[93m"
$C.BrightBlue   = "$ESC[94m"
$C.BrightCyan   = "$ESC[96m"
$C.BgGreen  = "$ESC[42m"
$C.BgRed    = "$ESC[41m"
$C.BgYellow = "$ESC[43m"

# Unicode 符号 (Windows Terminal 支持)
$S = @{}
$S.Check   = [char]0x2713
$S.Cross   = [char]0x2717
$S.Arrow   = [char]0x2192
$S.Up      = [char]0x2191
$S.Down    = [char]0x2193
$S.Bullet  = [char]0x2022
$S.Diamond = [char]0x25C6

# ============================================================
#  UI 函数
# ============================================================

function Write-Header {
    param([string]$Text, [string]$Color = "Cyan")
    $width = 60
    $line = "=" * $width
    Write-Host ""
    Write-Host $line -ForegroundColor $Color
    Write-Host "  $Text" -ForegroundColor $Color
    Write-Host $line -ForegroundColor $Color
    Write-Host ""
}

function Write-Success {
    param([string]$Text)
    Write-Host "[OK] $Text" -ForegroundColor Green
}

function Write-Warning {
    param([string]$Text)
    Write-Host "[WARN] $Text" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Text)
    Write-Host "[ERR] $Text" -ForegroundColor Red
}

function Write-Info {
    param([string]$Text)
    Write-Host "[INFO] $Text" -ForegroundColor Blue
}

function Wait-Exit {
    Write-Host ""
    Exit-BenchmarkScript -ExitCode 1
}

# ============================================================
#  工具函数
# ============================================================

function Test-LaunchedFromExplorer {
    try {
        $self = Get-CimInstance Win32_Process -Filter "ProcessId=$PID" -ErrorAction Stop
        if ($null -eq $self) {
            return $false
        }

        $parentPid = [int]$self.ParentProcessId
        if ($parentPid -le 0) {
            return $false
        }

        $parent = Get-CimInstance Win32_Process -Filter "ProcessId=$parentPid" -ErrorAction Stop
        return $null -ne $parent -and $parent.Name -ieq "explorer.exe"
    } catch {
        return $false
    }
}

$script:PauseOnExit = $false

function Exit-BenchmarkScript {
    param(
        [int]$ExitCode = 0,
        [string]$Prompt = "Press Enter to close"
    )

    if ($script:PauseOnExit) {
        Write-Host ""
        Read-Host $Prompt | Out-Null
    }

    exit $ExitCode
}

function Get-GitCommit {
    try {
        $commit = git rev-parse --short HEAD 2>$null
        return $commit.Trim()
    } catch {
        return "unknown"
    }
}

function Get-GitVersion {
    try {
        $describe = git describe --tags --always 2>$null
        return $describe.Trim()
    } catch {
        return "unknown"
    }
}

function Test-VersionChanged {
    $HistoryFile = "benchmark\results\history.csv"
    if (-not (Test-Path $HistoryFile)) {
        return $true
    }
    $CurrentCommit = Get-GitCommit
    $lines = Get-Content $HistoryFile -Tail 5
    foreach ($line in $lines) {
        if ($line -match ",$CurrentCommit,") {
            return $false
        }
    }
    return $true
}

function Test-Java {
    $JavaHome = $env:JAVA_HOME
    $JavaCandidates = @(
        "C:\Program Files\Java\jdk-25",
        "C:\Program Files\Java\jdk-21",
        "$env:USERProfile\.jdks\jdk-25",
        "$env:USERProfile\.jdks\jdk-21"
    )

    if (-not $JavaHome -or -not (Test-Path "$JavaHome\bin\java.exe")) {
        foreach ($candidate in $JavaCandidates) {
            if (Test-Path "$candidate\bin\java.exe") {
                $JavaHome = $candidate
                break
            }
        }
    }

    if (-not $JavaHome -or -not (Test-Path "$JavaHome\bin\java.exe")) {
        Write-Error "JDK not found!"
        Write-Host ""
        Write-Host "Please set JAVA_HOME or install JDK 25+" -ForegroundColor Yellow
        Write-Info "Download: https://jdk.java.net/25/"
        Wait-Exit
    }

    $env:JAVA_HOME = $JavaHome
    $env:PATH = "$JavaHome\bin;" + $env:PATH

    return $JavaHome
}

function Test-TcpPortOpen {
    param(
        [string]$Address = "127.0.0.1",
        [int]$Port,
        [int]$TimeoutMs = 500
    )

    $Client = [System.Net.Sockets.TcpClient]::new()
    try {
        $Async = $Client.BeginConnect($Address, $Port, $null, $null)
        if (-not $Async.AsyncWaitHandle.WaitOne($TimeoutMs, $false)) {
            return $false
        }

        $Client.EndConnect($Async)
        return $Client.Connected
    } catch {
        return $false
    } finally {
        $Client.Close()
    }
}

function Get-LatestSummaryFile {
    $SummaryDir = Join-Path $ProjectRoot "benchmark\results\reports"
    if (-not (Test-Path $SummaryDir)) {
        return $null
    }

    return Get-ChildItem -Path $SummaryDir -Filter "summary-*.txt" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
}

function ConvertFrom-BenchmarkMarker {
    param([string]$Marker)

    $Result = @{}
    if ([string]::IsNullOrWhiteSpace($Marker)) {
        return $Result
    }

    $FirstPipe = $Marker.IndexOf('|')
    if ($FirstPipe -lt 0 -or $FirstPipe -ge $Marker.Length - 1) {
        return $Result
    }

    $Payload = $Marker.Substring($FirstPipe + 1)
    foreach ($Part in $Payload.Split('|')) {
        $Pair = $Part.Split('=', 2)
        if ($Pair.Count -eq 2 -and -not [string]::IsNullOrWhiteSpace($Pair[0])) {
            $Result[$Pair[0]] = $Pair[1]
        }
    }

    return $Result
}

function Stop-OrphanBenchmarkServers {
    param([string]$ProjectRoot)

    $Killed = 0
    try {
        $Candidates = Get-CimInstance Win32_Process -Filter "Name='java.exe'" -ErrorAction SilentlyContinue |
            Where-Object {
                if (-not $_.CommandLine) { return $false }
                $Cmd = $_.CommandLine
                $IsBenchServer = $Cmd.Contains("-Dloomq.node.id=bench-") -and $Cmd.Contains("-jar") -and $Cmd.Contains("loomq-server")
                $IsBenchmarkWorker = $Cmd.Contains("com.loomq.benchmark.") -or $Cmd.Contains("SchedulerTriggerBenchmarkWithMockServer")
                $TouchesWorkspace = $Cmd.Contains($ProjectRoot)
                return ($IsBenchServer -or $IsBenchmarkWorker) -and $TouchesWorkspace
            }

        foreach ($Proc in $Candidates) {
            try {
                Stop-ProcessTree -RootPid $Proc.ProcessId
                $Killed++
            } catch {
            }
        }
    } catch {
    }

    if ($Killed -gt 0) {
        Write-Warning "Cleaned $Killed orphan benchmark server process(es)."
    }
}

function Stop-ProcessTree {
    param([int]$RootPid)

    if ($RootPid -le 0) {
        return
    }

    $Children = @()
    try {
        $Children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId=$RootPid" -ErrorAction SilentlyContinue)
    } catch {
    }

    foreach ($Child in $Children) {
        Stop-ProcessTree -RootPid $Child.ProcessId
    }

    try {
        Stop-Process -Id $RootPid -Force -ErrorAction SilentlyContinue
    } catch {
    }
}

function Invoke-BenchmarkScenario {
    param(
        [string]$Name,
        [string]$ServerModuleDir,
        [string]$MainClass,
        [string]$ExecArgs = $null,
        [string]$BenchmarkBaseUrl = $null,
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

function Get-FreePort {
    $Listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    try {
        $Listener.Start()
        return ([System.Net.IPEndPoint]$Listener.LocalEndpoint).Port
    } finally {
        $Listener.Stop()
    }
}

function Get-ServerJarPath {
    param([string]$ServerModuleDir)

    $TargetDir = Join-Path $ServerModuleDir "target"
    if (-not (Test-Path $TargetDir)) {
        return $null
    }

    $Jar = Get-ChildItem -Path $TargetDir -Filter "loomq-server-*.jar" -File -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -notlike "original-*" } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1

    if ($null -eq $Jar) {
        return $null
    }

    return $Jar.FullName
}

function Ensure-ServerJar {
    param(
        [string]$ProjectRoot,
        [string]$ServerModuleDir,
        [switch]$AllowBuild
    )

    $JarPath = Get-ServerJarPath -ServerModuleDir $ServerModuleDir
    if ($JarPath) {
        return $JarPath
    }

    if (-not $AllowBuild) {
        throw "Server jar not found. Run without -NoCompile once so the jar can be packaged."
    }

    Write-Header "Packaging Server Jar"
    Write-Info "Building loomq-server shade jar for isolated HTTP benchmark"

    $PreviousLocation = Get-Location
    $PreviousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        Set-Location $ProjectRoot
        $PackageOutput = & mvn -pl loomq-server -am package -DskipTests "-Dmaven.repo.local=$BenchmarkMavenRepo" -q 2>&1
        $PackageResult = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
        Set-Location $PreviousLocation
    }

    if ($PackageResult -ne 0) {
        Write-Host ""
        Write-Host "Packaging output:" -ForegroundColor Yellow
        $PackageOutput | Select-Object -Last 40 | ForEach-Object { Write-Host "  $_" }
        throw "Failed to package loomq-server"
    }

    $JarPath = Get-ServerJarPath -ServerModuleDir $ServerModuleDir
    if (-not $JarPath) {
        throw "Server jar packaging succeeded but no loomq-server-*.jar file was found."
    }

    return $JarPath
}

function Ensure-BenchmarkRepo {
    param(
        [string]$ProjectRoot,
        [string]$RepoPath,
        [switch]$ForceRefresh
    )

    New-Item -ItemType Directory -Force -Path $RepoPath | Out-Null

    $ParentPom = Join-Path $RepoPath "com\loomq\loomq-parent\0.7.0-SNAPSHOT\loomq-parent-0.7.0-SNAPSHOT.pom"
    $BomPom = Join-Path $RepoPath "com\loomq\loomq-bom\0.7.0-SNAPSHOT\loomq-bom-0.7.0-SNAPSHOT.pom"
    $CoreJar = Join-Path $RepoPath "com\loomq\loomq-core\0.7.0-SNAPSHOT\loomq-core-0.7.0-SNAPSHOT.jar"
    $ServerJar = Join-Path $RepoPath "com\loomq\loomq-server\0.7.0-SNAPSHOT\loomq-server-0.7.0-SNAPSHOT.jar"

    $RepoReady = (Test-Path $ParentPom) -and (Test-Path $BomPom) -and (Test-Path $CoreJar) -and (Test-Path $ServerJar)
    if (-not $ForceRefresh -and $RepoReady) {
        return
    }

    Write-Header "Preparing Maven Repo"
    Write-Info "Syncing reactor artifacts into isolated repo: $RepoPath"

    $PreviousLocation = Get-Location
    $PreviousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        Set-Location $ProjectRoot
        $InstallOutput = & mvn -DskipTests "-Dmaven.repo.local=$RepoPath" install -q 2>&1
        $InstallResult = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
        Set-Location $PreviousLocation
    }

    if ($InstallResult -ne 0) {
        Write-Host ""
        Write-Host "Install output:" -ForegroundColor Yellow
        $InstallOutput | Select-Object -Last 40 | ForEach-Object { Write-Host "  $_" }
        throw "Failed to prepare isolated Maven repo"
    }
}

function Start-BenchmarkServer {
    param(
        [string]$ProjectRoot,
        [string]$JavaHome,
        [string]$ServerJarPath,
        [int]$Port,
        [string]$DataDir,
        [string]$NodeId
    )

    $LogDir = Join-Path $ProjectRoot "benchmark\results\logs"
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    $Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $StdOutLog = Join-Path $LogDir ("server-$Stamp.out.log")
    $StdErrLog = Join-Path $LogDir ("server-$Stamp.err.log")
    $Args = @(
        "-Dserver.port=$Port",
        "-Dloomq.data.dir=$DataDir",
        "-Dloomq.node.id=$NodeId",
        "-jar",
        $ServerJarPath
    )

    $Process = Start-Process -FilePath "$JavaHome\bin\java.exe" -ArgumentList $Args -WorkingDirectory $ProjectRoot -PassThru -WindowStyle Hidden -RedirectStandardOutput $StdOutLog -RedirectStandardError $StdErrLog

    $BaseUrl = "http://127.0.0.1:$Port"
    for ($i = 0; $i -lt 120; $i++) {
        if (Test-TcpPortOpen -Port $Port) {
            return [pscustomobject]@{
                Process = $Process
                BaseUrl = $BaseUrl
                Port = $Port
                DataDir = $DataDir
                NodeId = $NodeId
                StdOutLog = $StdOutLog
                StdErrLog = $StdErrLog
            }
        }

        if ($Process.HasExited) {
            break
        }

        Start-Sleep -Seconds 1
    }

    $TailOut = if (Test-Path $StdOutLog) { Get-Content $StdOutLog -Tail 40 } else { @() }
    $TailErr = if (Test-Path $StdErrLog) { Get-Content $StdErrLog -Tail 40 } else { @() }
    throw "Server failed to start on $BaseUrl.`nstdout:`n$($TailOut -join [Environment]::NewLine)`nstderr:`n$($TailErr -join [Environment]::NewLine)"
}

function Stop-BenchmarkServer {
    param($ServerHandle)

    if ($null -ne $ServerHandle -and $ServerHandle.Process -and -not $ServerHandle.Process.HasExited) {
        try {
            Stop-Process -Id $ServerHandle.Process.Id -Force -ErrorAction SilentlyContinue
        } catch {
        }
    }
}

function Get-ResultDouble {
    param(
        [hashtable]$Data,
        [string]$Key,
        [double]$Default = 0.0
    )

    if ($null -eq $Data -or -not $Data.ContainsKey($Key)) {
        return $Default
    }

    $Value = $Data[$Key]
    if ([string]::IsNullOrWhiteSpace([string]$Value)) {
        return $Default
    }

    $Parsed = 0.0
    if ([double]::TryParse([string]$Value, [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$Parsed)) {
        return $Parsed
    }

    return $Default
}

function Get-ResultInt {
    param(
        [hashtable]$Data,
        [string]$Key,
        [int]$Default = 0
    )

    if ($null -eq $Data -or -not $Data.ContainsKey($Key)) {
        return $Default
    }

    $Value = $Data[$Key]
    if ([string]::IsNullOrWhiteSpace([string]$Value)) {
        return $Default
    }

    $Parsed = 0
    if ([int]::TryParse([string]$Value, [ref]$Parsed)) {
        return $Parsed
    }

    return $Default
}

function Get-BenchmarkRowData {
    param([string[]]$Rows)

    $ParsedRows = New-Object System.Collections.Generic.List[hashtable]
    if ($null -eq $Rows) {
        return @()
    }

    foreach ($Row in $Rows) {
        $Data = ConvertFrom-BenchmarkMarker $Row
        if ($null -ne $Data -and $Data.Count -gt 0) {
            [void]$ParsedRows.Add($Data)
        }
    }

    return $ParsedRows.ToArray()
}

function Get-DoubleSeriesStats {
    param([double[]]$Values)

    if ($null -eq $Values -or $Values.Count -eq 0) {
        return [pscustomobject]@{
            Count = 0
            Mean = 0.0
            StdDev = 0.0
            CvPct = 0.0
            Min = 0.0
            Max = 0.0
        }
    }

    $Count = $Values.Count
    $Sum = 0.0
    $Min = $Values[0]
    $Max = $Values[0]
    foreach ($Value in $Values) {
        $Sum += $Value
        if ($Value -lt $Min) { $Min = $Value }
        if ($Value -gt $Max) { $Max = $Value }
    }
    $Mean = $Sum / $Count

    $Variance = 0.0
    if ($Count -gt 1) {
        foreach ($Value in $Values) {
            $Delta = $Value - $Mean
            $Variance += $Delta * $Delta
        }
        $Variance = $Variance / ($Count - 1)
    }

    $StdDev = [math]::Sqrt([math]::Max(0.0, $Variance))
    $CvPct = if ([math]::Abs($Mean) -gt 1e-9) {
        ($StdDev / [math]::Abs($Mean)) * 100.0
    } else {
        0.0
    }

    return [pscustomobject]@{
        Count = $Count
        Mean = $Mean
        StdDev = $StdDev
        CvPct = $CvPct
        Min = $Min
        Max = $Max
    }
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
        $SchedulerResult
    )

    $Lines = New-Object System.Collections.Generic.List[string]
    $Lines.Add("LoomQ Benchmark Summary")
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

        $Lines.Add("1) In-process upper bound")
        $Lines.Add(("   direct store: {0:n0} intents/s @ {1} threads for {2}s" -f $InternalQps, $Threads, $Duration))
        $Lines.Add(("   retained memory: {0:n2} MB, {1:n0} bytes/intent" -f ($MemDelta / 1MB), $BytesPerIntent))
        $Lines.Add(("   scenario runtime: {0:n1} s" -f $InternalResult.DurationSec))
        $Lines.Add("   shortboard: this is the storage-side ceiling; HTTP and scheduler overhead are excluded.")
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

    return $Lines.ToArray()
}

# ============================================================
#  新主程序
# ============================================================

$script:PauseOnExit = (-not $NoPause) -and ($KeepOpen -or (Test-LaunchedFromExplorer))
$script:VerboseScenarioOutput = $VerboseOutput -or $script:PauseOnExit

# 显示帮助
if ($Help) {
    Write-Host @"
LoomQ Performance Benchmark v0.7.1

Usage: .\benchmark.ps1 [Options]

Options:
  -InternalOnly    Run only internal tests (no server required)
  -Quick           Quick test mode
  -Compare         Show history comparison only
  -NoCompile       Skip compilation
  -Save            Auto-save results to history
  -NoSave          Preview mode (do not save)
  -KeepOpen        Keep console window open after finish
  -NoPause         Never pause on exit (for scripted runs)
  -VerboseOutput   Show full scenario logs
  -Help            Show this help

Examples:
  .\benchmark.ps1                  # Run full tests, ask to save
  .\benchmark.ps1 -InternalOnly    # Internal tests only
  .\benchmark.ps1 -Compare         # View history
  .\benchmark.ps1 -Save            # Auto save results
  .\benchmark.ps1 -NoSave          # Preview mode
  .\benchmark.ps1 -KeepOpen        # Keep window open
  .\benchmark.ps1 -VerboseOutput   # Full scenario logs
"@
    Exit-BenchmarkScript -ExitCode 0
}

# 横幅
Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Magenta
Write-Host "|          LoomQ Atomic Performance Benchmark v0.7.1         |" -ForegroundColor Magenta
Write-Host "+============================================================+" -ForegroundColor Magenta
Write-Host ""

# 查找项目根目录
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

# 检查 Java
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

# 检查 Maven
$MavenCmd = Get-Command mvn -ErrorAction SilentlyContinue
if (-not $MavenCmd) {
    Write-Error "Maven not found!"
    Write-Host ""
    Write-Host "Please install Maven and add to PATH" -ForegroundColor Yellow
    Write-Info "Download: https://maven.apache.org/download.cgi"
    Wait-Exit
}

# Git 信息
$GitCommit = Get-GitCommit
$GitVersion = Get-GitVersion
Write-Host "   Git: $GitCommit ($GitVersion)" -ForegroundColor Gray

# 显示历史对比
if ($Compare) {
    Write-Header "Latest Summary"
    $LatestSummary = Get-LatestSummaryFile
    if ($LatestSummary) {
        Write-Info "Summary: $($LatestSummary.FullName)"
        Get-Content $LatestSummary.FullName | ForEach-Object { Write-Host $_ }
    } else {
        Write-Warning "No benchmark summary found yet."
    }
    Exit-BenchmarkScript -ExitCode 0
}

# 编译项目
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
$InternalResult = $null
$HttpResult = $null
$SchedulerResult = $null

try {
    $InternalResult = Invoke-BenchmarkScenario `
        -Name "1) In-process upper bound" `
        -ServerModuleDir $ServerModuleDir `
        -MainClass "com.loomq.benchmark.InternalBenchmark" `
        -TimeoutSec $(if ($Quick) { 120 } else { 240 }) `
        -Quick:$Quick `
        -VerboseOutput:$script:VerboseScenarioOutput

    if ($InternalResult.ExitCode -ne 0) {
        throw "Internal benchmark failed with exit code $($InternalResult.ExitCode)"
    }

    if (-not $InternalOnly) {
        $ServerJarPath = Ensure-ServerJar -ProjectRoot $ProjectRoot -ServerModuleDir $ServerModuleDir -AllowBuild:(-not $NoCompile)
        $TempServerPort = Get-FreePort
        $TempDataDir = Join-Path $ProjectRoot ("benchmark\results\runtime\data-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
        New-Item -ItemType Directory -Force -Path $TempDataDir | Out-Null
        $TempNodeId = "bench-" + (Get-Date -Format "yyyyMMddHHmmss")

        Write-Info "Starting isolated server on http://127.0.0.1:$TempServerPort"
        Write-Info "Data dir: $TempDataDir"
        Write-Info "Node id:  $TempNodeId"

        $TempServerHandle = Start-BenchmarkServer `
            -ProjectRoot $ProjectRoot `
            -JavaHome $JavaHome `
            -ServerJarPath $ServerJarPath `
            -Port $TempServerPort `
            -DataDir $TempDataDir `
            -NodeId $TempNodeId

        try {
            $HttpResult = Invoke-BenchmarkScenario `
                -Name "2) HTTP create path" `
                -ServerModuleDir $ServerModuleDir `
                -MainClass "com.loomq.benchmark.HttpVirtualThreadBenchmark" `
                -BenchmarkBaseUrl $TempServerHandle.BaseUrl `
                -TimeoutSec $(if ($Quick) { 180 } else { 420 }) `
                -Quick:$Quick `
                -VerboseOutput:$script:VerboseScenarioOutput

            if ($HttpResult.ExitCode -ne 0) {
                throw "HTTP benchmark failed with exit code $($HttpResult.ExitCode)"
            }

            $SchedulerResult = Invoke-BenchmarkScenario `
                -Name "3) Scheduler trigger path" `
                -ServerModuleDir $ServerModuleDir `
                -MainClass "com.loomq.scheduler.SchedulerTriggerBenchmarkWithMockServer" `
                -TimeoutSec $(if ($Quick) { 180 } else { 420 }) `
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

    $SummaryLines = Build-BenchmarkSummary `
        -ProjectRoot $ProjectRoot `
        -GitCommit $GitCommit `
        -GitVersion $GitVersion `
        -JavaHome $JavaHome `
        -Quick ([bool]$Quick) `
        -InternalResult $InternalResult `
        -HttpResult $HttpResult `
        -SchedulerResult $SchedulerResult

    Write-Header "Summary"
    $SummaryLines | ForEach-Object { Write-Host $_ }

    if (-not $NoSave) {
        $SummaryFile = Save-BenchmarkSummary -Lines $SummaryLines
        Write-Host ""
        Write-Success "Summary saved"
        Write-Info "Report: $SummaryFile"
        $Latest = Get-LatestSummaryFile
        if ($Latest) {
            Write-Info "Latest: $($Latest.FullName)"
        }
    } else {
        Write-Host ""
        Write-Info "Preview mode: summary not saved"
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
}
