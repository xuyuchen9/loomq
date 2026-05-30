# lib/server.ps1 — LoomQ Benchmark Server Lifecycle
# Server start/stop, JAR management, process tree cleanup, orphan detection.

function Get-LatestSummaryFile {
    $SummaryDir = Join-Path $ProjectRoot "benchmark\results\reports"
    if (-not (Test-Path $SummaryDir)) {
        return $null
    }

    return Get-ChildItem -Path $SummaryDir -Filter "summary-*.txt" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
}

function Remove-OldReports {
    param([int]$Keep = 10)

    $ReportsDir = Join-Path $ProjectRoot "benchmark\results\reports"
    $LogsDir = Join-Path $ProjectRoot "benchmark\results\logs"

    try {
        if (Test-Path $ReportsDir) {
            $reports = Get-ChildItem -Path $ReportsDir -File -ErrorAction SilentlyContinue |
                Where-Object { $_.Extension -in '.json','.txt' } |
                Sort-Object LastWriteTime -Descending
            if ($null -ne $reports -and $reports.Count -gt ($Keep * 2)) {
                $reports | Select-Object -Skip ($Keep * 2) | Remove-Item -Force -ErrorAction SilentlyContinue
            }
        }
    } catch {}

    try {
        if (Test-Path $LogsDir) {
            $logs = Get-ChildItem -Path $LogsDir -Filter "*.log" -File -ErrorAction SilentlyContinue |
                Sort-Object LastWriteTime -Descending
            if ($null -ne $logs -and $logs.Count -gt $Keep) {
                $logs | Select-Object -Skip $Keep | Remove-Item -Force -ErrorAction SilentlyContinue
            }
        }
    } catch {}
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

    $ParentPom = Join-Path $RepoPath "com\loomq\loomq-parent\0.9.2\loomq-parent-0.9.2.pom"
    $BomPom = Join-Path $RepoPath "com\loomq\loomq-bom\0.9.2\loomq-bom-0.9.2.pom"
    $CoreJar = Join-Path $RepoPath "com\loomq\loomq-core\0.9.2\loomq-core-0.9.2.jar"
    $ServerJar = Join-Path $RepoPath "com\loomq\loomq-server\0.9.2\loomq-server-0.9.2.jar"

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
        [int]$GrpcPort,
        [string]$DataDir,
        [string]$NodeId,
        [string[]]$ExtraJvmArgs = @()
    )

    $LogDir = Join-Path $ProjectRoot "benchmark\results\logs"
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    $Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $StdOutLog = Join-Path $LogDir ("server-$Stamp.out.log")
    $StdErrLog = Join-Path $LogDir ("server-$Stamp.err.log")
    $Args = @(
        "-Dserver.port=$Port",
        "-Dgrpc.enabled=true",
        "-Dgrpc.port=$GrpcPort",
        "-Dloomq.data.dir=$DataDir",
        "-Dloomq.node.id=$NodeId"
    )
    foreach ($arg in $ExtraJvmArgs) {
        if (-not [string]::IsNullOrWhiteSpace($arg)) {
            $Args += $arg
        }
    }
    $Args += @("-jar", $ServerJarPath)

    $Process = Start-Process -FilePath "$JavaHome\bin\java.exe" -ArgumentList $Args -WorkingDirectory $ProjectRoot -PassThru -WindowStyle Hidden -RedirectStandardOutput $StdOutLog -RedirectStandardError $StdErrLog

    $BaseUrl = "http://127.0.0.1:$Port"
    for ($i = 0; $i -lt 120; $i++) {
        if (Test-TcpPortOpen -Port $Port) {
            return [pscustomobject]@{
                Process = $Process
                BaseUrl = $BaseUrl
                Port = $Port
                GrpcPort = $GrpcPort
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
            Stop-ProcessTree -RootPid $ServerHandle.Process.Id
        } catch {
        }
    }
}
