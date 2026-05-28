# benchmark-util.ps1 — LoomQ Benchmark Utility Functions
# Environment detection, marker parsing, result extraction, statistics.

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

function Get-FreePort {
    $Listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    try {
        $Listener.Start()
        return ([System.Net.IPEndPoint]$Listener.LocalEndpoint).Port
    } finally {
        $Listener.Stop()
    }
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

function Get-RowQpsStats {
    param($Rows)
    $Values = @()
    foreach ($Row in $Rows) { $Values += Get-ResultDouble $Row "qps" }
    return Get-DoubleSeriesStats -Values ([double[]]$Values)
}

function ConvertTo-ParsedBenchmark {
    param($ScenarioResult)

    $Parsed = @{
        Rows = @()
        Summary = @{}
        Latency = @{}
        E2E = @{}
        Semaphore = @{}
        Env = @{}
        Global = @{}
        Cohort = @{}
        Optimization = @{}
        SysQps = @{}
    }

    if (-not $ScenarioResult -or -not $ScenarioResult.Lines) { return $Parsed }

    foreach ($line in $ScenarioResult.Lines) {
        if ($line -match '^(RESULT_[A-Z0-9_]+)\|(.+)') {
            $markerType = $Matches[1]
            $row = @{}
            foreach ($pair in $Matches[2].Split('|')) {
                $kv = $pair.Split('=', 2)
                if ($kv.Count -eq 2) { $row[$kv[0]] = $kv[1] }
            }
            switch ($markerType) {
                'RESULT_ROW'            { $Parsed.Rows += $row }
                'RESULT_LATENCY'        { $Parsed.Latency[$row["tier"]] = $row }
                'RESULT_E2E_LATENCY'    { $Parsed.E2E[$row["tier"]] = $row }
                'RESULT_SEMAPHORE'      { $Parsed.Semaphore[$row["tier"]] = $row }
                'RESULT_ENV'            { $Parsed.Env = $row }
                'RESULT_GLOBAL_LATENCY' { $Parsed.Global = $row }
                'RESULT_COHORT'         { $Parsed.Cohort = $row }
                'RESULT_OPTIMIZATION'   { $Parsed.Optimization = $row }
                'RESULT_SYSTEM_QPS'     { $Parsed.SysQps = $row }
            }
        }
    }

    # Also use the pre-parsed Data as Summary
    if ($ScenarioResult.Data) {
        $Parsed.Summary = $ScenarioResult.Data
    }

    return $Parsed
}
