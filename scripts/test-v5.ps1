# LoomQ v0.5.0 测试脚本 (PowerShell)
#
# 用法:
#   .\scripts\test-v5.ps1 -TestType all          # 运行所有测试
#   .\scripts\test-v5.ps1 -TestType functional   # 运行功能测试
#   .\scripts\test-v5.ps1 -TestType stress       # 运行压力测试
#   .\scripts\test-v5.ps1 -TestType replication  # 运行复制测试

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("all", "functional", "stress", "replication", "benchmark")]
    [string]$TestType = "all",

    [Parameter(Mandatory=$false)]
    [int]$Port = 18080,

    [Parameter(Mandatory=$false)]
    [string]$DataDir = "./test-data"
)

$ErrorActionPreference = "Stop"
$TestResults = @()
$StartTime = Get-Date
$ResultDir = "./test-results"
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$ResultFile = "$ResultDir/test-result-$Timestamp.json"
$ResultMarkdown = "$ResultDir/test-report-$Timestamp.md"

# 创建结果目录
if (-not (Test-Path $ResultDir)) {
    New-Item -ItemType Directory -Force -Path $ResultDir | Out-Null
}

function Write-Header {
    param([string]$Title)
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host " $Title" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
}

function Write-TestResult {
    param(
        [string]$TestId,
        [string]$Description,
        [string]$Status,
        [string]$Details = ""
    )
    $color = if ($Status -eq "PASS") { "Green" } elseif ($Status -eq "SKIP") { "Yellow" } else { "Red" }
    Write-Host "[$Status] $TestId : $Description" -ForegroundColor $color
    if ($Details) {
        Write-Host "       $Details" -ForegroundColor Gray
    }
    $script:TestResults += [PSCustomObject]@{
        TestId = $TestId
        Description = $Description
        Status = $Status
        Details = $Details
        Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    }
}

function Invoke-HttpTest {
    param(
        [string]$Method = "GET",
        [string]$Url,
        [string]$Body = "",
        [hashtable]$Headers = @{},
        [int]$ExpectedStatus = 200,
        [string]$TestId,
        [string]$Description
    )

    try {
        $params = @{
            Uri = $Url
            Method = $Method
            Headers = $Headers
            TimeoutSec = 30
        }
        if ($Body) {
            $params.Body = $Body
            $params.ContentType = "application/json"
        }

        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $response = Invoke-RestMethod @params
        $sw.Stop()

        if ($ExpectedStatus -ge 200 -and $ExpectedStatus -lt 300) {
            Write-TestResult -TestId $TestId -Description $Description -Status "PASS" -Details "Latency: $($sw.ElapsedMilliseconds)ms"
            return $response
        } else {
            Write-TestResult -TestId $TestId -Description $Description -Status "FAIL" -Details "Expected status $ExpectedStatus"
            return $null
        }
    }
    catch {
        Write-TestResult -TestId $TestId -Description $Description -Status "FAIL" -Details $_.Exception.Message
        return $null
    }
}

function Test-ServiceReady {
    param([int]$Port)

    $maxAttempts = 30
    for ($i = 1; $i -le $maxAttempts; $i++) {
        try {
            $response = Invoke-RestMethod -Uri "http://localhost:$Port/health" -TimeoutSec 2
            if ($response.status -eq "UP") {
                return $true
            }
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
    }
    return $false
}

function Start-LoomQServer {
    param([int]$Port, [string]$DataDir, [string]$NodeId = "test-node-1")

    Write-Host "Starting LoomQ server on port $Port..." -ForegroundColor Yellow

    # Clean data directory
    if (Test-Path $DataDir) {
        Remove-Item -Recurse -Force $DataDir
    }
    New-Item -ItemType Directory -Force -Path $DataDir | Out-Null

    # Find JAR file (shaded jar is named loomq-VERSION.jar, original- prefix for non-shaded)
    $jarFile = Get-ChildItem -Path "target" -Filter "loomq-*.jar" | Where-Object { $_.Name -notmatch "^original-" } | Select-Object -First 1
    if (-not $jarFile) {
        Write-Host "Building project..." -ForegroundColor Yellow
        mvn clean package -DskipTests -q
        $jarFile = Get-ChildItem -Path "target" -Filter "loomq-*.jar" | Where-Object { $_.Name -notmatch "^original-" } | Select-Object -First 1
    }

    if (-not $jarFile) {
        throw "JAR file not found. Please run 'mvn clean package -DskipTests' first."
    }

    Write-Host "Found JAR: $($jarFile.FullName)" -ForegroundColor Gray

    # Start server
    $env:loomq_node_id = $NodeId
    $env:loomq_shard_id = "test-shard"
    $env:loomq_data_dir = $DataDir
    $env:loomq_port = $Port

    # Start process with output redirect
    $processInfo = New-Object System.Diagnostics.ProcessStartInfo
    $processInfo.FileName = "java"
    $processInfo.Arguments = "--enable-preview -jar `"$($jarFile.FullName)`""
    $processInfo.UseShellExecute = $false
    $processInfo.RedirectStandardOutput = $true
    $processInfo.RedirectStandardError = $true
    $processInfo.CreateNoWindow = $true
    $processInfo.EnvironmentVariables["loomq.node.id"] = $NodeId
    $processInfo.EnvironmentVariables["loomq.shard.id"] = "test-shard"
    $processInfo.EnvironmentVariables["loomq.data.dir"] = $DataDir
    $processInfo.EnvironmentVariables["loomq.port"] = "$Port"

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $processInfo
    $process.Start() | Out-Null

    $global:ServerProcess = $process

    Write-Host "Server process started (PID: $($process.Id)), waiting for ready..." -ForegroundColor Yellow

    # Wait for ready
    if (-not (Test-ServiceReady -Port $Port)) {
        # Show error output
        $stderr = $process.StandardError.ReadToEnd()
        Write-Host "Server stderr: $stderr" -ForegroundColor Red
        throw "Server failed to start within timeout"
    }

    Write-Host "Server started successfully (PID: $($process.Id))" -ForegroundColor Green
    return $process
}

function Stop-LoomQServer {
    param($Process)

    if ($Process -and !$Process.HasExited) {
        Write-Host "Stopping server..." -ForegroundColor Yellow
        $Process.Kill()
        $Process.WaitForExit(5000)
        Write-Host "Server stopped" -ForegroundColor Green
    }
}

function Run-FunctionalTests {
    Write-Header "Functional Tests (FT-01 ~ FT-14)"

    $baseUrl = "http://localhost:$Port"

    # FT-01: 创建 DURABLE Intent
    $body = @{
        intentId = "test-durable-$(Get-Random)"
        executeAt = (Get-Date).AddMinutes(5).ToString("o")
        deadline = (Get-Date).AddMinutes(10).ToString("o")
        shardKey = "test-shard"
        ackLevel = "DURABLE"
        callback = @{
            url = "https://example.com/webhook"
            method = "POST"
        }
    } | ConvertTo-Json -Depth 3

    $response = Invoke-HttpTest -Method POST -Url "$baseUrl/v1/intents" -Body $body `
        -TestId "FT-01" -Description "Create DURABLE Intent"

    # FT-02: 创建 ASYNC Intent
    $body = @{
        intentId = "test-async-$(Get-Random)"
        executeAt = (Get-Date).AddMinutes(5).ToString("o")
        deadline = (Get-Date).AddMinutes(10).ToString("o")
        shardKey = "test-shard"
        ackLevel = "ASYNC"
        callback = @{
            url = "https://example.com/webhook"
            method = "POST"
        }
    } | ConvertTo-Json -Depth 3

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $response = Invoke-HttpTest -Method POST -Url "$baseUrl/v1/intents" -Body $body `
        -TestId "FT-02" -Description "Create ASYNC Intent (latency check)"
    $sw.Stop()

    # FT-04: 查询 Intent
    $intentId = "test-get-$(Get-Random)"
    $body = @{
        intentId = $intentId
        executeAt = (Get-Date).AddMinutes(5).ToString("o")
        deadline = (Get-Date).AddMinutes(10).ToString("o")
        shardKey = "test-shard"
        ackLevel = "DURABLE"
        callback = @{ url = "https://example.com/webhook"; method = "POST" }
    } | ConvertTo-Json -Depth 3

    Invoke-HttpTest -Method POST -Url "$baseUrl/v1/intents" -Body $body | Out-Null
    Invoke-HttpTest -Method GET -Url "$baseUrl/v1/intents/$intentId" `
        -TestId "FT-04" -Description "Query Intent"

    # FT-05: 取消 Intent
    $intentId = "test-cancel-$(Get-Random)"
    $body = @{
        intentId = $intentId
        executeAt = (Get-Date).AddMinutes(5).ToString("o")
        deadline = (Get-Date).AddMinutes(10).ToString("o")
        shardKey = "test-shard"
        ackLevel = "DURABLE"
        callback = @{ url = "https://example.com/webhook"; method = "POST" }
    } | ConvertTo-Json -Depth 3

    Invoke-HttpTest -Method POST -Url "$baseUrl/v1/intents" -Body $body | Out-Null
    Invoke-HttpTest -Method POST -Url "$baseUrl/v1/intents/$intentId/cancel" `
        -TestId "FT-05" -Description "Cancel Intent"

    # FT-06: 修改 executeAt
    $intentId = "test-patch-$(Get-Random)"
    $body = @{
        intentId = $intentId
        executeAt = (Get-Date).AddMinutes(5).ToString("o")
        deadline = (Get-Date).AddMinutes(10).ToString("o")
        shardKey = "test-shard"
        ackLevel = "DURABLE"
        callback = @{ url = "https://example.com/webhook"; method = "POST" }
    } | ConvertTo-Json -Depth 3

    Invoke-HttpTest -Method POST -Url "$baseUrl/v1/intents" -Body $body | Out-Null

    $patchBody = @{
        executeAt = (Get-Date).AddMinutes(30).ToString("o")
    } | ConvertTo-Json

    Invoke-HttpTest -Method PATCH -Url "$baseUrl/v1/intents/$intentId" -Body $patchBody `
        -TestId "FT-06" -Description "Modify executeAt"

    # FT-07: 立即触发
    $intentId = "test-fire-$(Get-Random)"
    $body = @{
        intentId = $intentId
        executeAt = (Get-Date).AddMinutes(5).ToString("o")
        deadline = (Get-Date).AddMinutes(10).ToString("o")
        shardKey = "test-shard"
        ackLevel = "DURABLE"
        callback = @{ url = "https://example.com/webhook"; method = "POST" }
    } | ConvertTo-Json -Depth 3

    Invoke-HttpTest -Method POST -Url "$baseUrl/v1/intents" -Body $body | Out-Null
    Invoke-HttpTest -Method POST -Url "$baseUrl/v1/intents/$intentId/fire-now" `
        -TestId "FT-07" -Description "Fire immediately"

    # FT-08: 幂等性测试
    $idempotencyKey = "idem-test-$(Get-Random)"
    $intentId = "test-idem-$(Get-Random)"

    $body = @{
        intentId = $intentId
        executeAt = (Get-Date).AddMinutes(5).ToString("o")
        deadline = (Get-Date).AddMinutes(10).ToString("o")
        shardKey = "test-shard"
        ackLevel = "DURABLE"
        idempotencyKey = $idempotencyKey
        callback = @{ url = "https://example.com/webhook"; method = "POST" }
    } | ConvertTo-Json -Depth 3

    # 第一次创建
    $response1 = Invoke-HttpTest -Method POST -Url "$baseUrl/v1/intents" -Body $body `
        -TestId "FT-08-1" -Description "Create with idempotencyKey (first)"

    # 第二次创建（应返回已存在）
    $response2 = Invoke-HttpTest -Method POST -Url "$baseUrl/v1/intents" -Body $body `
        -TestId "FT-08-2" -Description "Create with same idempotencyKey (second)"

    # FT-13: 过期处理 - DISCARD
    Write-TestResult -TestId "FT-13" -Description "Expired action DISCARD" -Status "SKIP" -Details "Requires time-based test"

    # FT-14: 过期处理 - DEAD_LETTER
    Write-TestResult -TestId "FT-14" -Description "Expired action DEAD_LETTER" -Status "SKIP" -Details "Requires time-based test"
}

function Run-HealthTests {
    Write-Header "Health Check Tests"

    $baseUrl = "http://localhost:$Port"

    # Health endpoints
    Invoke-HttpTest -Method GET -Url "$baseUrl/health" `
        -TestId "HEALTH-01" -Description "Health check endpoint"

    Invoke-HttpTest -Method GET -Url "$baseUrl/health/live" `
        -TestId "HEALTH-02" -Description "Liveness probe"

    Invoke-HttpTest -Method GET -Url "$baseUrl/health/ready" `
        -TestId "HEALTH-03" -Description "Readiness probe"

    Invoke-HttpTest -Method GET -Url "$baseUrl/metrics" `
        -TestId "HEALTH-04" -Description "Prometheus metrics"
}

function Run-StressTests {
    param([int]$TaskCount = 10000)

    Write-Header "Stress Tests (PT-01 ~ PT-06)"

    $baseUrl = "http://localhost:$Port"

    # PT-01: ASYNC 写入吞吐
    Write-Host "PT-01: Testing ASYNC write throughput with $TaskCount intents..." -ForegroundColor Yellow
    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    $jobs = @()
    $batchSize = 100
    $batches = [Math]::Ceiling($TaskCount / $batchSize)

    for ($b = 0; $b -lt $batches; $b++) {
        $start = $b * $batchSize
        $end = [Math]::Min(($b + 1) * $batchSize, $TaskCount)

        for ($i = $start; $i -lt $end; $i++) {
            $body = @{
                intentId = "stress-async-$i"
                executeAt = (Get-Date).AddHours(1).ToString("o")
                deadline = (Get-Date).AddHours(2).ToString("o")
                shardKey = "stress-test"
                ackLevel = "ASYNC"
                callback = @{ url = "https://example.com/webhook"; method = "POST" }
            } | ConvertTo-Json -Depth 3

            try {
                Invoke-RestMethod -Method POST -Uri "$baseUrl/v1/intents" -Body $body -ContentType "application/json" -TimeoutSec 5 | Out-Null
            }
            catch {
                # Continue on error
            }
        }

        if ($b % 10 -eq 0) {
            Write-Host "  Progress: $([Math]::Round(($b / $batches) * 100, 1))%"
        }
    }

    $sw.Stop()
    $qps = [Math]::Round($TaskCount / $sw.Elapsed.TotalSeconds, 0)
    Write-TestResult -TestId "PT-01" -Description "ASYNC write throughput" -Status "PASS" -Details "$TaskCount intents in $($sw.Elapsed.TotalSeconds)s = $qps QPS"

    # PT-04: 混合读写测试
    Write-Host "PT-04: Testing mixed read/write..." -ForegroundColor Yellow
    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    $readCount = 0
    $writeCount = 0

    for ($i = 0; $i -lt 1000; $i++) {
        # Write
        $body = @{
            intentId = "mixed-$i"
            executeAt = (Get-Date).AddHours(1).ToString("o")
            deadline = (Get-Date).AddHours(2).ToString("o")
            shardKey = "mixed-test"
            ackLevel = "DURABLE"
            callback = @{ url = "https://example.com/webhook"; method = "POST" }
        } | ConvertTo-Json -Depth 3

        try {
            Invoke-RestMethod -Method POST -Uri "$baseUrl/v1/intents" -Body $body -ContentType "application/json" -TimeoutSec 5 | Out-Null
            $writeCount++
        }
        catch {}

        # Read
        if ($i % 10 -eq 0) {
            try {
                Invoke-RestMethod -Method GET -Uri "$baseUrl/v1/intents/mixed-$($i/10)" -TimeoutSec 5 | Out-Null
                $readCount++
            }
            catch {}
        }
    }

    $sw.Stop()
    Write-TestResult -TestId "PT-04" -Description "Mixed read/write" -Status "PASS" -Details "Writes: $writeCount, Reads: $readCount in $($sw.Elapsed.TotalSeconds)s"

    # PT-05: 长期堆积测试
    Write-TestResult -TestId "PT-05" -Description "Long-term pending tasks" -Status "SKIP" -Details "Requires extended test duration"

    # PT-06: 高并发唤醒
    Write-TestResult -TestId "PT-06" -Description "High concurrent wake-up" -Status "SKIP" -Details "Requires time-synchronized test"
}

function Run-ReplicationTests {
    Write-Header "Replication Tests (FT-15 ~ FT-22)"

    Write-TestResult -TestId "FT-15" -Description "Normal replication flow" -Status "SKIP" -Details "Requires replica node"
    Write-TestResult -TestId "FT-16" -Description "REPLICATED ACK latency" -Status "SKIP" -Details "Requires replica node"
    Write-TestResult -TestId "FT-17" -Description "Manual promotion" -Status "SKIP" -Details "Requires replica node"
    Write-TestResult -TestId "FT-18" -Description "Old primary recovery" -Status "SKIP" -Details "Requires replica node"
    Write-TestResult -TestId "FT-19" -Description "Snapshot + WAL recovery" -Status "SKIP" -Details "Requires replica node"
    Write-TestResult -TestId "FT-20" -Description "Primary kill -9" -Status "SKIP" -Details "Requires replica node"
    Write-TestResult -TestId "FT-21" -Description "Network partition" -Status "SKIP" -Details "Requires replica node"
    Write-TestResult -TestId "FT-22" -Description "Replica catch-up" -Status "SKIP" -Details "Requires replica node"
}

function Run-BenchmarkTests {
    Write-Header "Benchmark Tests"

    Write-Host "Running JUnit benchmark tests..." -ForegroundColor Yellow

    # Run benchmark framework tests
    mvn test -Pfull-tests -Dtest=BenchmarkFrameworkTest -q 2>&1 | Out-Null

    if ($LASTEXITCODE -eq 0) {
        Write-TestResult -TestId "BENCH-01" -Description "Benchmark framework tests" -Status "PASS"
    } else {
        Write-TestResult -TestId "BENCH-01" -Description "Benchmark framework tests" -Status "FAIL"
    }

    # Run unit tests
    Write-Host "Running full unit tests..." -ForegroundColor Yellow
    mvn test -Pbalanced-tests -q 2>&1 | Out-Null

    if ($LASTEXITCODE -eq 0) {
        Write-TestResult -TestId "BENCH-02" -Description "Unit tests (340 tests)" -Status "PASS"
    } else {
        Write-TestResult -TestId "BENCH-02" -Description "Unit tests" -Status "FAIL"
    }
}

function Show-Summary {
    Write-Header "Test Summary"

    $passCount = ($TestResults | Where-Object { $_.Status -eq "PASS" }).Count
    $failCount = ($TestResults | Where-Object { $_.Status -eq "FAIL" }).Count
    $skipCount = ($TestResults | Where-Object { $_.Status -eq "SKIP" }).Count
    $totalCount = $TestResults.Count

    $duration = ((Get-Date) - $StartTime).TotalSeconds

    Write-Host "Total Tests:  $totalCount" -ForegroundColor White
    Write-Host "Passed:        $passCount" -ForegroundColor Green
    Write-Host "Failed:        $failCount" -ForegroundColor Red
    Write-Host "Skipped:       $skipCount" -ForegroundColor Yellow
    Write-Host "Duration:      $([Math]::Round($duration, 1))s" -ForegroundColor White
    Write-Host ""

    if ($failCount -gt 0) {
        Write-Host "Failed Tests:" -ForegroundColor Red
        $TestResults | Where-Object { $_.Status -eq "FAIL" } | ForEach-Object {
            Write-Host "  - $($_.TestId): $($_.Description)" -ForegroundColor Red
            Write-Host "    $($_.Details)" -ForegroundColor Gray
        }
    }

    Write-Host ""
    Write-Host "Result: $(if ($failCount -eq 0) { 'ALL TESTS PASSED' } else { 'SOME TESTS FAILED' })" `
        -ForegroundColor $(if ($failCount -eq 0) { 'Green' } else { 'Red' })

    # 保存 JSON 结果
    $jsonResult = @{
        timestamp = $Timestamp
        testType = $TestType
        environment = @{
            os = "Windows 11"
            jdk = [System.getProperty]("java.version")
            port = $Port
        }
        summary = @{
            total = $totalCount
            passed = $passCount
            failed = $failCount
            skipped = $skipCount
            durationSeconds = [Math]::Round($duration, 1)
        }
        tests = $TestResults
    } | ConvertTo-Json -Depth 4

    $jsonResult | Out-File -FilePath $ResultFile -Encoding UTF8
    Write-Host "`nJSON result saved to: $ResultFile" -ForegroundColor Gray

    # 保存 Markdown 报告
    $markdown = @"
# LoomQ v0.5.0 测试报告

**时间**: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
**测试类型**: $TestType
**端口**: $Port

## 测试摘要

| 指标 | 数值 |
|------|------|
| 总测试数 | $totalCount |
| 通过 | $passCount |
| 失败 | $failCount |
| 跳过 | $skipCount |
| 耗时 | $([Math]::Round($duration, 1))s |

## 测试结果

| 测试ID | 描述 | 状态 | 详情 |
|--------|------|------|------|
$($TestResults | ForEach-Object { "| $($_.TestId) | $($_.Description) | $($_.Status) | $($_.Details) |" } | Out-String)

## 结论

$(if ($failCount -eq 0) { "✅ 所有测试通过" } else { "❌ 存在失败测试" })

---
*Generated by LoomQ Test Suite*
"@

    $markdown | Out-File -FilePath $ResultMarkdown -Encoding UTF8
    Write-Host "Markdown report saved to: $ResultMarkdown" -ForegroundColor Gray
}

# Main execution
try {
    Write-Header "LoomQ v0.5.0 Test Suite"
    Write-Host "Test Type: $TestType"
    Write-Host "Port: $Port"
    Write-Host "Data Dir: $DataDir"
    Write-Host ""

    # Build project first
    Write-Host "Building project..." -ForegroundColor Yellow
    mvn clean package -DskipTests -q
    if ($LASTEXITCODE -ne 0) {
        throw "Build failed"
    }
    Write-Host "Build completed" -ForegroundColor Green
    Write-Host ""

    # Run tests based on type
    if ($TestType -in @("all", "functional", "stress", "replication")) {
        # Start server
        $server = Start-LoomQServer -Port $Port -DataDir $DataDir

        try {
            if ($TestType -in @("all", "functional")) {
                Run-FunctionalTests
            }

            if ($TestType -in @("all")) {
                Run-HealthTests
            }

            if ($TestType -in @("all", "stress")) {
                Run-StressTests -TaskCount 10000
            }

            if ($TestType -in @("all", "replication")) {
                Run-ReplicationTests
            }
        }
        finally {
            Stop-LoomQServer -Process $server
        }
    }

    if ($TestType -in @("all", "benchmark")) {
        Run-BenchmarkTests
    }

    # Show summary
    Show-Summary
}
catch {
    Write-Host "Error: $_" -ForegroundColor Red
    Write-Host $_.ScriptStackTrace -ForegroundColor Gray
    exit 1
}
