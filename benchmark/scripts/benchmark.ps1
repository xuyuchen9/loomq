<#
.SYNOPSIS
    LoomQ Performance Benchmark Test Script

.DESCRIPTION
    Run atomic performance tests for LoomQ delayed task queue.

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
#>

param(
    [switch]$InternalOnly,
    [switch]$Quick,
    [switch]$Compare,
    [switch]$NoCompile,
    [switch]$Save,
    [switch]$NoSave,
    [switch]$Help
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
    Read-Host "Press Enter to exit"
    exit 1
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
    Read-Host "Press Enter to exit"
    exit 1
}

# ============================================================
#  工具函数
# ============================================================

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

# ============================================================
#  主程序
# ============================================================

# 显示帮助
if ($Help) {
    Write-Host @"
LoomQ Performance Benchmark v0.7.0

Usage: .\benchmark.ps1 [Options]

Options:
  -InternalOnly    Run only internal tests (no server required)
  -Quick           Quick test mode
  -Compare         Show history comparison only
  -NoCompile       Skip compilation
  -Save            Auto-save results to history
  -NoSave          Preview mode (do not save)
  -Help            Show this help

Examples:
  .\benchmark.ps1                  # Run full tests, ask to save
  .\benchmark.ps1 -InternalOnly    # Internal tests only
  .\benchmark.ps1 -Compare         # View history
  .\benchmark.ps1 -Save            # Auto save results
  .\benchmark.ps1 -NoSave          # Preview mode
"@
    Read-Host "`nPress Enter to exit"
    exit 0
}

# 横幅
Write-Host ""
Write-Host "+============================================================+" -ForegroundColor Magenta
Write-Host "|          LoomQ Atomic Performance Benchmark v0.7.0         |" -ForegroundColor Magenta
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

# 检查 Java
Write-Header "Environment Check"
$JavaHome = Test-Java
$JavaVersion = & "$JavaHome\bin\java" -version 2>&1 | Select-Object -First 1
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
    Write-Header "History Comparison"
    $HistoryFile = "benchmark\results\history.csv"
    if (Test-Path $HistoryFile) {
        $lines = Get-Content $HistoryFile
        $header = $true
        foreach ($line in $lines) {
            if ($header) {
                Write-Host $line -ForegroundColor Gray
                $header = $false
            } elseif ($line -match "UP") {
                Write-Host $line -ForegroundColor Green
            } elseif ($line -match "DOWN") {
                Write-Host $line -ForegroundColor Red
            } else {
                Write-Host $line
            }
        }
    } else {
        Write-Warning "No history records found."
    }
    Read-Host "`nPress Enter to exit"
    exit 0
}

# 编译项目
if (-not $NoCompile) {
    Write-Header "Compiling Project"
    $CompileStart = Get-Date

    Write-Host "Running: mvn compile test-compile -q" -ForegroundColor Gray

    $CompileOutput = & mvn compile test-compile -q 2>&1
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

# 检查服务器状态
Write-Header "Checking Server Status"
$ServerRunning = $false
try {
    $Response = Invoke-WebRequest -Uri "http://localhost:8080/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
    if ($Response.StatusCode -eq 200) {
        $ServerRunning = $true
        Write-Success "Server is running: http://localhost:8080"
    }
} catch {
    # Server not running
}

if (-not $ServerRunning) {
    Write-Warning "Server not running. Running internal tests only."
    Write-Host "   Start server: java -jar loomq-server\target\loomq-server-0.7.0-SNAPSHOT.jar" -ForegroundColor Gray
    $InternalOnly = $true
}

# 跟踪是否保存
$ShouldSave = $false
$VersionChanged = $false

# 检查版本变化
if (-not $NoSave) {
    $CurrentCommit = Get-GitCommit
    $CurrentVersion = Get-GitVersion
    $VersionChanged = Test-VersionChanged
    Write-Host ""
    Write-Host "Current commit: $CurrentCommit (version: $CurrentVersion)" -ForegroundColor Gray
    if ($VersionChanged) {
        Write-Warning "Version changed since last benchmark!"
        Write-Host "   It is recommended to save results for comparison." -ForegroundColor Yellow
    }
}

# 运行基准测试
Write-Header "Running Benchmark Tests"
Write-Host ""

$TestStart = Get-Date
if ($Quick) {
    $env:BENCHMARK_QUICK = "true"
}

# 构建命令
$MvnArgs = @(
    "exec:java",
    "-Dexec.mainClass=com.loomq.benchmark.BenchmarkTest",
    "-Dexec.classpathScope=test",
    "-q"
)

# 运行测试并实时输出
& mvn @MvnArgs 2>&1 | ForEach-Object {
    $line = $_
    if ($line -match "^Running:") {
        Write-Host "  $line" -ForegroundColor Gray
    } elseif ($line -match "^>>>") {
        Write-Host ""
        Write-Host $line -ForegroundColor Cyan
    } elseif ($line -match "^==") {
        Write-Host $line -ForegroundColor Cyan
    } elseif ($line -match "UP") {
        Write-Host $line -ForegroundColor Green
    } elseif ($line -match "DOWN") {
        Write-Host $line -ForegroundColor Red
    } elseif ($line -match "^\|") {
        Write-Host $line
    } elseif ($line -match "^##") {
        Write-Host ""
        Write-Host $line -ForegroundColor Yellow
    } elseif ($line -match "^-") {
        Write-Host $line
    } else {
        Write-Host $line
    }
}

$TestTime = [math]::Round(((Get-Date) - $TestStart).TotalSeconds, 1)

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Header "Test Completed"
    Write-Success "Total time: $TestTime seconds"

    # 显示报告位置
    $ReportFiles = Get-ChildItem "benchmark\results\reports\report-*.md" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending
    $HistoryFile = "benchmark\results\history.csv"

    # 询问是否保存
    if (-not $NoSave -and -not $Save) {
        Write-Host ""
        Write-Host "==============================================================" -ForegroundColor Yellow
        Write-Host "  Save Results?" -ForegroundColor Yellow
        Write-Host "==============================================================" -ForegroundColor Yellow
        Write-Host ""
        if ($VersionChanged) {
            Write-Host "  [!] Version changed - Recommended to save for comparison" -ForegroundColor Yellow
        } else {
            Write-Host "  [i] Same version as last benchmark" -ForegroundColor Gray
        }
        Write-Host ""
        Write-Host "  [Y] Yes - Save results to history"
        Write-Host "  [N] No  - Discard results (preview mode)"
        Write-Host "  [V] View - View report before deciding"
        Write-Host ""
        $Choice = Read-Host "  Save results? [Y/N/V]"
        switch ($Choice.ToUpper()) {
            "Y" { $ShouldSave = $true }
            "V" {
                if ($ReportFiles) {
                    Get-Content $ReportFiles[0].FullName | Write-Host
                }
                Write-Host ""
                $Confirm = Read-Host "  Save results? [Y/N]"
                if ($Confirm.ToUpper() -eq "Y") {
                    $ShouldSave = $true
                }
            }
            default { $ShouldSave = $false }
        }
    } elseif ($Save) {
        $ShouldSave = $true
    }

    if ($ShouldSave) {
        Write-Host ""
        Write-Success "Results saved to history!"
        if ($ReportFiles) {
            Write-Info "Report: $($ReportFiles[0].FullName)"
        }
        Write-Info "History: $HistoryFile"
    } else {
        Write-Host ""
        Write-Info "Results NOT saved (preview mode)"
        if ($ReportFiles) {
            Write-Info "Report: $($ReportFiles[0].FullName)"
        }
    }

    Write-Host ""
    Write-Host "Tip: Use -Compare to view history comparison" -ForegroundColor Gray
    Write-Host "Tip: Use -Save to auto-save, -NoSave to preview without saving" -ForegroundColor Gray
    Read-Host "`nPress Enter to exit"
} else {
    Write-Error "Test failed!"
    Wait-Exit
}
