#
# LoomQ Startup Script (Windows PowerShell)
# Version: 0.6.1
#

param(
    [string]$JVM_XMS = $env:JVM_XMS,
    [string]$JVM_XMX = $env:JVM_XMX,
    [string]$JVM_GC = $env:JVM_GC,
    [int]$JVM_GC_PAUSE = $env:JVM_GC_PAUSE,
    [int]$Port = $env:LOOMQ_PORT,
    [string]$Host = $env:LOOMQ_HOST,
    [string]$DataDir = $env:LOOMQ_DATA_DIR,
    [switch]$Help
)

# Default values
$JAR_FILE = "target/loomq-0.6.0.jar"
if (-not $JVM_XMS) { $JVM_XMS = "2g" }
if (-not $JVM_XMX) { $JVM_XMX = "2g" }
if (-not $JVM_GC) { $JVM_GC = "ZGC" }
if (-not $JVM_GC_PAUSE) { $JVM_GC_PAUSE = 10 }
if (-not $Port) { $Port = 8080 }
if (-not $Host) { $Host = "0.0.0.0" }
if (-not $DataDir) { $DataDir = "./data/wal" }

# Colors
function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Error($msg) { Write-Host "[ERROR] $msg" -ForegroundColor Red }

# Show help
if ($Help) {
    @"
LoomQ Startup Script (PowerShell)

Usage: .\scripts\start.ps1 [options]

Parameters:
  -JVM_XMS         Initial heap size (default: 2g)
  -JVM_XMX         Maximum heap size (default: 2g)
  -JVM_GC          Garbage collector: ZGC, G1GC, ParallelGC (default: ZGC)
  -JVM_GC_PAUSE    Max GC pause target in ms (default: 10)
  -Port            Server port (default: 8080)
  -Host            Server host (default: 0.0.0.0)
  -DataDir         WAL data directory (default: ./data/wal)
  -Help            Show this help message

Environment Variables:
  All parameters can also be set via environment variables with prefix:
  JVM_XMS, JVM_XMX, JVM_GC, JVM_GC_PAUSE, LOOMQ_PORT, LOOMQ_HOST, LOOMQ_DATA_DIR

Examples:
  # Start with defaults
  .\scripts\start.ps1

  # Start with custom heap size
  .\scripts\start.ps1 -JVM_XMS 4g -JVM_XMX 4g

  # Start cluster node
  .\scripts\start.ps1 -Port 8081

  # Using environment variables
  \$env:JVM_XMX = "4g"
  .\scripts\start.ps1
"@
    exit 0
}

# Check Java version
function Test-Java {
    try {
        $javaVersion = & java -version 2>&1 | Select-String "version" | ForEach-Object { $_ -match '"(.+)"' | Out-Null; $matches[1] }
        $majorVersion = [int]($javaVersion -split '\.' | Select-Object -First 1)

        if ($majorVersion -lt 25) {
            Write-Error "Java 25+ is required, found: $javaVersion"
            exit 1
        }
        Write-Info "Java version: $javaVersion"
    }
    catch {
        Write-Error "Java is not installed or not in PATH"
        exit 1
    }
}

# Check JAR file
function Test-Jar {
    if (-not (Test-Path $JAR_FILE)) {
        Write-Error "JAR file not found: $JAR_FILE"
        Write-Info "Please build the project first: mvn clean package -DskipTests"
        exit 1
    }
}

# Setup directories
function Initialize-Directories {
    if (-not (Test-Path $DataDir)) {
        Write-Info "Creating data directory: $DataDir"
        New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
    }
}

# Show configuration
function Show-Config {
    Write-Host ""
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "  LoomQ Configuration" -ForegroundColor Cyan
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host "  JAR File:        $JAR_FILE"
    Write-Host "  JVM Heap:        $JVM_XMS / $JVM_XMX"
    Write-Host "  GC Type:         $JVM_GC"
    Write-Host "  Max GC Pause:    ${JVM_GC_PAUSE}ms"
    Write-Host "  Server Host:     $Host"
    Write-Host "  Server Port:     $Port"
    Write-Host "  Data Directory:  $DataDir"
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host ""
}

# Start LoomQ
function Start-LoomQ {
    Write-Info "Starting LoomQ..."

    # Build JVM arguments
    $jvmArgs = @(
        "-Xms${JVM_XMS}"
        "-Xmx${JVM_XMX}"
        "-XX:+Use${JVM_GC}"
        "-XX:MaxGCPauseMillis=${JVM_GC_PAUSE}"
    )

    # System properties
    $sysProps = @(
        "-Dloomq.server.host=${Host}"
        "-Dloomq.server.port=${Port}"
    )

    # Add extra properties from environment
    if ($env:LOOMQ_EXTRA_PROPS) {
        $sysProps += $env:LOOMQ_EXTRA_PROPS -split '\s+'
    }

    # Build command
    $javaCmd = "java $($jvmArgs -join ' ') $($sysProps -join ' ') -jar `"$JAR_FILE`""

    Write-Info "Command: $javaCmd"
    Write-Host ""

    # Start the process
    try {
        & java @jvmArgs @sysProps -jar $JAR_FILE
    }
    catch {
        Write-Error "Failed to start LoomQ: $_"
        exit 1
    }
}

# Handle Ctrl+C
$null = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action {
    Write-Warn "Received shutdown signal, stopping LoomQ..."
}

# Main execution
Write-Info "LoomQ Startup Script v0.6.1"

Test-Java
Test-Jar
Initialize-Directories
Show-Config
Start-LoomQ
