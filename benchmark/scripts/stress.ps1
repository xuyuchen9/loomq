<#
.SYNOPSIS
    LoomQ gRPC Stress Test — convenience wrapper for benchmark.ps1 -Stress

.DESCRIPTION
    Runs gRPC create-path benchmarks with multi-tier pressure to find the
    system's throughput inflection point. This is a thin wrapper around
    benchmark.ps1 -Stress -StressOnly grpc.

.PARAMETER StressThreads
    Comma-separated thread counts (default: "500,1000,2000,3000")

.PARAMETER StressDuration
    Duration per thread tier in seconds (default: 30)

.PARAMETER StressCooldown
    Cooldown between tiers in milliseconds (default: 5000)

.PARAMETER Quick
    Quick mode: reduced tiers and duration

.PARAMETER NoCompile
    Skip compilation

.PARAMETER VerboseOutput
    Show full scenario logs

.PARAMETER NoPause
    Never pause on exit
#>

param(
    [string]$StressThreads = "500,1000,2000,3000",
    [int]$StressDuration = 30,
    [int]$StressCooldown = 5000,
    [switch]$Quick,
    [switch]$NoCompile,
    [switch]$VerboseOutput,
    [switch]$NoPause
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BenchmarkScript = Join-Path $ScriptDir "benchmark.ps1"

$Args = @(
    "-Stress"
    "-StressOnly", "grpc"
    "-StressThreads", $StressThreads
    "-StressDuration", $StressDuration
    "-StressCooldown", $StressCooldown
)
if ($Quick) { $Args += "-Quick" }
if ($NoCompile) { $Args += "-NoCompile" }
if ($VerboseOutput) { $Args += "-VerboseOutput" }
if ($NoPause) { $Args += "-NoPause" }

& $BenchmarkScript @Args
