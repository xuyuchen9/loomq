# lib/ui.ps1 — LoomQ Benchmark UI Utilities
# Colors, symbols, and Write-* helper functions.

# ANSI 转义字符 (PowerShell 5.1 兼容)
$ESC = [char]27

# 颜色定义
$C = @{}
$C.Reset   = "$ESC$([char]91)0m"
$C.Bold    = "$ESC$([char]91)1m"
$C.Dim     = "$ESC$([char]91)2m"
$C.Red     = "$ESC$([char]91)31m"
$C.Green   = "$ESC$([char]91)32m"
$C.Yellow  = "$ESC$([char]91)33m"
$C.Blue    = "$ESC$([char]91)34m"
$C.Magenta = "$ESC$([char]91)35m"
$C.Cyan    = "$ESC$([char]91)36m"
$C.White   = "$ESC$([char]91)37m"
$C.BrightRed    = "$ESC$([char]91)91m"
$C.BrightGreen  = "$ESC$([char]91)92m"
$C.BrightYellow = "$ESC$([char]91)93m"
$C.BrightBlue   = "$ESC$([char]91)94m"
$C.BrightCyan   = "$ESC$([char]91)96m"
$C.BgGreen  = "$ESC$([char]91)42m"
$C.BgRed    = "$ESC$([char]91)41m"
$C.BgYellow = "$ESC$([char]91)43m"

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
