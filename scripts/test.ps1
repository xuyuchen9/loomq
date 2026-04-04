param(
    [ValidateSet("fast", "balanced", "integration", "full", "changed")]
    [string]$Mode = "fast",
    [string]$BaseRef = "origin/main"
)

$ErrorActionPreference = "Stop"

function Invoke-Maven {
    param([string]$Arguments)

    Write-Host ">> mvn $Arguments" -ForegroundColor Cyan
    & mvn $Arguments
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

function Convert-TestPathToClassName {
    param([string]$Path)

    $relative = $Path -replace '^src/test/java/', ''
    $relative = $relative -replace '\.java$', ''
    return ($relative -replace '/', '.')
}

function Get-ChangedFiles {
    param([string]$DiffBaseRef)

    $diffRange = "$DiffBaseRef...HEAD"
    $output = git diff --name-only --diff-filter=ACMR $diffRange 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "无法基于 $DiffBaseRef 计算变更，回退到 HEAD~1...HEAD" -ForegroundColor Yellow
        $output = git diff --name-only --diff-filter=ACMR HEAD~1...HEAD
    }
    return @($output | Where-Object { $_ -and $_.Trim().Length -gt 0 })
}

switch ($Mode) {
    "fast" {
        Invoke-Maven "test"
    }
    "balanced" {
        Invoke-Maven "test -Pbalanced-tests"
    }
    "integration" {
        Invoke-Maven "test -Pintegration-tests"
    }
    "full" {
        Invoke-Maven "test -Pfull-tests"
    }
    "changed" {
        $changedFiles = Get-ChangedFiles -DiffBaseRef $BaseRef
        if ($changedFiles.Count -eq 0) {
            Write-Host "未检测到改动，跳过测试。" -ForegroundColor Yellow
            exit 0
        }

        $changedTestFiles = @($changedFiles | Where-Object { $_ -like 'src/test/java/*.java' })
        $changedMainFiles = @($changedFiles | Where-Object { $_ -like 'src/main/java/*.java' })

        if ($changedTestFiles.Count -gt 0) {
            $testClasses = @(
                $changedTestFiles |
                ForEach-Object { Convert-TestPathToClassName -Path $_ } |
                Sort-Object -Unique
            )

            $testArg = $testClasses -join ','
            Write-Host ("检测到测试文件改动，将只运行: {0}" -f ($testClasses -join ', ')) -ForegroundColor Green
            Invoke-Maven "test -Dtest=$testArg"
            exit 0
        }

        if ($changedMainFiles.Count -gt 0) {
            Write-Host "检测到主代码改动但无测试文件改动，回退到 balanced 测试集。" -ForegroundColor Yellow
            Invoke-Maven "test -Pbalanced-tests"
            exit 0
        }

        Write-Host "仅检测到非 Java 代码变更（例如文档/配置），跳过测试。" -ForegroundColor Yellow
        exit 0
    }
}
