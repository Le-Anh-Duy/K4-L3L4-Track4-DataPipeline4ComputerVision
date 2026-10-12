<#
.SYNOPSIS
    Chạy toàn bộ pipeline không qua Airflow (PowerShell).
.DESCRIPTION
    Tương đương scripts/run_pipeline.sh.
    Mặc định sẽ thực thi bên trong container Airflow đang chạy (nơi đã có đầy đủ Python, dbt, DuckDB, FFmpeg).
    Nếu dùng cờ -Local, script sẽ thực thi trực tiếp trên máy chủ Windows hiện tại (yêu cầu Python venv cục bộ).
    
    Cú pháp:
      .\scripts\run_pipeline.ps1            # Chạy toàn bộ pipeline
      .\scripts\run_pipeline.ps1 -Reset     # Reset lake trước khi chạy
      .\scripts\run_pipeline.ps1 --reset    # Tương tự như trên
      .\scripts\run_pipeline.ps1 -Local     # Chạy trực tiếp trên máy host
#>

[CmdletBinding()]
param(
    [switch]$Reset,
    [switch]$Local,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RemainingArgs
)

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $ProjectRoot

$ShouldReset = $Reset.IsPresent -or ($RemainingArgs -contains "--reset") -or ($RemainingArgs -contains "-Reset")

if (-not $Local) {
    # Mặc định: Chạy qua container Airflow
    Write-Host "==> Chạy pipeline bên trong container Airflow..." -ForegroundColor Cyan
    $ResetFlag = if ($ShouldReset) { "--reset" } else { "" }
    & (Join-Path $PSScriptRoot "lab.ps1") run $ResetFlag
    exit $LASTEXITCODE
}

# Chế độ chạy cục bộ trên máy host (nếu có môi trường Python + dbt cục bộ)
Write-Host "==> Chạy pipeline trực tiếp trên máy Host..." -ForegroundColor Cyan

$PY = if ($env:PY) { $env:PY } else { "python" }
$DBT = if ($env:DBT) { $env:DBT } else { "dbt" }
$DBT_DIR = if ($env:DBT_DIR) { $env:DBT_DIR } else { "dbt" }

$env:DBT_PROFILES_DIR = if ($env:DBT_PROFILES_DIR) { $env:DBT_PROFILES_DIR } else { (Join-Path $ProjectRoot $DBT_DIR) }
$env:DBT_TARGET_PATH = if ($env:DBT_TARGET_PATH) { $env:DBT_TARGET_PATH } else { (Join-Path $ProjectRoot "work/dbt-target") }
$env:DBT_LOG_PATH = if ($env:DBT_LOG_PATH) { $env:DBT_LOG_PATH } else { (Join-Path $ProjectRoot "work/dbt-logs") }

if ($ShouldReset) {
    Write-Host "== Resetting lake..." -ForegroundColor Yellow
    & $PY -m pipeline.cli reset
    $DuckDbFile = if ($env:DUCKDB_PATH) { $env:DUCKDB_PATH } else { "work/lakehouse.duckdb" }
    if (Test-Path $DuckDbFile) {
        Remove-Item -Force $DuckDbFile
    }
}

function Step-Log([string]$msg) {
    Write-Host "`n== $msg" -ForegroundColor Green
}

Step-Log "bronze"
& $PY -m pipeline.cli ingest --drive drive_a
& $PY -m pipeline.cli ingest --drive drive_b
& $PY -m pipeline.cli ingest-trips

Step-Log "silver frames (one call per trip)"
$TripsJson = & $PY -m pipeline.cli list-trips
$TripsList = $TripsJson | & $PY -c "import sys,json; print(' '.join(json.loads(sys.stdin.read().splitlines()[-1])))"
foreach ($t in ($TripsList -split '\s+')) {
    if ($t) {
        & $PY -m pipeline.cli extract --trip $t | Out-Null
    }
}

Step-Log "dbt silver + gate"
Push-Location $DBT_DIR
try {
    & $DBT run --no-use-colors --select path:models/staging path:models/silver
    & $DBT test --no-use-colors --select source:lake path:models/silver path:models/staging
}
finally {
    Pop-Location
}

Step-Log "dbt gold + gate"
Push-Location $DBT_DIR
try {
    & $DBT run --no-use-colors --select path:models/gold path:models/marts
    & $DBT test --no-use-colors --select path:models/gold path:models/marts
}
finally {
    Pop-Location
}

Step-Log "publish"
& $PY -m pipeline.cli publish-lake | Out-Null
& $PY -m pipeline.cli export-dataset
& $PY -m pipeline.cli publish-warehouse

Step-Log "verify"
& $PY -m pipeline.cli verify

Write-Host "`nPipeline hoàn tất!" -ForegroundColor Green
exit $LASTEXITCODE
