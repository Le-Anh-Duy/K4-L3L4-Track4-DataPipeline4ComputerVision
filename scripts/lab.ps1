<#
.SYNOPSIS
    Lệnh ngắn cho bài lab (chạy trên PowerShell, thực thi trong container Airflow đang chạy).
.DESCRIPTION
    scripts\lab.ps1 step <1..5|all> [--reset]   chạy TỪNG cấu phần, in kết quả và kiểm tra (step all = cả pipeline)
    scripts\lab.ps1 exp <idempotency|break-gate|leakage|threshold> [--trip ...]   thí nghiệm, ghi bằng chứng vào report/evidence/
    scripts\lab.ps1 verify [--module N]     kiểm tra tự động (27 kiểm tra, 5 module); --module 2 chỉ chạy module 2
    scripts\lab.ps1 test                    unit test (dhash, blur_score...)
    scripts\lab.ps1 run [--reset]           chạy cả pipeline không qua Airflow (≈ 1 phút)
    scripts\lab.ps1 cli <lệnh> [tham số]    ví dụ: cli ingest --drive drive_a | cli history silver.frames | cli sql "select 1"
    scripts\lab.ps1 dbt <lệnh> [tham số]    ví dụ: dbt run --select silver_telemetry | dbt test --select gold_dataset_frames
    scripts\lab.ps1 shell                   mở shell trong container
    scripts\lab.ps1 get-trip <MSSV>         xác định trip phân công theo MSSV (cho bài tập Bước 5)
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Action,

    [Parameter(Position = 1, ValueFromRemainingArguments = $true)]
    [string[]]$RemainingArgs
)

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

# Chuyển về thư mục gốc của project
$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $ProjectRoot

function Show-Help {
    Write-Host @"
================================================================================
 Lab Day 9 Helper (PowerShell)
================================================================================
Cú pháp:
  .\scripts\lab.ps1 step <1..5|all> [--reset]  Chạy TỪNG cấu phần, in kết quả và kiểm tra (step all = cả pipeline)
  .\scripts\lab.ps1 exp <tên_thí_nghiệm>        Chạy thí nghiệm: idempotency, break-gate, leakage, threshold
  .\scripts\lab.ps1 verify [--module N]        Kiểm tra tự động (27 kiểm tra, 5 module); ví dụ: --module 2
  .\scripts\lab.ps1 test                       Chạy unit test (dHash, blur_score...)
  .\scripts\lab.ps1 run [--reset]              Chạy cả pipeline trong container không qua Airflow (≈ 1 phút)
  .\scripts\lab.ps1 cli <lệnh> [tham số]       Chạy lệnh pipeline CLI (ingest, sql, history, list-trips, ...)
  .\scripts\lab.ps1 dbt <lệnh> [tham số]       Chạy dbt trong container (run, test...)
  .\scripts\lab.ps1 shell                      Mở bash interactive shell trong container Airflow
  .\scripts\lab.ps1 get-trip <MSSV>            In ra mã trip phân công cho MSSV của bạn (Bước 5)

Ví dụ:
  .\scripts\lab.ps1 step all --reset
  .\scripts\lab.ps1 exp idempotency
  .\scripts\lab.ps1 cli sql "select * from lake.bronze_video_files"
  .\scripts\lab.ps1 get-trip 20210001
================================================================================
"@
}

if (-not $Action -or $Action -in @("-h", "--help", "help")) {
    Show-Help
    exit 0
}

$PY = "/home/airflow/lab-venv/bin/python"
$DBT = "/home/airflow/lab-venv/bin/dbt"

switch ($Action.ToLower()) {
    "step" {
        & docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli step @RemainingArgs
    }
    "exp" {
        & docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli exp @RemainingArgs
    }
    "verify" {
        & docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli verify @RemainingArgs
    }
    "test" {
        & docker compose exec -T -w /opt/lab airflow $PY -m unittest discover -s tests @RemainingArgs
    }
    "run" {
        & docker compose exec -T -w /opt/lab airflow bash scripts/run_pipeline.sh @RemainingArgs
    }
    "cli" {
        & docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli @RemainingArgs
    }
    "dbt" {
        & docker compose exec -T -w /opt/dbt airflow $DBT @RemainingArgs --no-use-colors
    }
    "shell" {
        & docker compose exec airflow bash
    }
    "get-trip" {
        if (-not $RemainingArgs -or $RemainingArgs.Count -eq 0) {
            Write-Error "Vui lòng nhập MSSV. Ví dụ: .\scripts\lab.ps1 get-trip 20210001"
            exit 1
        }
        $Mssv = $RemainingArgs[0]
        $PyCode = "import sys,json; t=json.loads(sys.stdin.read().lstrip('\ufeff')); m=int(sys.argv[1]); print(t[m % len(t)])"
        $TripsJson = & docker compose exec -T -w /opt/lab airflow $PY -m pipeline.cli list-trips 2>$null
        $Trip = ""
        if ($LASTEXITCODE -eq 0 -and $TripsJson) {
            $Trip = ($TripsJson | & docker compose exec -T -w /opt/lab airflow $PY -c $PyCode $Mssv)
            if ($Trip) { $Trip = $Trip.Trim() }
        } else {
            # Fallback đọc từ data/trips.csv nếu bronze chưa nạp
            $CsvPath = Join-Path $ProjectRoot "data/trips.csv"
            if (Test-Path $CsvPath) {
                $Trip = python -c "import sys,csv; f=open(r'$CsvPath'); r=csv.DictReader(f); trips=sorted(x['trip_id'] for x in r); m=int(sys.argv[1]); print(trips[m % len(trips)])" $Mssv
                $Trip = $Trip.Trim()
            }
        }
        if ($Trip) {
            Write-Host "MSSV: $Mssv -> Trip phân công: $Trip" -ForegroundColor Green
            Write-Host "Thêm vào .env để làm bài tập bước 5: LAB_FAIL_ONCE=$Trip" -ForegroundColor Yellow
        } else {
            Write-Host "Chưa thể lấy danh sách trips. Vui lòng kiểm tra data/trips.csv hoặc chạy 'step 1'." -ForegroundColor Red
            exit 1
        }
    }
    default {
        Write-Error "Lệnh không hợp lệ: '$Action'"
        Show-Help
        exit 1
    }
}

exit $LASTEXITCODE
