<#
.SYNOPSIS
    Script khởi tạo và chuẩn bị toàn bộ môi trường Lab Day 9 trên Windows (PowerShell).
.DESCRIPTION
    Tự động hoá các bước ở Mục 0 trong GUIDE.md:
    1. Kiểm tra Docker & Docker Compose
    2. Khởi tạo file .env (từ .env.example) nếu chưa có
    3. Giải nén dataset/day09-data.zip vào thư mục gốc nếu chưa giải nén
    4. Kiểm tra toàn vẹn dữ liệu bằng scripts/check_data.py
    5. Đảm bảo line endings (LF) cho các script Linux chạy trong Docker
    6. Khởi động các container: MinIO, PostgreSQL, Airflow
    7. Chờ các service healthy và chạy thử unit tests
#>

[CmdletBinding()]
param(
    [string]$StudentId = ""
)

[Console]::InputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $ProjectRoot

Write-Host @"
================================================================================
 KHỞI TẠO MÔI TRƯỜNG LAB DAY 9 (PowerShell / Windows)
================================================================================
"@ -ForegroundColor Cyan

# 1. Kiểm tra Docker
Write-Host "==> [1/7] Kiểm tra Docker..." -ForegroundColor Yellow
$dockerInfo = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[LỖI] Docker daemon chưa chạy. Vui lòng mở Docker Desktop và thử lại!" -ForegroundColor Red
    exit 1
}
Write-Host "  [OK] Docker đang hoạt động." -ForegroundColor Green

# 2. Thiết lập .env
Write-Host "`n==> [2/7] Kiểm tra file cấu hình .env..." -ForegroundColor Yellow
if (-not (Test-Path ".env")) {
    Write-Host "  Chưa tìm thấy .env. Đang tạo từ .env.example..." -ForegroundColor Gray
    Copy-Item ".env.example" ".env"
    Write-Host "  [OK] Đã tạo file .env." -ForegroundColor Green
} else {
    Write-Host "  [OK] File .env đã tồn tại." -ForegroundColor Green
}

if ($StudentId) {
    $envContent = Get-Content ".env" -Raw
    if ($envContent -match "LAB_STUDENT_ID=") {
        $envContent = $envContent -replace "LAB_STUDENT_ID=.*", "LAB_STUDENT_ID=$StudentId"
    } else {
        $envContent += "`nLAB_STUDENT_ID=$StudentId`n"
    }
    Set-Content ".env" $envContent.Trim()
    Write-Host "  [OK] Đã cập nhật LAB_STUDENT_ID=$StudentId vào .env" -ForegroundColor Green
} else {
    $envContent = Get-Content ".env" -Raw
    if ($envContent -match "LAB_STUDENT_ID=([^\r\n]+)") {
        $currentId = $Matches[1].Trim()
        if ($currentId) {
            Write-Host "  LAB_STUDENT_ID hiện tại: $currentId" -ForegroundColor Gray
        } else {
            Write-Host "  [LƯU Ý] LAB_STUDENT_ID trong .env đang trống. Hãy mở .env và điền MSSV của bạn." -ForegroundColor Yellow
        }
    }
}

# 3. Kiểm tra dữ liệu data/
Write-Host "`n==> [3/7] Kiểm tra dữ liệu data/..." -ForegroundColor Yellow
$requiredDataFiles = @("drive_a.mp4", "drive_b.mp4", "sources.json", "telemetry_a.csv", "telemetry_b.csv", "trips.csv")
$dataMissing = $false
foreach ($f in $requiredDataFiles) {
    if (-not (Test-Path (Join-Path "data" $f))) {
        $dataMissing = $true
        break
    }
}

if ($dataMissing) {
    if (Test-Path "dataset\day09-data.zip") {
        Write-Host "  Đang giải nén dataset/day09-data.zip..." -ForegroundColor Gray
        Expand-Archive -Path "dataset\day09-data.zip" -DestinationPath . -Force
        Write-Host "  [OK] Đã giải nén xong dữ liệu." -ForegroundColor Green
    } else {
        Write-Host "  Không thấy zip, thử chạy python scripts/fetch_data.py..." -ForegroundColor Gray
        python scripts/fetch_data.py
    }
} else {
    Write-Host "  [OK] Đã có đầy đủ các file trong data/." -ForegroundColor Green
}

# 4. Kiểm tra toàn vẹn dữ liệu
Write-Host "`n==> [4/7] Kiểm tra mã băm SHA256 dữ liệu..." -ForegroundColor Yellow
python scripts/check_data.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "[CẢNH BÁO] Kiểm tra dữ liệu chưa đạt. Xem thông báo ở trên." -ForegroundColor Yellow
}

# 5. Đảm bảo LF cho các script chạy trong Linux container
Write-Host "`n==> [5/7] Chuẩn hoá ký tự xuống dòng (LF) cho Docker container..." -ForegroundColor Yellow
$shFiles = @(
    "docker/airflow-entrypoint.sh",
    "scripts/run_pipeline.sh",
    "scripts/prepare_images.sh",
    "scripts/lab.sh"
)
foreach ($sf in $shFiles) {
    if (Test-Path $sf) {
        $bytes = [System.IO.File]::ReadAllBytes($sf)
        $text = [System.Text.Encoding]::UTF8.GetString($bytes)
        if ($text -match "\r\n") {
            $lfText = $text -replace "\r\n", "`n"
            [System.IO.File]::WriteAllBytes($sf, [System.Text.Encoding]::UTF8.GetBytes($lfText))
            Write-Host "  Đã chuẩn hoá LF: $sf" -ForegroundColor Gray
        }
    }
}
Write-Host "  [OK] Định dạng file tương thích Linux container." -ForegroundColor Green

# 6. Khởi động Docker Compose
Write-Host "`n==> [6/7] Khởi động các container Docker (PostgreSQL, MinIO, Airflow)..." -ForegroundColor Yellow
docker compose --profile orchestrate up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "[LỖI] Không thể khởi động Docker Compose." -ForegroundColor Red
    exit 1
}

# Chờ Airflow healthy
Write-Host "  Đang chờ các dịch vụ sẵn sàng (chờ tối đa 60s)..." -ForegroundColor Gray
$maxWait = 60
$elapsed = 0
$healthy = $false
while ($elapsed -lt $maxWait) {
    Start-Sleep -Seconds 3
    $elapsed += 3
    $psOut = docker compose ps --format json 2>&1
    if ($psOut -match '"airflow"') {
        $status = docker inspect --format '{{.State.Health.Status}}' day09-airflow-1 2>$null
        if ($status -eq "healthy") {
            $healthy = $true
            break
        }
    }
}

if ($healthy) {
    Write-Host "  [OK] Airflow, MinIO, PostgreSQL đã healthy!" -ForegroundColor Green
} else {
    Write-Host "  Container đang tiếp tục khởi động trong nền (bạn có thể kiểm tra qua: docker compose ps)." -ForegroundColor Yellow
}

# 7. Kiểm tra kết nối và chạy thử unit tests
Write-Host "`n==> [7/7] Chạy thử unit tests trong container..." -ForegroundColor Yellow
& (Join-Path $PSScriptRoot "lab.ps1") test

Write-Host @"

================================================================================
 MÔI TRƯỜNG ĐÃ SẴN SÀNG!
================================================================================
Các cổng dịch vụ trên máy của bạn:
  * Airflow:   http://localhost:8080   (Tài khoản: admin / admin)
  * MinIO:     http://localhost:9001   (Tài khoản: minioadmin / minioadmin)
  * Metabase:  http://localhost:3000   (Bật bằng: docker compose --profile bi up -d)

Bước tiếp theo theo GUIDE.md:
  1. Chạy thử toàn bộ pipeline:
     .\scripts\lab.ps1 step all --reset
     (hoặc gõ ngắn gọn: .\lab.ps1 step all --reset)

  2. Sau đó chạy từng bước để quan sát và ghi nhận kết quả:
     .\lab.ps1 step 1 --reset
     .\lab.ps1 step 2
     ...
================================================================================
"@ -ForegroundColor Green
