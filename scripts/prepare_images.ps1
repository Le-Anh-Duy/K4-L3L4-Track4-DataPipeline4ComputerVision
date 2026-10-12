<#
.SYNOPSIS
    Chuẩn bị Docker images cho bài lab (PowerShell).
.DESCRIPTION
    Tải (pull) và build tất cả các Docker images trước buổi học.
    Có tùy chọn đóng gói thành day09-images.tar (~5 GB) để chia sẻ qua USB.
    
    Cú pháp:
      .\scripts\prepare_images.ps1          # pull + build
      .\scripts\prepare_images.ps1 -Save    # pull + build + lưu vào day09-images.tar
      .\scripts\prepare_images.ps1 --save   # tương tự như trên
#>

[CmdletBinding()]
param(
    [switch]$Save,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RemainingArgs
)

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $ProjectRoot

$ShouldSave = $Save.IsPresent -or ($RemainingArgs -contains "--save") -or ($RemainingArgs -contains "-Save")

Write-Host "==> [1/2] Đang tải các Docker image (pull)..." -ForegroundColor Cyan
docker compose --profile orchestrate --profile bi pull --ignore-buildable

Write-Host "==> [2/2] Đang build Docker image (day09-airflow)..." -ForegroundColor Cyan
docker compose --profile orchestrate build

if ($ShouldSave) {
    Write-Host "==> Đang lưu các image vào day09-images.tar (~5 GB)..." -ForegroundColor Yellow
    docker save -o day09-images.tar day09-airflow:latest bitnamilegacy/minio:2025.4.22 postgres:16-alpine metabase/metabase:latest
    if (Test-Path day09-images.tar) {
        $file = Get-Item day09-images.tar
        $sizeGB = [math]::Round($file.Length / 1GB, 2)
        Write-Host "Đã tạo thành công: day09-images.tar ($sizeGB GB)" -ForegroundColor Green
        Write-Host "Để load image trên máy khác, chạy: docker load -i day09-images.tar" -ForegroundColor Green
    }
}

Write-Host "Hoàn tất chuẩn bị Docker images!" -ForegroundColor Green
