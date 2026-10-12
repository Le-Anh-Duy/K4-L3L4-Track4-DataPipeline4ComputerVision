<#
.SYNOPSIS
    Kiểm tra bài nộp trước khi push lên repo (PowerShell).
.DESCRIPTION
    Tương đương: python scripts/check_submission.py
    Hỗ trợ tham số -SaveVerify để lưu kết quả verify vào report/verify.txt.

    Cú pháp:
      .\scripts\check_submission.ps1                 # Chạy kiểm tra
      .\scripts\check_submission.ps1 -SaveVerify     # Chạy lab verify và lưu report/verify.txt
      .\scripts\check_submission.ps1 --save-verify   # Tương tự như trên
#>

[CmdletBinding()]
param(
    [switch]$SaveVerify,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$RemainingArgs
)

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $ProjectRoot

$ShouldSave = $SaveVerify.IsPresent -or ($RemainingArgs -contains "--save-verify") -or ($RemainingArgs -contains "-SaveVerify")

if ($ShouldSave) {
    python scripts/check_submission.py --save-verify
} else {
    python scripts/check_submission.py @RemainingArgs
}

exit $LASTEXITCODE
