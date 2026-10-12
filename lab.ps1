<#
.SYNOPSIS
    Root shortcut cho scripts\lab.ps1
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Action,

    [Parameter(Position = 1, ValueFromRemainingArguments = $true)]
    [string[]]$RemainingArgs
)

& (Join-Path $PSScriptRoot "scripts\lab.ps1") $Action @RemainingArgs
exit $LASTEXITCODE
