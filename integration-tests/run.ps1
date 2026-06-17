#requires -Version 5.1
# Convenience runner for the Beacon Docker integration tests (Windows / PowerShell).
#
# Usage:
#   ./run.ps1                       # build the local Dockerfile and run the suite
#   $env:BEACON_IMAGE="ghcr.io/maris-development/beacon:latest"; ./run.ps1   # reuse an image
#
# Any extra arguments are passed through to pytest, e.g.:
#   ./run.ps1 -k external -v

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

$venv = Join-Path $PSScriptRoot ".venv"
if (-not (Test-Path $venv)) {
    python -m venv $venv
}
& (Join-Path $venv "Scripts/python.exe") -m pip install --quiet --upgrade pip
& (Join-Path $venv "Scripts/python.exe") -m pip install --quiet -r requirements.txt
& (Join-Path $venv "Scripts/python.exe") -m pytest @args
