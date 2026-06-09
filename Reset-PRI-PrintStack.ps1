#Requires -RunAsAdministrator
<#
PRI RFID Print Stack Reset
Purpose:
  Force-clear stuck Windows print jobs, restart Windows Print Spooler,
  restart BarTender / Seagull / Printer Maestro services, and restart RFID PrintSvc.

Run from an elevated PowerShell window:
  powershell.exe -ExecutionPolicy Bypass -File .\Reset-PRI-PrintStack.ps1

Optional:
  powershell.exe -ExecutionPolicy Bypass -File .\Reset-PRI-PrintStack.ps1 -LeavePrintSvcStopped
#>

param(
    [switch]$LeavePrintSvcStopped
)

$ErrorActionPreference = "Continue"

$PrintSvcName = "RFID-PrintSvc-BarTender"
$SpoolPath = Join-Path $env:SystemRoot "System32\spool\PRINTERS"
$LogRoot = "C:\PrintSvc\logs"
$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = Join-Path $LogRoot "print-stack-reset_$Stamp.log"

if (!(Test-Path $LogRoot)) {
    New-Item -ItemType Directory -Path $LogRoot -Force | Out-Null
}

Start-Transcript -Path $LogFile -Force | Out-Null

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==== $Message ====" -ForegroundColor Cyan
}

function Stop-ServiceIfExists {
    param([string]$Name)

    $svc = Get-Service -Name $Name -ErrorAction SilentlyContinue
    if ($null -eq $svc) {
        Write-Host "Service not found: $Name"
        return
    }

    if ($svc.Status -ne "Stopped") {
        Write-Host "Stopping service: $Name"
        Stop-Service -Name $Name -Force -ErrorAction SilentlyContinue
        try {
            $svc.WaitForStatus("Stopped", "00:00:20")
        } catch {
            Write-Warning "Service did not stop cleanly within 20 seconds: $Name"
        }
    } else {
        Write-Host "Already stopped: $Name"
    }
}

function Start-ServiceIfExists {
    param([string]$Name)

    $svc = Get-Service -Name $Name -ErrorAction SilentlyContinue
    if ($null -eq $svc) {
        Write-Host "Service not found: $Name"
        return
    }

    if ($svc.Status -ne "Running") {
        Write-Host "Starting service: $Name"
        try {
            Start-Service -Name $Name -ErrorAction Stop
            (Get-Service -Name $Name).WaitForStatus("Running", "00:00:30")
        } catch {
            Write-Warning "Could not start service: $Name :: $($_.Exception.Message)"
        }
    } else {
        Write-Host "Already running: $Name"
    }
}

Write-Step "Initial printer/job snapshot"
Get-Printer | Sort-Object Name | ForEach-Object {
    $printer = $_.Name
    $jobs = Get-PrintJob -PrinterName $printer -ErrorAction SilentlyContinue
    if ($jobs) {
        Write-Host ""
        Write-Host "Printer: $printer" -ForegroundColor Yellow
        $jobs | Select-Object ID, Name, JobStatus, SubmittedTime, Size | Format-Table -AutoSize
    }
}

Write-Step "Stop PRI RFID PrintSvc"
Stop-ServiceIfExists -Name $PrintSvcName

Write-Step "Find BarTender / Seagull / Printer Maestro services"
$btServices = Get-Service | Where-Object {
    (
        $_.Name -match "BarTender|Seagull|Printer.?Maestro" -or
        $_.DisplayName -match "BarTender|Seagull|Printer.?Maestro"
    ) -and
    $_.Name -ne $PrintSvcName
} | Sort-Object DisplayName

$btServices | Select-Object Name, DisplayName, Status | Format-Table -AutoSize

Write-Step "Stop BarTender / Seagull / Printer Maestro services"
foreach ($svc in $btServices) {
    Stop-ServiceIfExists -Name $svc.Name
}

Write-Step "Stop Windows Print Spooler"
Stop-ServiceIfExists -Name "Spooler"
Start-Sleep -Seconds 3

Write-Step "Kill lingering print processes"
foreach ($procName in @("spoolsv", "PrintIsolationHost", "splwow64")) {
    Get-Process -Name $procName -ErrorAction SilentlyContinue | ForEach-Object {
        Write-Host "Killing process: $($_.ProcessName) PID=$($_.Id)"
        Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
    }
}

Write-Step "Delete stuck spool files"
if (Test-Path $SpoolPath) {
    $files = Get-ChildItem -Path $SpoolPath -Force -ErrorAction SilentlyContinue
    Write-Host "Spool files found: $($files.Count)"
    $files | Remove-Item -Force -Recurse -ErrorAction SilentlyContinue
} else {
    Write-Warning "Spool path not found: $SpoolPath"
}

Write-Step "Start Windows Print Spooler"
Start-ServiceIfExists -Name "Spooler"

Write-Step "Start BarTender / Seagull / Printer Maestro services"
foreach ($svc in $btServices) {
    Start-ServiceIfExists -Name $svc.Name
}

Write-Step "Explicitly verify/start Printer Maestro"
$printerMaestro = Get-Service | Where-Object {
    $_.Name -match "Printer.?Maestro" -or $_.DisplayName -match "Printer.?Maestro"
}
if ($printerMaestro) {
    $printerMaestro | Select-Object Name, DisplayName, Status | Format-Table -AutoSize
    foreach ($svc in $printerMaestro) {
        Start-ServiceIfExists -Name $svc.Name
    }
} else {
    Write-Warning "Printer Maestro service was not found by name/display-name match."
}

if ($LeavePrintSvcStopped) {
    Write-Step "Leaving PRI RFID PrintSvc stopped by request"
} else {
    Write-Step "Start PRI RFID PrintSvc"
    Start-ServiceIfExists -Name $PrintSvcName
}

Write-Step "Final service status"
Get-Service | Where-Object {
    $_.Name -eq $PrintSvcName -or
    $_.Name -eq "Spooler" -or
    $_.Name -match "BarTender|Seagull|Printer.?Maestro" -or
    $_.DisplayName -match "BarTender|Seagull|Printer.?Maestro"
} | Sort-Object DisplayName | Select-Object Name, DisplayName, Status | Format-Table -AutoSize

Write-Step "Final printer/job snapshot"
$foundJobs = $false
Get-Printer | Sort-Object Name | ForEach-Object {
    $printer = $_.Name
    $jobs = Get-PrintJob -PrinterName $printer -ErrorAction SilentlyContinue
    if ($jobs) {
        $foundJobs = $true
        Write-Host ""
        Write-Host "Printer: $printer" -ForegroundColor Yellow
        $jobs | Select-Object ID, Name, JobStatus, SubmittedTime, Size | Format-Table -AutoSize
    }
}
if (-not $foundJobs) {
    Write-Host "No remaining Windows print jobs found." -ForegroundColor Green
}

Write-Host ""
Write-Host "Log written to: $LogFile" -ForegroundColor Green

Stop-Transcript | Out-Null
