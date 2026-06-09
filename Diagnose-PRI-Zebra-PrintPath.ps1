#Requires -RunAsAdministrator
<#
PRI Zebra / Windows Print Path Diagnostic
Purpose:
  Inspect printer queues, ports, and TCP 9100 reachability for Zebra printers.
  This does not clear jobs and does not print.
#>

$ErrorActionPreference = "Continue"

Write-Host "==== Zebra / PRI printer queue status ====" -ForegroundColor Cyan

$printers = Get-Printer | Sort-Object Name | Where-Object {
    $_.Name -match "Zebra|ZD621|ZD621R|P[0-9]" -or $_.DriverName -match "Zebra|ZDesigner"
}

if (-not $printers) {
    Write-Warning "No Zebra-like printers found by name/driver filter. Showing all printers."
    $printers = Get-Printer | Sort-Object Name
}

foreach ($p in $printers) {
    Write-Host ""
    Write-Host "Printer: $($p.Name)" -ForegroundColor Yellow
    $p | Select-Object Name, PrinterStatus, JobCount, DriverName, PortName, Shared, Published | Format-List

    $port = Get-PrinterPort -Name $p.PortName -ErrorAction SilentlyContinue
    if ($port) {
        Write-Host "Port:"
        $port | Select-Object Name, PrinterHostAddress, PortNumber, Protocol, SNMPEnabled | Format-List

        if ($port.PrinterHostAddress) {
            Write-Host "TCP test to $($port.PrinterHostAddress):9100"
            Test-NetConnection -ComputerName $port.PrinterHostAddress -Port 9100 -InformationLevel Detailed
        }
    }

    $jobs = Get-PrintJob -PrinterName $p.Name -ErrorAction SilentlyContinue
    if ($jobs) {
        Write-Host "Jobs:"
        $jobs | Select-Object ID, Name, JobStatus, SubmittedTime, Size | Format-Table -AutoSize
    } else {
        Write-Host "Jobs: none"
    }
}
