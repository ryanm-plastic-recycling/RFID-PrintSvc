# Stop PrintSvc so no new jobs are submitted while clearing
Stop-Service "RFID-PrintSvc-BarTender" -Force -ErrorAction SilentlyContinue

# Stop BarTender services that may hold spooler handles
$btServices = @(
  "BarTender Integration Service",
  "BarTender Print Scheduler",
  "BarTender Print Router Service",
  "BarTender System Service",
  "Seagull Driver Notification",
  "Maestro"
)

foreach ($svc in $btServices) {
    Stop-Service $svc -Force -ErrorAction SilentlyContinue
}

# Stop Windows spooler
Stop-Service Spooler -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 3

# Kill stuck print processes
Get-Process spoolsv -ErrorAction SilentlyContinue | Stop-Process -Force
Get-Process PrintIsolationHost -ErrorAction SilentlyContinue | Stop-Process -Force
Get-Process splwow64 -ErrorAction SilentlyContinue | Stop-Process -Force

# Clear spool files
$spoolPath = "$env:SystemRoot\System32\spool\PRINTERS"
Get-ChildItem $spoolPath -Force -ErrorAction SilentlyContinue | Remove-Item -Force -Recurse -ErrorAction SilentlyContinue

# Start spooler
Start-Service Spooler

# Restart BarTender services, even if we are not relying on them right now
foreach ($svc in $btServices) {
    Start-Service $svc -ErrorAction SilentlyContinue
}

# Restart PrintSvc
Start-Service "RFID-PrintSvc-BarTender"

# Show any remaining Windows print jobs
Get-Printer | ForEach-Object {
    $jobs = Get-PrintJob -PrinterName $_.Name -ErrorAction SilentlyContinue
    if ($jobs) {
        Write-Host "`nPrinter: $($_.Name)"
        $jobs | Select-Object ID, Name, JobStatus, SubmittedTime, Size
    }
}