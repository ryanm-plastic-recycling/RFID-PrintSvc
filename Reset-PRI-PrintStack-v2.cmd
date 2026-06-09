@echo off
setlocal

REM PRI RFID Print Stack Reset v2 launcher.
REM Right-click this file and choose "Run as administrator".

set SCRIPT_DIR=%~dp0
set PS1=%SCRIPT_DIR%Reset-PRI-PrintStack-v2.ps1

if not exist "%PS1%" (
  echo Could not find "%PS1%".
  echo Keep this CMD file and Reset-PRI-PrintStack-v2.ps1 in the same folder.
  pause
  exit /b 1
)

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%PS1%"

echo.
echo Complete.
pause
