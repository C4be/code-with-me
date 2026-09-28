@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\build-desktop.ps1" %*
if errorlevel 1 (
  echo.
  echo Сборка завершилась с ошибкой.
) else (
  echo.
  echo Сборка завершена.
)
pause
