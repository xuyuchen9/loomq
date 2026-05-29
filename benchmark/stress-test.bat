@echo off
powershell -ExecutionPolicy Bypass -File "%~dp0scripts\stress-test.ps1" %*
if %errorlevel% neq 0 pause
