@echo off
powershell -ExecutionPolicy Bypass -File "%~dp0scripts\benchmark.ps1" %*
if %errorlevel% neq 0 pause
