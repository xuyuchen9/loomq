@echo off
powershell -ExecutionPolicy Bypass -File "%~dp0scripts\test-suite.ps1" %*
if %errorlevel% neq 0 pause
