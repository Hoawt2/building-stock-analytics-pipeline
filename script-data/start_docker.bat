@echo off
setlocal
set LOGFILE=run_log.txt

echo ==== %date% %time% - Starting Docker Desktop... ==== >> %LOGFILE%

REM Khởi động Docker Desktop nếu tồn tại
if exist "C:\Program Files\Docker\Docker\Docker Desktop.exe" (
    start "" "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    echo [%date% %time%] 🟢 Docker Desktop launched. >> %LOGFILE%
) else (
    echo [%date% %time%] ❌ ERROR: Docker Desktop not found at expected location. >> %LOGFILE%
    exit /b 1
)

echo [%date% %time%] ⏳ Waiting for Docker daemon to be ready (no timeout)... >> %LOGFILE%

REM Lặp vô hạn đến khi Docker daemon sẵn sàng
:wait_docker
docker info >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    timeout /t 2 >nul
    goto wait_docker
)
echo [%date% %time%] ✅ Docker daemon is ready. >> %LOGFILE%