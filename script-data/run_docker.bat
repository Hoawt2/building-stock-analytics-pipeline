@echo off
cd /d "D:\ProjectBI" || (
    echo [%date% %time%] ❌ Failed to change directory >> run_log.txt
    exit /b
)

:: Kiểm tra lại Docker daemon đã sẵn sàng
docker version --format "{{.Server.Version}}" >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] ❌ Docker daemon is NOT ready. Aborting. >> run_log.txt
    echo Docker daemon is NOT ready. >> run_log.txt
    exit /b
)

echo ==== [%date% %time%] - Stopping existing containers (if any)... ==== >> run_log.txt
docker-compose stop >> run_log.txt 2>&1

echo ==== [%date% %time%] - Removing old containers (if any)... ==== >> run_log.txt
docker-compose down --remove-orphans >> run_log.txt 2>&1

echo ==== [%date% %time%] - Building Docker containers... ==== >> run_log.txt
docker-compose build >> run_log.txt 2>&1

echo ==== [%date% %time%] - Starting Docker containers... ==== >> run_log.txt
docker-compose up -d >> run_log.txt 2>&1

if %ERRORLEVEL% NEQ 0 (
    echo [%date% %time%] ❌ Docker containers failed to start. >> run_log.txt
    echo Docker containers failed to start. Check run_log.txt for errors.
    exit /b
)

:: Ghi log từ container ETL
echo ==== [%date% %time%] - Logging projectbi-etl container... ==== >> run_log.txt
docker logs projectbi-etl >> run_log.txt 2>&1

echo ==== ✅ DONE at [%date% %time%] ==== >> run_log.txt
echo Docker containers started successfully.
echo Check run_log.txt for details.