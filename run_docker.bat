@echo off
cd /d "E:\ProjectBI-" || (echo Failed to change directory >> run_log.txt & exit /b)

echo ==== %date% %time% - Building Docker containers... ==== >> run_log.txt
docker-compose build >> run_log.txt 2>&1

echo ==== %date% %time% - Starting Docker containers... ==== >> run_log.txt
docker-compose up >> run_log.txt 2>&1

echo ==== DONE at %date% %time% ==== >> run_log.txt
echo Docker containers started successfully.
echo Check run_log.txt for details.

