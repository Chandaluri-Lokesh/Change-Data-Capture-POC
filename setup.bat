@echo off
setlocal enabledelayedexpansion
title CDC Pipeline - First-Time Setup
color 0A

echo.
echo ============================================================
echo   CDC Pipeline  --  First-Time Setup
echo ============================================================
echo.

:: ── Python check ─────────────────────────────────────────────────────────────
echo [1/6] Checking Python version...
python --version >nul 2>&1
if errorlevel 1 (
    echo   [FAIL] Python not found. Install Python 3.9+ and add to PATH.
    goto :fail
)
for /f "tokens=2" %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo   [OK] Python %PYVER%

:: ── Venv + pip install ───────────────────────────────────────────────────────
echo.
echo [2/6] Creating virtual environment and installing dependencies...
if exist ".venv\Scripts\activate.bat" (
    echo   [OK] Virtual environment already exists, skipping.
    goto :venv_done
)
python -m venv .venv
if errorlevel 1 (
    echo   [FAIL] Could not create .venv.
    goto :fail
)
echo   [OK] Virtual environment created.
.venv\Scripts\python.exe -m pip install --quiet -r requirements.txt
if errorlevel 1 (
    echo   [FAIL] pip install failed. Check requirements.txt and your internet connection.
    goto :fail
)
echo   [OK] Dependencies installed.
:venv_done

:: ── MongoDB data directory ───────────────────────────────────────────────────
echo.
echo [3/6] Creating MongoDB replica-set data directory...
if not exist "mongodb-data" (
    mkdir mongodb-data
    echo   [OK] Created mongodb-data\
) else (
    echo   [OK] mongodb-data\ already exists.
)

:: ── .env check ───────────────────────────────────────────────────────────────
echo.
echo [4/6] Checking .env file...
if exist ".env" (
    echo   [OK] .env already exists.
    goto :env_done
)
if exist ".env.example" (
    copy ".env.example" ".env" >nul
    echo   [OK] Copied .env.example to .env  -- edit it if your ports differ.
    goto :env_done
)
echo   [WARN] No .env file found. Using defaults (see kafka_consumer.py).
:env_done

:: ── PostgreSQL DDL ───────────────────────────────────────────────────────────
echo.
echo [5/6] Running PostgreSQL schema initialization...

:: Try to locate psql.exe
set PSQL=
for %%d in (
    "C:\Program Files\PostgreSQL\17\bin\psql.exe"
    "C:\Program Files\PostgreSQL\16\bin\psql.exe"
    "C:\Program Files\PostgreSQL\15\bin\psql.exe"
    "C:\Program Files\PostgreSQL\14\bin\psql.exe"
) do (
    if exist %%d ( set PSQL=%%~d )
)

if "%PSQL%"=="" (
    where psql >nul 2>&1
    if not errorlevel 1 ( set PSQL=psql )
)

if "%PSQL%"=="" (
    echo   [WARN] psql.exe not found. Run these manually after Postgres is installed:
    echo          psql -U postgres -d analytics -f init\postgres\01_analytics.sql
    echo          psql -U postgres -d finance   -f init\postgres\02_finance.sql
    goto :skip_ddl
)

:: Check that the databases exist; create them if not
echo   Creating databases if needed...
"%PSQL%" -U postgres -c "CREATE DATABASE analytics;" 2>nul
"%PSQL%" -U postgres -c "CREATE DATABASE finance;"   2>nul

echo   Applying analytics schema...
"%PSQL%" -U postgres -d analytics -f "init\postgres\01_analytics.sql"
if errorlevel 1 ( echo   [WARN] analytics schema may already exist -- continuing. )
"%PSQL%" -U postgres -d analytics -f "init\postgres\03_metrics.sql"
if errorlevel 1 ( echo   [WARN] metrics schema may already exist -- continuing. )

echo   Applying finance schema...
"%PSQL%" -U postgres -d finance -f "init\postgres\02_finance.sql"
if errorlevel 1 ( echo   [WARN] finance schema may already exist -- continuing. )

echo   [OK] PostgreSQL schemas applied.
:skip_ddl

:: ── WSL / Kafka reminder ─────────────────────────────────────────────────────
echo.
echo [6/6] WSL / Kafka setup reminder...
echo.
echo   Kafka and Kafka Connect run inside WSL and must be set up once:
echo.
echo   Step A  --  In your WSL terminal, run the Debezium plugin setup:
echo               bash setup_ubuntu_debezium.sh
echo.
echo   Step B  --  Start Kafka (KRaft, no ZooKeeper):
echo               cd ~/kafka_2.13-4.2.0
echo               bin/kafka-server-start.sh config/kraft/server.properties
echo.
echo   Step C  --  Start Kafka Connect:
echo               bin/connect-distributed.sh config/connect-distributed.properties
echo.
echo   These only need to be done once per WSL session.
echo.

:: ── Done ─────────────────────────────────────────────────────────────────────
echo ============================================================
echo   Setup complete!
echo   Run  launch.bat  to start the full pipeline.
echo ============================================================
echo.
pause
exit /b 0

:fail
echo.
echo   Setup failed. Fix the errors above and re-run setup.bat.
echo.
pause
exit /b 1
