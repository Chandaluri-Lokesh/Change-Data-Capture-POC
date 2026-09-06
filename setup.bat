@echo off
setlocal enabledelayedexpansion
title P2P CDC Pipeline - First-Time Setup
color 0A

echo.
echo ============================================================
echo   P2P CDC Pipeline  --  First-Time Setup
echo ============================================================
echo   Installs Python deps, sets up Postgres schema, installs
echo   the React frontend, and reminds you about WSL + Neo4j.
echo ============================================================
echo.

set ROOT=%~dp0
set VENV=%ROOT%.venv
set PY=%VENV%\Scripts\python.exe
set NPM=npm

:: ── Step 1: Python ────────────────────────────────────────────────────────
echo [1/7] Checking Python version...
python --version >nul 2>&1
if errorlevel 1 (
    echo   [FAIL] Python not found. Install Python 3.11+ and add to PATH.
    goto :fail
)
for /f "tokens=2" %%v in ('python --version 2^>^&1') do set PYVER=%%v
echo   [OK] Python %PYVER%

:: ── Step 2: Virtual environment + pip ────────────────────────────────────
echo.
echo [2/7] Creating virtual environment and installing Python dependencies...
if exist "%VENV%\Scripts\activate.bat" (
    echo   [OK] Virtual environment already exists.
    goto :pip_install
)
python -m venv .venv
if errorlevel 1 (
    echo   [FAIL] Could not create .venv.
    goto :fail
)
echo   [OK] Virtual environment created.

:pip_install
echo   Installing from requirements.txt...
"%PY%" -m pip install --quiet --upgrade pip
"%PY%" -m pip install --quiet -r requirements.txt
if errorlevel 1 (
    echo   [FAIL] pip install failed. Check requirements.txt and internet connection.
    goto :fail
)
echo   [OK] Python dependencies installed.

:: ── Step 3: MongoDB data directory ───────────────────────────────────────
echo.
echo [3/7] Creating MongoDB data directory...
if not exist "mongodb-data" (
    mkdir mongodb-data
    echo   [OK] Created mongodb-data\
) else (
    echo   [OK] mongodb-data\ already exists.
)

:: ── Step 4: .env check ───────────────────────────────────────────────────
echo.
echo [4/7] Checking .env file...
if exist ".env" (
    echo   [OK] .env found.
) else (
    echo   [WARN] .env not found. Creating from defaults...
    (
        echo MONGO_URI=mongodb://localhost:27018/?replicaSet=rs0
        echo MONGO_DIRECT_URI=mongodb://localhost:27018/?directConnection=true
        echo MONGO_RS_HOST=localhost:27018
        echo KAFKA_BROKER_URL=localhost:9092
        echo KAFKA_CONNECT_REST_URL=http://localhost:8083
        echo POSTGRES_ANALYTICS_HOST=localhost
        echo POSTGRES_ANALYTICS_PORT=5432
        echo POSTGRES_ANALYTICS_USER=postgres
        echo POSTGRES_ANALYTICS_PASSWORD=postgres
        echo POSTGRES_ANALYTICS_DB=analytics
        echo NEO4J_URI=bolt://localhost:7687
        echo NEO4J_USER=neo4j
        echo NEO4J_PASSWORD=password
        echo NEO4J_DATABASE=DBMD-Minor
    ) > .env
    echo   [OK] Created .env with defaults -- edit NEO4J_PASSWORD and NEO4J_DATABASE to match your Neo4j Desktop DBMS.
)

:: ── Step 5: PostgreSQL DDL ────────────────────────────────────────────────
echo.
echo [5/7] Running PostgreSQL schema initialisation...

set PSQL=
for %%d in (
    "C:\Program Files\PostgreSQL\17\bin\psql.exe"
    "C:\Program Files\PostgreSQL\16\bin\psql.exe"
    "C:\Program Files\PostgreSQL\15\bin\psql.exe"
    "C:\Program Files\PostgreSQL\14\bin\psql.exe"
) do (
    if exist %%d set PSQL=%%~d
)
if "%PSQL%"=="" (
    where psql >nul 2>&1
    if not errorlevel 1 set PSQL=psql
)

if "%PSQL%"=="" (
    echo   [WARN] psql.exe not found. Run these manually once Postgres is installed:
    echo.
    echo          psql -U postgres -c "CREATE DATABASE analytics;"
    echo          psql -U postgres -d analytics -f init\postgres\01_analytics.sql
    echo          psql -U postgres -d analytics -f init\postgres\04_p2p.sql
    echo.
    goto :skip_ddl
)

echo   Creating analytics database if not exists...
"%PSQL%" -U postgres -c "CREATE DATABASE analytics;" 2>nul

echo   Applying legacy metrics schema (01_analytics.sql)...
"%PSQL%" -U postgres -d analytics -f "init\postgres\01_analytics.sql" >nul 2>&1

echo   Applying P2P schema (04_p2p.sql)...
"%PSQL%" -U postgres -d analytics -f "init\postgres\04_p2p.sql"
if errorlevel 1 (
    echo   [WARN] P2P DDL reported an error -- tables may already exist, continuing.
)

echo   [OK] PostgreSQL schemas applied to analytics database.
echo.
echo   NOTE: Neo4j constraints are applied automatically when the consumer
echo         starts (neo4j_writer.apply_constraints). You can also run them
echo         manually from Neo4j Browser:
echo           init\neo4j\constraints.cypher
:skip_ddl

:: ── Step 6: Node.js + React frontend ────────────────────────────────────
echo.
echo [6/7] Installing React frontend dependencies...

where node >nul 2>&1
if errorlevel 1 (
    echo   [WARN] Node.js not found. Install from https://nodejs.org (LTS recommended).
    echo          After installing, re-run this step:  cd web ^&^& npm install
    goto :skip_npm
)

for /f "tokens=*" %%v in ('node --version 2^>^&1') do set NODEVER=%%v
echo   [OK] Node.js %NODEVER%

if not exist "web\node_modules" (
    echo   Running npm install in web\...
    pushd web
    call npm install --silent
    if errorlevel 1 (
        echo   [FAIL] npm install failed. Check web\package.json and internet connection.
        popd
        goto :fail
    )
    popd
    echo   [OK] Frontend dependencies installed.
) else (
    echo   [OK] web\node_modules already exists.
)
:skip_npm

:: ── Step 7: WSL + Neo4j reminders ────────────────────────────────────────
echo.
echo [7/7] WSL / Kafka / Neo4j setup reminders...
echo.
echo   ── WSL (Kafka + Kafka Connect) ────────────────────────────────────
echo   Kafka and Debezium run inside WSL. One-time setup per machine:
echo.
echo   A)  Install Debezium MongoDB plugin (in WSL terminal):
echo           bash setup_ubuntu_debezium.sh
echo.
echo   B)  Enable WSL mirrored networking (run once in PowerShell as Admin):
echo           Add "[wsl2]" and "networkingMode=mirrored" to ~/.wslconfig
echo           Then: wsl --shutdown
echo.
echo   C)  Start Kafka (KRaft, no ZooKeeper):
echo           /home/lokesh/kafka/bin/kafka-server-start.sh /home/lokesh/kafka/config/server.properties
echo.
echo   D)  Start Kafka Connect with Debezium plugin:
echo           /home/lokesh/kafka/bin/connect-distributed.sh /home/lokesh/kafka/config/connect-distributed.properties
echo.
echo   ── Neo4j (Local Desktop) ───────────────────────────────────────────
echo   1)  Open Neo4j Desktop and start your local DBMS.
echo   2)  Default bolt port: 7687. Update NEO4J_PASSWORD in .env.
echo   3)  Constraints are applied automatically on first consumer start.
echo.

:: ── Done ─────────────────────────────────────────────────────────────────
echo ============================================================
echo   Setup complete!
echo.
echo   Next step:  launch.bat
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
