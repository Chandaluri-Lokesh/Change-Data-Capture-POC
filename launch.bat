@echo off
setlocal enabledelayedexpansion
title CDC Pipeline - Launcher
color 0A

echo.
echo ============================================================
echo   CDC Pipeline  --  Launcher
echo ============================================================
echo   Prerequisites: Kafka + Kafka Connect must be running in WSL.
echo   Run setup.bat first if this is a fresh machine.
echo ============================================================
echo.

set ROOT=%~dp0
set VENV=%ROOT%.venv
set PY=%VENV%\Scripts\python.exe
set MONGOD="C:\Program Files\MongoDB\Server\8.2\bin\mongod.exe"
set MONGO_PORT=27018
set MONGO_DATA=%ROOT%mongodb-data
set CONNECT_URL=http://localhost:8083
set UI_PORT=5000

:: ── Venv check ────────────────────────────────────────────────────────────────
if not exist "%VENV%\Scripts\activate.bat" (
    echo   [FAIL] Virtual environment not found. Run setup.bat first.
    goto :fail
)

:: ── Step 1: MongoDB ───────────────────────────────────────────────────────────
echo [1/6] Starting MongoDB replica set on port %MONGO_PORT%...
netstat -an | findstr ":%MONGO_PORT% " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo   [OK] MongoDB already running on :%MONGO_PORT%
    goto :mongo_done
)

if not exist %MONGOD% (
    echo   [FAIL] mongod.exe not found at %MONGOD%
    echo          Update MONGOD variable in launch.bat to your install path.
    goto :fail
)

start "MongoDB :27018" %MONGOD% ^
    --replSet rs0 ^
    --bind_ip_all ^
    --port %MONGO_PORT% ^
    --dbpath "%MONGO_DATA%" ^
    --logpath "%MONGO_DATA%\mongod.log" ^
    --logappend

echo   Waiting for MongoDB to boot...
timeout /t 4 /nobreak >nul
:mongo_done

:: ── Step 2: Replica Set Init ─────────────────────────────────────────────────
echo.
echo [2/6] Initialising MongoDB replica set (idempotent)...
"%PY%" scripts\init_replica_set.py
if errorlevel 1 (
    echo   [WARN] Could not init replica set -- ensure MongoDB started correctly.
)

:: ── Step 3: Kafka Broker (WSL) ────────────────────────────────────────────────
echo.
echo [3/6] Starting Kafka broker in WSL...
netstat -an | findstr ":9092 " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo   [OK] Kafka broker already running on :9092
    goto :connect_check
)
echo   Launching Kafka broker in WSL (new window)...
start "Kafka Broker (WSL)" wsl bash -c "cd ~/kafka_2.13-4.2.0 && bin/kafka-server-start.sh config/server.properties"
echo   Waiting for Kafka broker on :9092 (up to 60s)...
set KAFKA_TRIES=0
:wait_kafka
netstat -an | findstr ":9092 " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 goto :kafka_up
set /a KAFKA_TRIES+=1
if %KAFKA_TRIES% GEQ 30 (
    echo   [FAIL] Kafka broker did not start within 60s. Check the WSL window.
    goto :fail
)
timeout /t 2 /nobreak >nul
goto :wait_kafka
:kafka_up
echo   [OK] Kafka broker is up.

:connect_check
:: ── Step 3b: Kafka Connect (WSL) ──────────────────────────────────────────────
netstat -an | findstr ":8083 " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo   [OK] Kafka Connect already running on :8083
    goto :kafka_done
)
echo   Launching Kafka Connect in WSL (new window)...
start "Kafka Connect (WSL)" wsl bash -c "cd ~/kafka_2.13-4.2.0 && bin/connect-distributed.sh config/connect-distributed.properties"
echo   Waiting for Kafka Connect on :8083 (up to 90s)...
set CONNECT_TRIES=0
:wait_connect
netstat -an | findstr ":8083 " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 goto :connect_up
set /a CONNECT_TRIES+=1
if %CONNECT_TRIES% GEQ 45 (
    echo   [FAIL] Kafka Connect did not start within 90s. Check the WSL window.
    goto :fail
)
timeout /t 2 /nobreak >nul
goto :wait_connect
:connect_up
echo   [OK] Kafka Connect is up.
:kafka_done

:: ── Step 4: Register connectors ──────────────────────────────────────────────
echo.
echo [4/6] Registering Debezium connectors...
"%PY%" src\connector\manager.py
echo   [OK] Connector registration complete.

:: ── Step 5: Simulator + Consumer ─────────────────────────────────────────────
echo.
echo [5/6] Starting Simulator and Consumer...
start "CDC Simulator" cmd /k "cd /d %ROOT% && set PYTHONPATH=src && "%PY%" src\generator\simulator.py"
timeout /t 2 /nobreak >nul
start "CDC Consumer" cmd /k "cd /d %ROOT% && set PYTHONPATH=src && "%PY%" src\consumer\kafka_consumer.py"
timeout /t 2 /nobreak >nul
echo   [OK] Simulator and Consumer launched in separate windows.

:: ── Step 6: Dashboard UI ─────────────────────────────────────────────────────
echo.
echo [6/6] Starting Dashboard UI on port %UI_PORT%...
start "CDC Dashboard" cmd /k "cd /d %ROOT% && "%PY%" -m streamlit run src\ui\dashboard.py --server.port %UI_PORT% --server.headless true"
timeout /t 3 /nobreak >nul
timeout /t 3 /nobreak >nul
start http://localhost:%UI_PORT%
echo   [OK] Dashboard opened at http://localhost:%UI_PORT%

:: ── Done ─────────────────────────────────────────────────────────────────────
echo.
echo ============================================================
echo   Pipeline is running!
echo.
echo   Dashboard   : http://localhost:%UI_PORT%
echo   Consumer log: see "CDC Consumer" window
echo   Simulator   : see "CDC Simulator" window
echo.
echo   To stop: close all "CDC *" windows and stop MongoDB.
echo   To reset offsets: "%PY%" src\ops\offset_manager.py lag
echo   To run tests:     "%PY%" -m pytest tests\
echo   To verify:        "%PY%" scripts\verify.py
echo ============================================================
echo.
pause
exit /b 0

:fail
echo.
echo   Launch aborted. Fix the error above and try again.
echo.
pause
exit /b 1
