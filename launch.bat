@echo off
setlocal enabledelayedexpansion
title P2P CDC Pipeline - Launcher
color 0A

echo.
echo ============================================================
echo   P2P CDC Pipeline  --  Launcher
echo ============================================================
echo   Prerequisites: Kafka + Kafka Connect running in WSL,
echo                  Neo4j Desktop DBMS started.
echo   Run setup.bat first if this is a fresh machine.
echo ============================================================
echo.

set ROOT=%~dp0
set VENV=%ROOT%.venv
set PY=%VENV%\Scripts\python.exe
set MONGOD="C:\Program Files\MongoDB\Server\8.2\bin\mongod.exe"
set MONGO_PORT=27018
set MONGO_DATA=%ROOT%mongodb-data
set API_PORT=8000
set UI_PORT=5173

:: ── Venv check ────────────────────────────────────────────────────────────
if not exist "%VENV%\Scripts\activate.bat" (
    echo   [FAIL] Virtual environment not found. Run setup.bat first.
    goto :fail
)

:: ── Step 1: MongoDB ───────────────────────────────────────────────────────
echo [1/7] Starting MongoDB replica set on port %MONGO_PORT%...
netstat -an | findstr ":%MONGO_PORT% " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo   [OK] MongoDB already running on :%MONGO_PORT%
    goto :mongo_done
)
if not exist %MONGOD% (
    echo   [FAIL] mongod.exe not found at %MONGOD%
    echo          Update the MONGOD variable in launch.bat to your install path.
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

:: ── Step 2: Replica set init ──────────────────────────────────────────────
echo.
echo [2/7] Initialising MongoDB replica set (idempotent)...
set PYTHONPATH=%ROOT%src
"%PY%" -c "from pymongo import MongoClient; from pymongo.errors import OperationFailure; c=MongoClient('mongodb://localhost:%MONGO_PORT%/?directConnection=true',serverSelectionTimeoutMS=5000); [c.admin.command('replSetInitiate',{'_id':'rs0','members':[{'_id':0,'host':'localhost:%MONGO_PORT%'}]}) if not c.admin.command('replSetGetStatus').get('ok') else None]" 2>nul
echo   [OK] Replica set ready.

:: ── Step 3: Kafka broker (WSL) ────────────────────────────────────────────
echo.
echo [3/7] Checking Kafka broker (:9092)...
netstat -an | findstr ":9092 " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo   [OK] Kafka broker already running.
    goto :connect_check
)
echo   Launching Kafka broker in WSL (new window)...
set MSYS_NO_PATHCONV=1
start "Kafka Broker (WSL)" wsl -e bash -c "/home/lokesh/kafka/bin/kafka-server-start.sh /home/lokesh/kafka/config/server.properties"
echo   Waiting for Kafka on :9092 (up to 60s)...
set KAFKA_TRIES=0
:wait_kafka
netstat -an | findstr ":9092 " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 goto :kafka_up
set /a KAFKA_TRIES+=1
if %KAFKA_TRIES% GEQ 30 (
    echo   [FAIL] Kafka did not start within 60s. Check the WSL window.
    goto :fail
)
timeout /t 2 /nobreak >nul
goto :wait_kafka
:kafka_up
echo   [OK] Kafka broker is up.

:connect_check
:: ── Step 3b: Kafka Connect (WSL) ─────────────────────────────────────────
netstat -an | findstr ":8083 " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo   [OK] Kafka Connect already running on :8083
    goto :kafka_done
)
echo   Launching Kafka Connect in WSL (new window)...
start "Kafka Connect (WSL)" wsl -e bash -c "/home/lokesh/kafka/bin/connect-distributed.sh /home/lokesh/kafka/config/connect-distributed.properties"
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

:: ── Step 4: Register P2P connector ───────────────────────────────────────
echo.
echo [4/7] Registering Debezium P2P connector...
set PYTHONPATH=%ROOT%src
"%PY%" src\connector\manager.py
if errorlevel 1 (
    echo   [WARN] Connector registration returned an error -- check output above.
) else (
    echo   [OK] p2p-connector registered.
)

:: ── Step 5: Consumer + Simulator ─────────────────────────────────────────
echo.
echo [5/7] Starting CDC Consumer and P2P Simulator...
start "CDC Consumer" cmd /k "cd /d %ROOT% && set PYTHONPATH=src && "%PY%" src\consumer\kafka_consumer.py"
timeout /t 2 /nobreak >nul
start "P2P Simulator" cmd /k "cd /d %ROOT% && set PYTHONPATH=src && "%PY%" src\generator\p2p_simulator.py"
timeout /t 2 /nobreak >nul
echo   [OK] Consumer and Simulator launched in separate windows.

:: ── Step 6: FastAPI backend ───────────────────────────────────────────────
echo.
echo [6/7] Starting FastAPI backend on port %API_PORT%...
netstat -an | findstr ":%API_PORT% " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo   [OK] Something already listening on :%API_PORT% -- skipping.
    goto :api_done
)
start "CDC API :%API_PORT%" cmd /k "cd /d %ROOT% && set PYTHONPATH=src && "%PY%" -m uvicorn api.main:app --port %API_PORT%"
echo   Waiting for API on :%API_PORT% (up to 30s)...
set API_TRIES=0
:wait_api
netstat -an | findstr ":%API_PORT% " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 goto :api_up
set /a API_TRIES+=1
if %API_TRIES% GEQ 15 (
    echo   [WARN] API did not bind in 30s -- check the "CDC API" window.
    goto :api_done
)
timeout /t 2 /nobreak >nul
goto :wait_api
:api_up
echo   [OK] FastAPI backend running at http://localhost:%API_PORT%
:api_done

:: ── Step 7: React dev server ──────────────────────────────────────────────
echo.
echo [7/7] Starting React dev server on port %UI_PORT%...

if not exist "web\node_modules" (
    echo   [WARN] web\node_modules not found. Running npm install...
    pushd web
    call npm install --silent
    popd
)

netstat -an | findstr ":%UI_PORT% " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 (
    echo   [OK] Something already listening on :%UI_PORT%
    goto :ui_done
)
start "CDC Dashboard :%UI_PORT%" cmd /k "cd /d %ROOT%web && npm run dev"
echo   Waiting for React dev server on :%UI_PORT% (up to 20s)...
set UI_TRIES=0
:wait_ui
netstat -an | findstr ":%UI_PORT% " | findstr "LISTENING" >nul 2>&1
if not errorlevel 1 goto :ui_up
set /a UI_TRIES+=1
if %UI_TRIES% GEQ 10 (
    echo   [WARN] React dev server did not bind in 20s -- check the "CDC Dashboard" window.
    goto :ui_done
)
timeout /t 2 /nobreak >nul
goto :wait_ui
:ui_up
timeout /t 1 /nobreak >nul
start http://localhost:%UI_PORT%
echo   [OK] Dashboard opened at http://localhost:%UI_PORT%
:ui_done

:: ── Done ─────────────────────────────────────────────────────────────────
echo.
echo ============================================================
echo   Pipeline is running!
echo.
echo   Dashboard    : http://localhost:%UI_PORT%
echo   API (Swagger): http://localhost:%API_PORT%/docs
echo.
echo   Windows                       WSL
echo   ───────────────────────────   ────────────────────────────
echo   MongoDB        :27018         Kafka Broker      :9092
echo   Neo4j Desktop  :7687          Kafka Connect     :8083
echo   FastAPI        :%API_PORT%         Debezium MongoDB
echo   React dev      :%UI_PORT%
echo.
echo   Useful commands (run in a new terminal):
echo     Verify E2E:   set PYTHONPATH=src ^& "%PY%" scripts\verify.py
echo     Stop all:     close "CDC *" windows; stop MongoDB + Neo4j
echo.
echo   WSL networking: requires networkingMode=mirrored in ~/.wslconfig
echo   Neo4j: start manually from Neo4j Desktop (DBMD-Minor database).
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
