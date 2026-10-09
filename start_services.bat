@echo off
set ROOT=%~dp0
set PY=%ROOT%.venv\Scripts\python.exe

start "CDC Consumer" cmd /k "cd /d "%ROOT%" && set PYTHONPATH=%ROOT%src && "%PY%" src\consumer\kafka_consumer.py"
timeout /t 2 /nobreak >nul

start "P2P Simulator" cmd /k "cd /d "%ROOT%" && set PYTHONPATH=%ROOT%src && "%PY%" src\generator\p2p_simulator.py"
timeout /t 2 /nobreak >nul

start "CDC API :8000" cmd /k "cd /d "%ROOT%" && set PYTHONPATH=%ROOT%src && "%PY%" -m uvicorn api.main:app --port 8000"
timeout /t 5 /nobreak >nul

start "CDC Dashboard :5173" cmd /k "cd /d "%ROOT%web" && npm run dev"

echo All services launched.
