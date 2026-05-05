@echo off
SETLOCAL EnableDelayedExpansion
SET "ROOT_DIR=%~dp0"
TITLE AEGIS - Media Protection Suite
echo Starting AEGIS System in %ROOT_DIR%...

:: 1. Start the Engine (Python FastAPI)
echo [1/4] Launching Engine (Python)...
start cmd /k "cd /d %ROOT_DIR%engine && ..\..\.venv\Scripts\activate && uvicorn main:app --reload --port 8000"

:: 2. Start the Backend (Node.js)
echo [2/4] Launching Backend (Node)...
start cmd /k "cd /d %ROOT_DIR%backend && npm run dev"

:: 3. Start the Frontend (React)
echo [3/4] Launching Dashboard UI...
start cmd /k "cd /d %ROOT_DIR%frontend && npm run dev"

:: 4. Setup Info
echo [4/4] AEGIS System Initialized.
echo.
echo Dashboard: http://localhost:5173
echo Backend API: http://localhost:3000/api
echo Engine API: http://localhost:8000/docs
echo.
echo To run a scan, use:
echo   cd crawler_pipeline
echo   python main.py --source social --keywords "sports" --limit 5
echo.
pause
