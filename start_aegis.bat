@echo off
TITLE AEGIS - Media Protection Suite
echo Starting AEGIS System...

:: 1. Start the Engine (FastAPI)
echo [1/3] Launching Core Engine...
start cmd /k "cd engine && uvicorn main:app --reload"

:: 2. Start the Frontend (React)
echo [2/3] Launching Dashboard UI...
start cmd /k "cd frontend && npm run dev"

:: 3. Ready for Crawler
echo [3/3] AEGIS is ready.
echo To start scanning, open a new terminal and run:
echo cd crawler_pipeline && python main.py --source social --keywords "sports" --limit 5

echo.
echo Dashboard: http://localhost:5173
echo Engine API: http://localhost:8000/docs
pause
