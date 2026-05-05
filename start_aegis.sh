#!/bin/bash

# AEGIS - Media Protection Suite
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

echo "Starting AEGIS from: $SCRIPT_DIR"
echo "Logs: $LOG_DIR"

VENV_ACTIVATE="$SCRIPT_DIR/.venv/bin/activate"

if [ ! -f "$VENV_ACTIVATE" ]; then
    echo "ERROR: .venv not found."
    echo "Run: python3 -m venv .venv && source .venv/bin/activate && pip install -r engine/requirements.txt -r crawler_pipeline/requirements.txt"
    exit 1
fi

# 1. Backend
echo "[1/5] Backend (Node/Express) :3000..."
cd "$SCRIPT_DIR/backend" && npm run dev > "$LOG_DIR/backend.log" 2>&1 &
BACKEND_PID=$!

sleep 1

# 2. Engine
echo "[2/5] AI Engine (FastAPI) :8000..."
(source "$VENV_ACTIVATE" && cd "$SCRIPT_DIR/engine" && uvicorn main:app --reload --port 8000) > "$LOG_DIR/engine.log" 2>&1 &
ENGINE_PID=$!

sleep 1

# 3. Frontend
echo "[3/5] Dashboard (Vite) :5173..."
cd "$SCRIPT_DIR/frontend" && npm run dev > "$LOG_DIR/frontend.log" 2>&1 &
FRONTEND_PID=$!

sleep 1

# 4. Crawler
echo "[4/5] Crawler (Social)..."
(source "$VENV_ACTIVATE" && cd "$SCRIPT_DIR/crawler_pipeline" && python main.py --source social --keywords 'sports,copyright' --limit 5) > "$LOG_DIR/crawler.log" 2>&1 &
CRAWLER_PID=$!

sleep 1

# 5. Stock Scraper
echo "[5/5] Stock Scraper..."
(source "$VENV_ACTIVATE" && cd "$SCRIPT_DIR/crawler_pipeline" && python main.py --source stock --keywords 'sports' --limit 3) > "$LOG_DIR/scraper.log" 2>&1 &
SCRAPER_PID=$!

echo ""
echo "===================================================="
echo "  AEGIS — All 5 Components Running (PIDs below)"
echo "===================================================="
echo "  Backend:       http://localhost:3000  [PID $BACKEND_PID]"
echo "  AI Engine:     http://localhost:8000/docs  [PID $ENGINE_PID]"
echo "  Dashboard:     http://localhost:5173  [PID $FRONTEND_PID]"
echo "  Crawler:       [PID $CRAWLER_PID]"
echo "  Stock Scraper: [PID $SCRAPER_PID]"
echo "===================================================="
echo "  Logs: $LOG_DIR/"
echo "===================================================="
echo ""
echo "Ctrl+C to stop all services."

# Trap Ctrl+C — kill all children
trap "echo 'Stopping all...'; kill $BACKEND_PID $ENGINE_PID $FRONTEND_PID $CRAWLER_PID $SCRAPER_PID 2>/dev/null; exit" SIGINT SIGTERM

wait
