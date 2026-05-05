#!/bin/bash

# AEGIS - Media Protection Suite
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Starting AEGIS System from: $SCRIPT_DIR"

VENV_ACTIVATE="$SCRIPT_DIR/.venv/bin/activate"

if [ ! -f "$VENV_ACTIVATE" ]; then
    echo "ERROR: .venv not found."
    echo "Run: python3 -m venv .venv && source .venv/bin/activate && pip install -r engine/requirements.txt -r crawler_pipeline/requirements.txt"
    exit 1
fi

# 1. Backend (Node/Express + Supabase Vault)
echo "[1/5] Launching Backend (Node/Express) on :3000..."
konsole --new-tab -p tabtitle="AEGIS Backend" -e bash -c "cd '$SCRIPT_DIR/backend' && npm run dev; exec bash" &

sleep 1

# 2. Engine (FastAPI + Gemini AI)
echo "[2/5] Launching AI Engine (FastAPI) on :8000..."
konsole --new-tab -p tabtitle="AEGIS Engine" -e bash -c "source '$VENV_ACTIVATE' && cd '$SCRIPT_DIR/engine' && uvicorn main:app --reload --port 8000; exec bash" &

sleep 1

# 3. Frontend (React Dashboard)
echo "[3/5] Launching Dashboard UI (Vite) on :5173..."
konsole --new-tab -p tabtitle="AEGIS Frontend" -e bash -c "cd '$SCRIPT_DIR/frontend' && npm run dev; exec bash" &

sleep 1

# 4. Crawler (Social — Reddit/Twitter)
echo "[4/5] Launching Crawler Pipeline (Social)..."
konsole --new-tab -p tabtitle="AEGIS Crawler" -e bash -c "source '$VENV_ACTIVATE' && cd '$SCRIPT_DIR/crawler_pipeline' && python main.py --source social --keywords 'sports,copyright' --limit 5; exec bash" &

sleep 1

# 5. Stock Scraper (Asset Seeding)
echo "[5/5] Launching Stock Scraper (Asset Seeding)..."
konsole --new-tab -p tabtitle="AEGIS Stock Scraper" -e bash -c "source '$VENV_ACTIVATE' && cd '$SCRIPT_DIR/crawler_pipeline' && python main.py --source stock --keywords 'sports' --limit 3; exec bash" &

# Done
echo ""
echo "===================================================="
echo "  AEGIS — All 5 Components Booting"
echo "===================================================="
echo "  Backend (Vault):  http://localhost:3000"
echo "  AI Engine:        http://localhost:8000/docs"
echo "  Dashboard:        http://localhost:5173"
echo "  Crawler:          [Konsole tab]"
echo "  Stock Scraper:    [Konsole tab]"
echo "===================================================="
echo ""
echo "Press Enter to exit launcher (tabs stay open)..."
read
