import os
import uuid
import shutil
import datetime
from typing import Optional
from fastapi import APIRouter, File, UploadFile, Form, HTTPException
from fastapi.responses import FileResponse
import imagehash
from PIL import Image

from config import ASSETS_DIR, SUSPICIOUS_DIR
from database import load_db, save_db
from core.image_processing import compute_hashes, get_blur_index, check_screenshot_borders, orb_feature_match
from core.scoring import map_distance_to_confidence

router = APIRouter()

# ─── Register ────────────────────────────────────────────────────────────────

@router.post("/register")
async def register(file: UploadFile = File(...), asset_id: Optional[str] = Form(None)):
    if not asset_id:
        asset_id = str(uuid.uuid4())
    
    file_path = os.path.join(ASSETS_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        
    phash, dhash, ahash, chash, width, height = compute_hashes(file_path)
    blur_idx = float(get_blur_index(file_path))
    
    db = load_db()
    existing = next((i for i, a in enumerate(db["assets"]) if a["asset_id"] == asset_id), None)
    
    record = {
        "asset_id": asset_id,
        "phash": phash,
        "dhash": dhash,
        "ahash": ahash,
        "chash": chash,
        "width": width,
        "height": height,
        "blur_index": blur_idx,
        "file_path": file_path,
        "filename": file.filename,
        "registered_at": datetime.datetime.utcnow().isoformat() + "Z"
    }
    
    if existing is not None:
        db["assets"][existing] = record
    else:
        db["assets"].append(record)
        
    save_db(db)
    
    return {
        "status": "registered",
        "asset_id": asset_id,
        "hash": phash
    }

# ─── Scan ────────────────────────────────────────────────────────────────────

@router.post("/scan")
async def scan(file: UploadFile = File(...)):
    unique_id = str(uuid.uuid4())[:8]
    suspicious_path = os.path.join(SUSPICIOUS_DIR, f"susp_{unique_id}_{file.filename}")
    
    with open(suspicious_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    if os.path.getsize(suspicious_path) == 0:
        return {
            "match": False,
            "confidence": 0,
            "matched_asset": None,
            "reason": "Uploaded file is empty or corrupted.",
            "modifications": []
        }
        
    db = load_db()
    
    if not db.get("assets"):
        return {
            "match": False,
            "confidence": 0,
            "matched_asset": None,
            "reason": "No registered assets to compare against.",
            "modifications": []
        }

    # ─── 1. pHash Sieve (The "Sieve") ────────────────────────────────────────
    # Calculate hash for the new suspicious image
    try:
        susp_phash = str(imagehash.phash(Image.open(suspicious_path)))
    except Exception as e:
        return {"match": False, "confidence": 0, "reason": f"Error processing image: {e}", "modifications": []}

    # Calculate distances for ALL assets to find the most likely candidates
    candidates = []
    for asset in db["assets"]:
        asset_phash = imagehash.hex_to_hash(asset["phash"])
        target_phash = imagehash.hex_to_hash(susp_phash)
        distance = asset_phash - target_phash # Hamming distance
        candidates.append({
            "asset": asset,
            "distance": distance
        })

    # Sort by distance (lowest distance = most similar)
    candidates.sort(key=lambda x: x["distance"])
    
    # Take only the top 3 most similar assets to send to Gemini
    # This prevents the "Token Spike" by keeping the prompt small
    top_candidates = [c["asset"] for c in candidates[:3]]
    
    # If the closest match is extremely far away (e.g. > 30), it's probably not worth AI time
    if candidates[0]["distance"] > 35:
        # We still run Gemini just in case of heavy modification, 
        # but we've successfully filtered out the 97% that definitely don't match.
        pass

    # ─── 2. AI Verification ──────────────────────────────────────────────────
    from core.ai_engine import verify_semantic_match_with_gemini
    ai_result = await verify_semantic_match_with_gemini(suspicious_path, top_candidates)
    
    if ai_result:
        result = {
            "match": ai_result.get("match", False),
            "confidence": ai_result.get("similarity_score", 0),
            "matched_asset": ai_result.get("matched_asset_id"),
            "reason": ai_result.get("reason", "Analyzed via AI."),
            "modifications": ai_result.get("modifications", [])
        }
    else:
        result = {
            "match": False,
            "confidence": 0,
            "matched_asset": None,
            "reason": "AI Engine Unavailable or Failed.",
            "modifications": []
        }
    
    # ── Save to history ──────────────────────────────────────────────────────
    history_entry = {
        "id": str(uuid.uuid4()),
        "suspicious_file": os.path.basename(suspicious_path),
        "suspicious_path": suspicious_path,
        "original_filename": file.filename,
        "match": result["match"],
        "confidence": result["confidence"],
        "matched_asset": result["matched_asset"],
        "reason": result["reason"],
        "modifications": result["modifications"],
        "source": "manual",
        "post_url": None,
        "scanned_at": datetime.datetime.utcnow().isoformat() + "Z",
        "false_positive": False
    }
    db["history"].append(history_entry)
    save_db(db)
    
    result["scan_id"] = history_entry["id"]
    return result

# ─── History ─────────────────────────────────────────────────────────────────

@router.get("/history")
async def get_history():
    db = load_db()
    history = db.get("history", [])
    # Return newest first
    return sorted(history, key=lambda x: x.get("scanned_at", ""), reverse=True)

# ─── Assets List ─────────────────────────────────────────────────────────────

@router.get("/assets")
async def list_assets():
    db = load_db()
    return db.get("assets", [])

@router.delete("/assets/{asset_id}")
async def delete_asset(asset_id: str):
    db = load_db()
    idx = next((i for i, a in enumerate(db["assets"]) if a["asset_id"] == asset_id), None)
    if idx is None:
        raise HTTPException(status_code=404, detail="Asset not found")
    removed = db["assets"].pop(idx)
    # Remove file
    if os.path.exists(removed.get("file_path", "")):
        os.remove(removed["file_path"])
    save_db(db)
    return {"status": "deleted", "asset_id": asset_id}

# ─── Serve Images ────────────────────────────────────────────────────────────

@router.get("/assets/{asset_id}/image")
async def get_asset_image(asset_id: str):
    db = load_db()
    asset = next((a for a in db["assets"] if a["asset_id"] == asset_id), None)
    if not asset or not os.path.exists(asset.get("file_path", "")):
        raise HTTPException(status_code=404, detail="Asset image not found")
    return FileResponse(asset["file_path"])

@router.get("/suspicious/{filename}")
async def get_suspicious_image(filename: str):
    path = os.path.join(SUSPICIOUS_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Suspicious image not found")
    return FileResponse(path)

# ─── System Status ───────────────────────────────────────────────────────────

@router.get("/status")
async def get_status():
    db = load_db()
    history = db.get("history", [])
    assets = db.get("assets", [])
    matches = [h for h in history if h.get("match")]
    
    return {
        "engine": "online",
        "total_assets": len(assets),
        "total_scans": len(history),
        "total_matches": len(matches),
        "last_scan": history[-1]["scanned_at"] if history else None,
    }

# ─── Mark False Positive ────────────────────────────────────────────────────

@router.patch("/history/{scan_id}")
async def mark_false_positive(scan_id: str):
    db = load_db()
    entry = next((h for h in db["history"] if h["id"] == scan_id), None)
    if not entry:
        raise HTTPException(status_code=404, detail="Scan not found")
    entry["false_positive"] = True
    entry["match"] = False
    save_db(db)
    return {"status": "updated", "scan_id": scan_id}
