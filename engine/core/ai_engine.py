import os
import json
import base64
import asyncio
import httpx
from PIL import Image
from io import BytesIO
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
NVIDIA_API_KEY = os.environ.get("NVIDIA_API_KEY")

gemini_semaphore = asyncio.Semaphore(2)

PROMPT = """
You are an expert copyright and forensic media AI.
Determine if the Suspicious Image was derived from ANY of the Official Images.

FORENSIC RULES:
1. SEMANTIC MATCH: Ignore rotations (90/180/270 deg), grayscale filters, inversions, crops, or scribbles. If the core scene or textures are the same, it is a MATCH.
2. ALTERATION SCORE: Measures physical 'Damage' (0-100%). Rotations, filters, or scribbles should result in a HIGH score (70-100%).
3. SIMILARITY SCORE: Measures 'Certainty' (0-100%) that these share the same original source file.

Respond ONLY with valid JSON:
{
  "match": <bool>,
  "similarity_score": <0-100>,
  "alteration_score": <0-100>,
  "matched_asset_id": "<id or null>",
  "reason": "<str>",
  "modifications": ["<list every change: 'rotated', 'grayscale', etc.>"]
}
"""

def image_to_base64_optimized(img: Image.Image) -> str:
    img.thumbnail((768, 768))
    buffer = BytesIO()
    img.save(buffer, format="JPEG", quality=80)
    return base64.b64encode(buffer.getvalue()).decode("utf-8")

# ─── Gemini Engine ────────────────────────────────────────────────────────────

async def _try_gemini(suspicious_path: str, db_assets: list) -> dict | None:
    if not GEMINI_API_KEY: return None
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            client = genai.Client(api_key=GEMINI_API_KEY)
            contents = [PROMPT]
            susp_img = await asyncio.to_thread(Image.open, suspicious_path)
            contents.append("Suspicious Image:")
            contents.append(susp_img)
            for asset in db_assets:
                try:
                    img = await asyncio.to_thread(Image.open, asset["file_path"])
                    contents.append(f"Asset ID: {asset['asset_id']}")
                    contents.append(img)
                except Exception: pass

            response = await client.aio.models.generate_content(
                model="gemini-2.5-flash",
                contents=contents,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            if response.candidates and response.candidates[0].content.parts:
                text = response.candidates[0].content.parts[0].text.strip()
                return json.loads(text.replace("```json", "").replace("```", ""))
        except Exception:
            if attempt < max_attempts - 1:
                await asyncio.sleep(2 * (attempt + 1))
    return None

# ─── NVIDIA NIM Engine ────────────────────────────────────────────────────────

async def _try_nvidia_nim(suspicious_path: str, db_assets: list) -> dict | None:
    if not NVIDIA_API_KEY: return None
    
    try:
        susp_img = await asyncio.to_thread(Image.open, suspicious_path)
        susp_b64 = await asyncio.to_thread(image_to_base64_optimized, susp_img)
        
        content_parts = [
            {"type": "text", "text": f"{PROMPT}\nSuspicious Image follows."},
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{susp_b64}"}}
        ]
        
        for asset in db_assets:
            try:
                img = await asyncio.to_thread(Image.open, asset["file_path"])
                b64 = await asyncio.to_thread(image_to_base64_optimized, img)
                content_parts.append({"type": "text", "text": f"Official Asset: {asset['asset_id']}"})
                content_parts.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}})
            except Exception: pass
            
    except Exception as e:
        print(f"[NVIDIA] Prep Error: {e}")
        return None

    async with httpx.AsyncClient(timeout=60) as client:
        # Using the exact model and parameters from your snippet
        model = "mistralai/mistral-large-3-675b-instruct-2512"
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                print(f"[NVIDIA NIM] Requesting {model} (Attempt {attempt+1}/3)")
                resp = await client.post(
                    "https://integrate.api.nvidia.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {NVIDIA_API_KEY}",
                        "Accept": "application/json"
                    },
                    json={
                        "model": model,
                        "messages": [{"role": "user", "content": content_parts}],
                        "max_tokens": 2048,
                        "temperature": 0.15,
                        "top_p": 1.0,
                        "stream": False
                    }
                )
                
                if resp.status_code == 200:
                    data = resp.json()
                    text = data["choices"][0]["message"]["content"].strip()
                    res = json.loads(text.replace("```json", "").replace("```", ""))
                    res["_engine"] = f"nvidia/{model}"
                    return res
                else:
                    print(f"[NVIDIA] Status {resp.status_code}. Trying next model...")
                    # Automatic fallback to Pixtral-12B (Vision optimized) if Large-3 fails or doesn't support vision
                    model = "mistralai/pixtral-12b"
            except Exception as e:
                print(f"[NVIDIA] Error: {e}")
                model = "mistralai/pixtral-12b"
    return None

# ─── Main Entry ───────────────────────────────────────────────────────────────

async def verify_semantic_match_with_gemini(suspicious_path: str, db_assets: list) -> dict | None:
    async with gemini_semaphore:
        res = await _try_gemini(suspicious_path, db_assets)
        if res: 
            res["_engine"] = "gemini-2.5-flash"
            print(f"🚀 Engine: {res['_engine']}")
            return res
            
        print("[AEGIS] Falling back to NVIDIA NIM...")
        res = await _try_nvidia_nim(suspicious_path, db_assets)
        if res:
            print(f"🚀 Engine: {res['_engine']}")
            return res
            
        return None
