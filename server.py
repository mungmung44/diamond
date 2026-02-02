import os
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request, HTTPException
from google.cloud import firestore

app = FastAPI()

API_KEY = os.environ.get("API_KEY", "")
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")

MIN_VALUE = 0
MAX_VALUE = 99999

db = firestore.Client(project=PROJECT_ID)

@app.get("/")
def health():
    return {"status": "ok"}

def _valid_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        n = int(v)
    except Exception:
        return None
    return n if MIN_VALUE <= n <= MAX_VALUE else None

@app.post("/ingest")
async def ingest(request: Request):
    api_key = request.headers.get("x-api-key")
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

    data: Dict[str, Any] = await request.json()

    # ✅ pc_id 통일: 대문자/공백 들어와도 소문자로 정리
    pc_id_raw = str(data.get("pc_id", "")).strip()
    if not pc_id_raw:
        raise HTTPException(status_code=400, detail="missing pc_id")
    pc_id = pc_id_raw.lower()  # 예: "PC_001" -> "pc_001"

    a = _valid_int(data.get("A"))
    b = _valid_int(data.get("B"))

    # 둘 다 None이면(가려짐 등) 최신값 덮어쓰기 방지
    if a is None and b is None:
        return {"ok": True, "skipped": True}

    updated_at = data.get("updated_at")
    try:
        updated_at = int(updated_at) if updated_at is not None else int(time.time())
    except Exception:
        updated_at = int(time.time())

    now = int(time.time())

    payload = {
        "pc_id": pc_id,
        "A": a,
        "B": b,
        "updated_at": updated_at,
        "server_at": now,
    }

    # ✅ 최신값 저장: latest/{pc_id}
    db.collection("latest").document(pc_id).set(payload, merge=True)

    return {"ok": True}
