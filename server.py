import os
import time
import asyncio
import threading
from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, Request, HTTPException
from google.cloud import firestore

# Google Sheets API
from googleapiclient.discovery import build
from google.auth import default as google_auth_default


# =================================================
# FastAPI
# =================================================
app = FastAPI()


# =================================================
# 환경변수
# =================================================
API_KEY = os.environ.get("API_KEY", "")

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SHEET_LATEST = os.environ.get("SHEET_LATEST", "Latest")

FLUSH_INTERVAL_SEC = int(os.environ.get("FLUSH_INTERVAL_SEC", "300"))

MIN_VALUE = 0
MAX_VALUE = 99999


# =================================================
# Clients
# =================================================
db = firestore.Client(project=PROJECT_ID)


def _get_sheets_service():
    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# =================================================
# 서버 메모리 버퍼
# =================================================
BUFFER: Dict[str, Dict[str, Any]] = {}
DIRTY: Dict[str, bool] = {}
LOCK = threading.Lock()


# =================================================
# 유틸
# =================================================
def _valid_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        n = int(v)
    except Exception:
        return None
    return n if MIN_VALUE <= n <= MAX_VALUE else None


def _now_ts() -> int:
    return int(time.time())


# =================================================
# Sheets 관련
# =================================================
def _ensure_latest_header(service):
    header_range = f"{SHEET_LATEST}!A1:D1"
    res = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=header_range
    ).execute()
    if not res.get("values"):
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=header_range,
            valueInputOption="RAW",
            body={"values": [["pc_id", "A", "B", "server_at"]]}
        ).execute()


def _snapshot_dirty() -> List[Tuple[str, Dict[str, Any]]]:
    with LOCK:
        dirty_ids = [pc_id for pc_id, is_dirty in DIRTY.items() if is_dirty]
        items = []
        for pc_id in dirty_ids:
            items.append((pc_id, dict(BUFFER.get(pc_id, {}))))
            DIRTY[pc_id] = False
    return items


def _write_dirty_to_firestore(dirty_items):
    if not dirty_items:
        return

    batch = db.batch()
    col = db.collection("latest")

    for pc_id, v in dirty_items:
        ref = col.document(pc_id)

        payload = {
            "pc_id": pc_id,
            "server_at": firestore.SERVER_TIMESTAMP,
        }

        if v.get("A") is not None:
            payload["A"] = v["A"]
        if v.get("B") is not None:
            payload["B"] = v["B"]
        if v.get("updated_at") is not None:
            payload["updated_at"] = v["updated_at"]

        batch.set(ref, payload, merge=True)

    batch.commit()


def _read_firestore_latest_all():
    rows = []
    docs = db.collection("latest").stream()

    for doc in docs:
        d = doc.to_dict() or {}
        pc_id = d.get("pc_id") or doc.id

        server_at = d.get("server_at")
        if server_at:
            dt_utc = server_at.replace(tzinfo=timezone.utc)
            dt_kst = dt_utc.astimezone(timezone(timedelta(hours=9)))
            server_at_str = dt_kst.strftime("%Y-%m-%d %H:%M")
        else:
            server_at_str = ""

        rows.append([
            pc_id,
            d.get("A", ""),
            d.get("B", ""),
            server_at_str,
        ])

    rows.sort(key=lambda r: r[0])
    return rows


def _write_latest_sheet(service, rows):
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_LATEST}!A2:D",
        body={}
    ).execute()

    if not rows:
        return

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_LATEST}!A2:D{len(rows)+1}",
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()


# =================================================
# Flush Loop
# =================================================
async def _flush_loop():
    if not SPREADSHEET_ID:
        return

    service = _get_sheets_service()
    _ensure_latest_header(service)

    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SEC)

        dirty = _snapshot_dirty()
        _write_dirty_to_firestore(dirty)

        rows = _read_firestore_latest_all()
        _write_latest_sheet(service, rows)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(_flush_loop())


# =================================================
# API
# =================================================
@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/ingest")
async def ingest(request: Request):
    api_key = request.headers.get("x-api-key")
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401)

    data = await request.json()

    pc_id = str(data.get("pc_id", "")).strip().lower()
    if not pc_id:
        raise HTTPException(status_code=400, detail="missing pc_id")

    a = _valid_int(data.get("A"))
    b = _valid_int(data.get("B"))
    if a is None and b is None:
        return {"ok": True, "skipped": True}

    updated_at = int(data.get("updated_at", _now_ts()))

    with LOCK:
        cur = BUFFER.get(pc_id, {})
        changed = False

        if a is not None and cur.get("A") != a:
            cur["A"] = a
            changed = True
        if b is not None and cur.get("B") != b:
            cur["B"] = b
            changed = True

        cur["updated_at"] = updated_at
        BUFFER[pc_id] = cur

        if changed:
            DIRTY[pc_id] = True

    return {"ok": True}


# =================================================
# ✅ 스냅샷 API (하루 1번용)
# =================================================
@app.post("/snapshot")
async def snapshot(request: Request):
    api_key = request.headers.get("x-api-key")
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401)

    kst = timezone(timedelta(hours=9))
    today = datetime.now(kst).strftime("%Y-%m-%d")

    batch = db.batch()
    snap_col = db.collection("snapshots")

    count = 0
    for doc in db.collection("latest").stream():
        d = doc.to_dict() or {}
        pc_id = d.get("pc_id") or doc.id

        ref = snap_col.document(f"{today}_{pc_id}")
        batch.set(ref, {
            "date": today,
            "pc_id": pc_id,
            "A": d.get("A"),
            "B": d.get("B"),
            "created_at": firestore.SERVER_TIMESTAMP,
        })
        count += 1

    batch.commit()
    return {"ok": True, "date": today, "count": count}
