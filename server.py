import os
import time
import asyncio
import threading
from typing import Any, Dict, Optional, Tuple, List
from datetime import timezone, timedelta  # ✅ 추가

from fastapi import FastAPI, Request, HTTPException

from google.cloud import firestore

# Google Sheets API
from googleapiclient.discovery import build
from google.auth import default as google_auth_default


app = FastAPI()

# ---------------------------
# 환경변수
# ---------------------------
API_KEY = os.environ.get("API_KEY", "")

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")  # 필수
SHEET_LATEST = os.environ.get("SHEET_LATEST", "Latest")

FLUSH_INTERVAL_SEC = int(os.environ.get("FLUSH_INTERVAL_SEC", "300"))  # 기본 5분

MIN_VALUE = 0
MAX_VALUE = 99999


# ---------------------------
# Clients
# ---------------------------
db = firestore.Client(project=PROJECT_ID)


def _get_sheets_service():
    creds, _ = google_auth_default(
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# ---------------------------
# 서버 메모리 버퍼
# ---------------------------
BUFFER: Dict[str, Dict[str, Any]] = {}
DIRTY: Dict[str, bool] = {}
LOCK = threading.Lock()


# ---------------------------
# 유틸
# ---------------------------
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


def _ensure_latest_header(service):
    header_range = f"{SHEET_LATEST}!A1:D1"
    res = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=header_range
    ).execute()
    values = res.get("values", [])
    if not values:
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=header_range,
            valueInputOption="RAW",
            body={"values": [["pc_id", "A", "B", "server_at"]]}
        ).execute()


def _snapshot_dirty() -> List[Tuple[str, Dict[str, Any]]]:
    with LOCK:
        dirty_ids = [pc_id for pc_id, is_dirty in DIRTY.items() if is_dirty]
        items: List[Tuple[str, Dict[str, Any]]] = []
        for pc_id in dirty_ids:
            items.append((pc_id, dict(BUFFER.get(pc_id, {}))))
            DIRTY[pc_id] = False
    return items


def _write_dirty_to_firestore(dirty_items: List[Tuple[str, Dict[str, Any]]]):
    if not dirty_items:
        return

    batch = db.batch()
    col = db.collection("latest")

    for pc_id, v in dirty_items:
        doc_ref = col.document(pc_id)

        payload: Dict[str, Any] = {
            "pc_id": pc_id,
            "server_at": firestore.SERVER_TIMESTAMP,
        }

        if v.get("A") is not None:
            payload["A"] = v["A"]
        if v.get("B") is not None:
            payload["B"] = v["B"]
        if v.get("updated_at") is not None:
            payload["updated_at"] = v["updated_at"]

        batch.set(doc_ref, payload, merge=True)

    batch.commit()


def _read_firestore_latest_all() -> List[List[Any]]:
    """
    Firestore latest 전체 읽어서 시트용 rows 생성
    server_at → UTC+9, YYYY-MM-DD HH:MM 형식
    """
    rows: List[List[Any]] = []
    docs = db.collection("latest").stream()

    for doc in docs:
        d = doc.to_dict() or {}
        pc_id = d.get("pc_id") or doc.id
        a = d.get("A")
        b = d.get("B")

        server_at = d.get("server_at")
        if server_at is None:
            server_at_str = ""
        else:
            try:
                # Firestore Timestamp → UTC datetime
                dt_utc = server_at.replace(tzinfo=timezone.utc)

                # UTC+9 (한국 시간)
                dt_kst = dt_utc.astimezone(
                    timezone(timedelta(hours=9))
                )

                # 보기 좋은 포맷
                server_at_str = dt_kst.strftime("%Y-%m-%d %H:%M")
            except Exception:
                server_at_str = ""

        rows.append([
            str(pc_id),
            a if a is not None else "",
            b if b is not None else "",
            server_at_str,
        ])

    rows.sort(key=lambda r: r[0])
    return rows


def _write_latest_sheet(service, rows: List[List[Any]]):
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_LATEST}!A2:D",
        body={}
    ).execute()

    if not rows:
        return

    target_range = f"{SHEET_LATEST}!A2:D{len(rows) + 1}"
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=target_range,
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()


async def _flush_loop():
    if not SPREADSHEET_ID:
        print("[WARN] SPREADSHEET_ID is empty. Sheets flush disabled.")
        return

    service = _get_sheets_service()

    try:
        _ensure_latest_header(service)
    except Exception as e:
        print(f"[ERROR] ensure header failed: {e}")

    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SEC)
        try:
            dirty_items = _snapshot_dirty()
            _write_dirty_to_firestore(dirty_items)

            rows = _read_firestore_latest_all()
            _write_latest_sheet(service, rows)

            print(f"[INFO] flushed. dirty={len(dirty_items)}, rows={len(rows)}")
        except Exception as e:
            print(f"[ERROR] flush failed: {e}")


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(_flush_loop())


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/ingest")
async def ingest(request: Request):
    api_key = request.headers.get("x-api-key")
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

    data: Dict[str, Any] = await request.json()

    pc_id_raw = str(data.get("pc_id", "")).strip()
    if not pc_id_raw:
        raise HTTPException(status_code=400, detail="missing pc_id")
    pc_id = pc_id_raw.lower()

    a = _valid_int(data.get("A"))
    b = _valid_int(data.get("B"))

    if a is None and b is None:
        return {"ok": True, "skipped": True}

    updated_at = data.get("updated_at")
    try:
        updated_at = int(updated_at) if updated_at is not None else _now_ts()
    except Exception:
        updated_at = _now_ts()

    server_at = _now_ts()

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
        cur["server_at"] = server_at

        BUFFER[pc_id] = cur

        if changed:
            DIRTY[pc_id] = True
        else:
            DIRTY.setdefault(pc_id, False)

    return {"ok": True}
