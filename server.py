import os
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

# Cloud Run 환경변수에서 API_KEY 읽기
API_KEY = os.environ.get("API_KEY", "")

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/ingest")
async def ingest(request: Request):
    api_key = request.headers.get("x-api-key")
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

    data = await request.json()

    # 최소 검증
    if "pc_id" not in data:
        raise HTTPException(status_code=400, detail="missing pc_id")

    # 지금은 로그만 남김 (Cloud Run 로그에서 확인 가능)
    print("INGEST:", data)

    return {"ok": True}
