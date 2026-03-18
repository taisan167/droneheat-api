from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import sqlite3, json, os, time

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
DB_PATH = "droneheat.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try: yield conn
    finally: conn.close()

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS schools (id TEXT PRIMARY KEY, name TEXT, domain TEXT, api_key TEXT UNIQUE, plan TEXT DEFAULT 'basic', created_at TEXT DEFAULT (datetime('now')))")
    c.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, school_id TEXT, session_id TEXT, url TEXT, event_type TEXT, data TEXT, ts INTEGER, created_at TEXT DEFAULT (datetime('now')))")
    c.execute("CREATE INDEX IF NOT EXISTS idx_school ON events(school_id)")
    c.execute("CREATE TABLE IF NOT EXISTS popups (id INTEGER PRIMARY KEY AUTOINCREMENT, school_id TEXT, name TEXT, title TEXT, body TEXT, button_text TEXT DEFAULT '詳しく見る', trigger TEXT DEFAULT 'exit_intent', active INTEGER DEFAULT 1, created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now')))")
    schools = [("jaa-yokohama","JAA横浜","jaa-yokohama.jp","key_jaa_yokohama_demo","master"),("pilina","Pilina","pilina-drone.jp","key_pilina_demo","basic"),("autc","AUTC","autc-drone.jp","key_autc_demo","basic"),("skylink","SkyLink Japan","skylink-japan.jp","key_skylink_demo","pro")]
    for s in schools:
        c.execute("INSERT OR IGNORE INTO schools (id,name,domain,api_key,plan) VALUES (?,?,?,?,?)", s)
    c.execute("INSERT OR IGNORE INTO popups (school_id,name,title,body,trigger) VALUES (?,?,?,?,?)", ("jaa-yokohama","メイン","無料説明会を開催中！","今なら受講料10%OFFキャンペーン中","exit_intent"))
    conn.commit()
    conn.close()

init_db()

class EventItem(BaseModel):
    type: str; sid: str; session: str; url: str; ts: int
    data: Optional[Dict[str, Any]] = {}

class EventBatch(BaseModel):
    events: List[EventItem]

class PopupUpdate(BaseModel):
    title: Optional[str]=None; body: Optional[str]=None
    button_text: Optional[str]=None; trigger: Optional[str]=None; active: Optional[bool]=None

def verify(x_api_key: str=Header(None), db: sqlite3.Connection=Depends(get_db)):
    if not x_api_key: raise HTTPException(401, "API key required")
    s = db.execute("SELECT * FROM schools WHERE api_key=?", (x_api_key,)).fetchone()
    if not s: raise HTTPException(401, "Invalid API key")
    return dict(s)

@app.post("/v1/events")
async def collect(batch: EventBatch, db: sqlite3.Connection=Depends(get_db)):
    n = 0
    for ev in batch.events:
        s = db.execute("SELECT id FROM schools WHERE id=?", (ev.sid,)).fetchone()
        if not s: continue
        db.execute("INSERT INTO events (school_id,session_id,url,event_type,data,ts) VALUES (?,?,?,?,?,?)", (ev.sid,ev.session,ev.url,ev.type,json.dumps(ev.data),ev.ts))
        n += 1
    db.commit()
    return {"ok": True, "inserted": n}

@app.get("/v1/heatmap/clicks")
async def clicks(url: str, days: int=30, school: dict=Depends(verify), db: sqlite3.Connection=Depends(get_db)):
    since = int((datetime.now()-timedelta(days=days)).timestamp()*1000)
    rows = db.execute("SELECT data FROM events WHERE school_id=? AND event_type='click' AND url LIKE ? AND ts>? LIMIT 5000", (school["id"],f"%{url}%",since)).fetchall()
    pts = []
    for r in rows:
        d = json.loads(r["data"] or "{}")
        if "x" in d: pts.append({"x":d["x"],"y":d["y"],"tag":d.get("tag","")})
    return {"points": pts, "total": len(pts)}

@app.get("/v1/stats/summary")
async def summary(days: int=30, school: dict=Depends(verify), db: sqlite3.Connection=Depends(get_db)):
    since = int((datetime.now()-timedelta(days=days)).timestamp()*1000)
    now = int(datetime.now().timestamp()*1000)
    pv = db.execute("SELECT COUNT(*) as c FROM events WHERE school_id=? AND event_type='pageview' AND ts BETWEEN ? AND ?", (school["id"],since,now)).fetchone()["c"]
    pages = db.execute("SELECT url, COUNT(*) as pv FROM events WHERE school_id=? AND event_type='pageview' AND ts>? GROUP BY url ORDER BY pv DESC LIMIT 10", (school["id"],since)).fetchall()
    return {"pv":pv,"pages":[dict(r) for r in pages],"school":dict(school)}

@app.get("/v1/popups")
async def list_popups(school: dict=Depends(verify), db: sqlite3.Connection=Depends(get_db)):
    rows = db.execute("SELECT * FROM popups WHERE school_id=?", (school["id"],)).fetchall()
    return {"popups": [dict(r) for r in rows]}

@app.put("/v1/popups/{pid}")
async def update_popup(pid: int, body: PopupUpdate, school: dict=Depends(verify), db: sqlite3.Connection=Depends(get_db)):
    updates = {k:v for k,v in body.dict().items() if v is not None}
    if not updates: return {"ok":True}
    clause = ", ".join([f"{k}=?" for k in updates])
    db.execute(f"UPDATE popups SET {clause} WHERE id=? AND school_id=?", list(updates.values())+[pid,school["id"]])
    db.commit()
    return {"ok": True}

@app.get("/v1/popup/serve/{sid}")
async def serve(sid: str, db: sqlite3.Connection=Depends(get_db)):
    r = db.execute("SELECT * FROM popups WHERE school_id=? AND active=1 LIMIT 1", (sid,)).fetchone()
    return {"popup": dict(r) if r else None}

@app.get("/health")
async def health():
    return {"status":"ok","ts":int(time.time())}

if __name__=="__main__":
    import uvicorn, os
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
