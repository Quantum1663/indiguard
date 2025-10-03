from fastapi import FastAPI, WebSocket, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncpg
import asyncio, json, os, time

# Use redpanda by default; inside Docker network use service name/port
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")

PG_HOST = os.getenv("PGHOST", "postgres")
PG_USER = os.getenv("PGUSER", "postgres")
PG_PASS = os.getenv("PGPASSWORD", "postgres")
PG_DB   = os.getenv("PGDATABASE", "indiguard")
PG_PORT = int(os.getenv("PGPORT", "5432"))  # keep 5432 inside Docker

app = FastAPI(title="IndiGuard Gateway")

# (optional) allow local file dashboards to hit the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

class IngestItem(BaseModel):
    url: str | None = None
    headline: str
    body: str | None = None
    language_hint: str | None = None

# ---------- Robust startup with retries ----------
async def _start_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    delay = 1.0
    for attempt in range(12):
        try:
            await producer.start()
            return producer
        except Exception:
            if attempt == 11:
                raise
            await asyncio.sleep(delay)
            delay = min(delay * 1.7, 5.0)

async def _start_pg_pool():
    delay = 1.0
    for attempt in range(12):
        try:
            pool = await asyncpg.create_pool(
                host=PG_HOST, user=PG_USER, password=PG_PASS,
                database=PG_DB, port=PG_PORT, min_size=1, max_size=4
            )
            # sanity ping
            async with pool.acquire() as con:
                await con.execute("SELECT 1;")
            return pool
        except Exception:
            if attempt == 11:
                raise
            await asyncio.sleep(delay)
            delay = min(delay * 1.7, 5.0)

@app.on_event("startup")
async def startup():
    app.producer = await _start_kafka_producer()
    app.db = await _start_pg_pool()

@app.on_event("shutdown")
async def shutdown():
    await app.producer.stop()
    await app.db.close()

# ---------- Routes ----------
@app.get("/healthz")
async def healthz():
    # lightweight OK; deeper checks optional
    return {"ok": True, "kafka": KAFKA_BROKER, "pg": f"{PG_HOST}:{PG_PORT}"}

@app.post("/ingest")
async def ingest(item: IngestItem):
    payload = {
        "id": os.urandom(16).hex(),
        "source": "api",
        "url": item.url,
        "timestamp": time.time(),
        "language_hint": item.language_hint,
        "headline": item.headline,
        "body": item.body,
        "media": [],
    }
    await app.producer.send_and_wait("raw.ingest", json.dumps(payload).encode("utf-8"))
    return {"status": "queued", "id": payload["id"]}

@app.websocket("/ws/alerts")
async def ws_alerts(ws: WebSocket):
    await ws.accept()
    consumer = AIOKafkaConsumer("alerts", bootstrap_servers=KAFKA_BROKER, group_id="gateway-ws")
    # connect with retry
    delay = 1.0
    for attempt in range(12):
        try:
            await consumer.start()
            break
        except Exception:
            if attempt == 11:
                await ws.close(code=1011)
                return
            await asyncio.sleep(delay)
            delay = min(delay * 1.7, 5.0)
    try:
        async for msg in consumer:
            await ws.send_text(msg.value.decode("utf-8"))
    except Exception:
        # client disconnected or broker error
        pass
    finally:
        await consumer.stop()
        try:
            await ws.close()
        except Exception:
            pass

@app.get("/recent")
async def recent(
    n: int = 20,
    label: str | None = Query(None),
    lang: str | None = Query(None),
    q: str | None = Query(None),
):
    where = []
    args = []
    if label:
        where.append(f"p.top_label = ${len(args)+1}")
        args.append(label)
    if lang:
        where.append(f"i.lang = ${len(args)+1}")
        args.append(lang)
    if q:
        where.append(f"i.text ILIKE ${len(args)+1}")
        args.append(f"%{q}%")
    wsql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
      SELECT p.created_at, p.top_label, p.confidence, i.url, i.lang,
             LEFT(COALESCE(i.text,''), 180) AS snippet,
             c.img_text_sim
      FROM predictions p
      JOIN items i ON i.id = p.id
      LEFT JOIN consistency c ON c.id = p.id
      {wsql}
      ORDER BY p.created_at DESC
      LIMIT ${len(args) + 1}
    """

    args.append(n)
    async with app.db.acquire() as con:
        rows = await con.fetch(sql, *args)
    return [dict(r) for r in rows]

@app.get("/stats")
async def stats():
    sql = """
    SELECT
      (SELECT count(*) FROM items) AS items,
      (SELECT count(*) FROM predictions) AS predictions,
      (SELECT count(*) FROM predictions WHERE top_label='fake_political') AS fake_political,
      (SELECT count(*) FROM predictions WHERE top_label='hs_religious') AS hs_religious
    """
    async with app.db.acquire() as con:
        row = await con.fetchrow(sql)
    return dict(row)

