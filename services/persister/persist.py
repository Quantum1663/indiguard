import os, json, asyncio
import asyncpg
from aiokafka import AIOKafkaConsumer

KAFKA = os.getenv("KAFKA_BROKER", "redpanda:9092")

async def db():
    delay = 1.0
    for attempt in range(10):
        try:
            conn = await asyncpg.connect(
                host=os.getenv("PGHOST","postgres"),
                user=os.getenv("PGUSER","postgres"),
                password=os.getenv("PGPASSWORD","postgres"),
                database=os.getenv("PGDATABASE","indiguard"),
                port=int(os.getenv("PGPORT","5432")),
            )
            await conn.execute("SELECT 1;")
            return conn
        except Exception:
            if attempt == 9:
                raise
            await asyncio.sleep(delay)
            delay = min(delay * 1.6, 5.0)

async def run():
    conn = await db()
    # Subscribe to proc.text, predictions.text, and consistency.signals
    consumer = AIOKafkaConsumer(
        "proc.text", "predictions.text", "consistency.signals",
        bootstrap_servers=KAFKA, group_id="persister"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            topic = msg.topic
            x = json.loads(msg.value)
            if topic == "proc.text":
                # upsert item row
                await conn.execute(
                    """
                    INSERT INTO items (id, url, source, lang, text)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (id) DO UPDATE SET
                      url = COALESCE(excluded.url, items.url),
                      source = COALESCE(excluded.source, items.source),
                      lang = COALESCE(excluded.lang, items.lang),
                      text = COALESCE(excluded.text, items.text)
                    """,
                    x["id"], x.get("url"), x.get("source"), x.get("lang"), x.get("text")
                )
            elif topic == "predictions.text":
                # ensure item exists (minimal insert if missing)
                await conn.execute(
                    "INSERT INTO items (id) VALUES ($1) ON CONFLICT (id) DO NOTHING",
                    x["id"]
                )
                await conn.execute(
                    """
                    INSERT INTO predictions (id, top_label, confidence, raw)
                    VALUES ($1, $2, $3, $4::jsonb)
                    """,
                    x["id"], x.get("top_label"), float(x.get("confidence", 0.0)), json.dumps(x)
                )
            elif topic == "consistency.signals":
                # ensure item row exists
                await conn.execute("INSERT INTO items (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", x["id"])
                await conn.execute(
                    """
                    INSERT INTO consistency (id, img_text_sim, ocr_chars, ocr_preview)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (id) DO UPDATE SET
                      img_text_sim = EXCLUDED.img_text_sim,
                      ocr_chars = EXCLUDED.ocr_chars,
                      ocr_preview = EXCLUDED.ocr_preview
                    """,
                    x["id"], float(x.get("img_text_sim", 0.0)), int(x.get("ocr_chars", 0)), x.get("ocr_preview")
                )
    finally:
        await consumer.stop()
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run())