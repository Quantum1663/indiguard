import asyncio, json, os, re
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import langid

KAFKA = os.getenv("KAFKA_BROKER", "redpanda:9092")

def compute_code_mix_ratio(text: str) -> float:
    # rough proxy: fraction of Latin letters among all letters
    letters = [ch for ch in text if ch.isalpha()]
    if not letters:
        return 0.0
    latin = [ch for ch in letters if "A" <= ch.upper() <= "Z"]
    return round(len(latin) / len(letters), 3)

def clean_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s)            # strip tags
    s = re.sub(r"&[a-z]+;", " ", s)           # unescape entities roughly
    s = re.sub(r"\s+", " ", s).strip()
    return s

async def connect():
    delay = 1.0
    for _ in range(12):  # ~30s
        try:
            consumer = AIOKafkaConsumer("raw.ingest", bootstrap_servers=KAFKA, group_id="normalizer")
            await consumer.start()
            producer = AIOKafkaProducer(bootstrap_servers=KAFKA)
            await producer.start()
            return consumer, producer
        except Exception:
            await asyncio.sleep(delay)
            delay = min(delay * 1.7, 5.0)
    raise RuntimeError("Kafka connect failed")

async def run():
    consumer, producer = await connect()
    try:
        async for m in consumer:
            x = json.loads(m.value)
            text = f"{x.get('headline','')}\n{x.get('body','')}"
            text = re.sub(r"\s+", " ", text).strip()
            # LID
            lang, _ = langid.classify(text if text else " ")
            # Code-mix
            cmr = compute_code_mix_ratio(text)
            out = {
                "id": x["id"],
                "lang": lang,
                "text": text,
                "code_mix_ratio": cmr,
                "source": x.get("source"),
                "url": x.get("url"),
                "timestamp": x.get("timestamp"),
            }
            await producer.send_and_wait("proc.text", json.dumps(out).encode())
    finally:
        await consumer.stop(); await producer.stop()

if __name__ == "__main__":
    asyncio.run(run())

