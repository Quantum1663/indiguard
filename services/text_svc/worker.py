import asyncio, json, os
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from transformers import pipeline

KAFKA = os.getenv("KAFKA_BROKER","redpanda:9092")
CANDS = ["fake political misinformation", "religious hate speech", "normal news"]
MODEL = os.getenv("ZS_MODEL", "joeddav/xlm-roberta-large-xnli")  # multilingual

def to_schema(pred):
    label = pred["labels"][0]
    score = float(pred["scores"][0])
    if label == "fake political misinformation": return "fake_political", score
    if label == "religious hate speech": return "hs_religious", score
    return "normal", score

async def connect():
    delay=1.0
    for _ in range(12):
        try:
            c=AIOKafkaConsumer("proc.text", bootstrap_servers=KAFKA, group_id="text-svc"); await c.start()
            p=AIOKafkaProducer(bootstrap_servers=KAFKA); await p.start()
            return c,p
        except Exception:
            await asyncio.sleep(delay); delay=min(delay*1.7,5.0)
    raise RuntimeError("Kafka connect failed")

async def run():
    nli = pipeline("zero-shot-classification", model=MODEL, device=-1)  # CPU
    consumer, producer = await connect()
    try:
        async for msg in consumer:
            x = json.loads(msg.value)
            txt = (x.get("text") or "")[:4000]
            pred = nli(txt, CANDS, hypothesis_template="This text is about {}.")
            top, conf = to_schema(pred)
            out = {
              "id": x["id"],
              "labels": {
                "fake_political": 1.0 if top=="fake_political" else 0.0,
                "hs_religious": 1.0 if top=="hs_religious" else 0.0,
                "normal": 1.0 if top=="normal" else 0.0
              },
              "top_label": top, "confidence": conf,
              "explanations": {"spans":[]},
              "model": {"name":"xlmr-zero-shot","version":"0.1"},
              "lang": x.get("lang","und"),
            }
            await producer.send_and_wait("predictions.text", json.dumps(out).encode())
            if top != "normal":
                await producer.send_and_wait("alerts", json.dumps({"id":x["id"],"label":top,"url":x.get("url")}).encode())
    finally:
        await consumer.stop(); await producer.stop()

if __name__ == "__main__":
    asyncio.run(run())

