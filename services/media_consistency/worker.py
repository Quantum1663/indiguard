import asyncio, json, os, time, io
from collections import defaultdict
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import requests
from PIL import Image
import pytesseract
from transformers import CLIPProcessor, CLIPModel

KAFKA = os.getenv("KAFKA_BROKER","redpanda:9092")
CLIP = os.getenv("CLIP_MODEL","openai/clip-vit-base-patch32")

# Simple in-memory join on id
cache_text = {}
cache_media = {}

def cosine(a, b):
    import torch
    a = a / a.norm(dim=-1, keepdim=True)
    b = b / b.norm(dim=-1, keepdim=True)
    return (a @ b.T).squeeze().item()

async def connect():
    delay=1.0
    for _ in range(12):
        try:
            c = AIOKafkaConsumer("raw.ingest","proc.text", bootstrap_servers=KAFKA, group_id="media-consistency")
            await c.start()
            p = AIOKafkaProducer(bootstrap_servers=KAFKA)
            await p.start()
            return c,p
        except Exception:
            await asyncio.sleep(delay); delay=min(delay*1.7, 5.0)
    raise RuntimeError("Kafka connect failed")

async def run():
    model = CLIPModel.from_pretrained(CLIP)
    proc  = CLIPProcessor.from_pretrained(CLIP)
    consumer, producer = await connect()
    try:
        async for msg in consumer:
            x = json.loads(msg.value)
            if msg.topic == "proc.text":
                cache_text[x["id"]] = {"text": x.get("text") or "", "lang": x.get("lang")}
            else:  # raw.ingest
                media = x.get("media") or []
                img_url = next((m.get("url") for m in media if m.get("type") == "image" and m.get("url")), None)
                if img_url:
                    cache_media[x["id"]] = {"img_url": img_url, "headline": x.get("headline",""), "body": x.get("body","")}
            # if we have both sides, compute
            if x["id"] in cache_text and x["id"] in cache_media:
                t = cache_text.pop(x["id"])
                m = cache_media.pop(x["id"])
                text = (m["headline"] + " " + m["body"]).strip() or t["text"]
                # download image
                sim = None; ocr_txt = ""
                try:
                    r = requests.get(m["img_url"], timeout=10)
                    img = Image.open(io.BytesIO(r.content)).convert("RGB")
                    # OCR
                    ocr_txt = pytesseract.image_to_string(img) or ""
                    # CLIP sim
                    inputs = proc(text=[text[:512]], images=img, return_tensors="pt", padding=True)
                    with torch.no_grad():
                        feats = model.get_text_features(**{k:v for k,v in inputs.items() if k.startswith("input_ids") or k.startswith("attention_mask")})
                        vfeats = model.get_image_features(pixel_values=inputs["pixel_values"])
                    sim = cosine(feats, vfeats)
                except Exception:
                    sim = None
                out = {
                  "id": x["id"],
                  "img_text_sim": float(sim) if sim is not None else 0.0,
                  "ocr_chars": len(ocr_txt),
                  "ocr_preview": (ocr_txt[:200] if ocr_txt else None),
                  "timestamp": time.time()
                }
                await producer.send_and_wait("consistency.signals", json.dumps(out).encode())
    finally:
        await consumer.stop(); await producer.stop()

if __name__ == "__main__":
    import torch  # ensure imported for no_grad
    asyncio.run(run())
