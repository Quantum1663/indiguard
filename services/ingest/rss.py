import asyncio, json, os, time, re
import feedparser
from bs4 import BeautifulSoup
from aiokafka import AIOKafkaProducer

KAFKA = os.getenv("KAFKA_BROKER","redpanda:9092")
FEEDS = [
  "https://www.altnews.in/feed/",
  "https://www.boomlive.in/rss.xml"
]

def extract_image(entry):
    # 1) media_content (Yahoo Media RSS)
    mc = entry.get("media_content")
    if isinstance(mc, list) and mc:
        for m in mc:
            url = m.get("url")
            if url: return url
    # 2) enclosures
    for link in entry.get("links", []):
        if link.get("rel") == "enclosure" and (link.get("type") or "").startswith("image/"):
            return link.get("href")
    # 3) parse summary/content HTML
    html = entry.get("summary") or ""
    if "content" in entry and isinstance(entry["content"], list) and entry["content"]:
        html = entry["content"][0].get("value") or html
    if html:
        soup = BeautifulSoup(html, "html.parser")
        img = soup.find("img")
        if img and img.get("src"):
            return img["src"]
    return None

async def run():
    prod = AIOKafkaProducer(bootstrap_servers=KAFKA)
    await prod.start()
    try:
        seen = set()
        while True:
            for f in FEEDS:
                d = feedparser.parse(f)
                for e in d.entries[:20]:
                    url = e.get("link")
                    if not url or url in seen: continue
                    seen.add(url)
                    img_url = extract_image(e)
                    payload = {
                        "id": os.urandom(16).hex(),
                        "source": "rss",
                        "url": url,
                        "timestamp": time.time(),
                        "headline": e.get("title",""),
                        "body": e.get("summary",""),
                        "language_hint": None,
                        "media": ([{"type":"image","url":img_url}] if img_url else [])
                    }
                    await prod.send_and_wait("raw.ingest", json.dumps(payload).encode())
            await asyncio.sleep(60)
    finally:
        await prod.stop()

if __name__ == "__main__":
    asyncio.run(run())
