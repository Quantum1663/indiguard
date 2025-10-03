import os, time, json, threading, queue
import requests, pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# ---------------- Config ----------------
API_BASE = os.getenv("API_BASE", "http://localhost:8000")
WS_URL   = os.getenv("WS_URL",   "ws://localhost:8000/ws/alerts")

st.set_page_config(page_title="IndiGuard", layout="wide")

# ---------------- Helpers ----------------
def get_json(url, **kwargs):
    try:
        r = requests.get(url, timeout=10, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Couldn’t load {url}: {e}")
        return None

def post_json(url, payload):
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"Couldn’t send data: {e}")
        return None

def sim_hint(sim):
    try:
        s = float(sim)
    except:
        return ""
    if s >= 0.25: return "Image/Text: likely match"
    if s >= 0.15: return "Image/Text: uncertain"
    return "Image/Text: likely mismatch"

LABEL_EMOJI = {
    "fake_political": "🚨 Fake (Political)",
    "hs_religious": "⚠️ Religious Hate Speech",
    "normal": "✅ Normal",
}

# ---------------- Header ----------------
st.title("IndiGuard — Real-time Misinformation & Hate Speech Monitor")
st.caption("Simple, human-friendly view. Paste a headline, watch live alerts, and browse recent checks.")

# ---------------- Summary row ----------------
stats = get_json(f"{API_BASE}/stats") or {}
c1, c2, c3, c4 = st.columns(4)
c1.metric("Items", stats.get("items", 0))
c2.metric("Predictions", stats.get("predictions", 0))
c3.metric("Fake Political", stats.get("fake_political", 0))
c4.metric("Religious HS", stats.get("hs_religious", 0))

st.divider()

# ---------------- Tabs ----------------
tab_live, tab_explore, tab_help = st.tabs(["🔴 Live Feed", "🔎 Explore", "❓ About"])

# ======== LIVE FEED ========
with tab_live:
    st.subheader("Quick Check")
    with st.form("ingest_form", clear_on_submit=True):
        url = st.text_input("Link (optional)")
        headline = st.text_input("Headline", help="Add a short title or claim to check")
        body = st.text_area("Details (optional)")
        colA, colB = st.columns([1,3])
        with colA:
            submit = st.form_submit_button("Check now")
        with colB:
            st.caption("Tip: include words like “fake/rumor” or political terms to trigger an alert in the demo.")

    if submit and (headline or body):
        resp = post_json(f"{API_BASE}/ingest", {"url": url or None, "headline": headline, "body": body or None})
        if resp:
            st.success(f"Queued ✓ (id: {resp.get('id')})")

    st.subheader("Live Alerts")
    placeholder = st.empty()

    # WebSocket background thread
    if "alert_queue" not in st.session_state:
        st.session_state.alert_queue = queue.Queue()
    if "ws_thread" not in st.session_state:
        st.session_state.ws_stop = threading.Event()

        def ws_loop(url, q, stop_event):
            """Background WS client that pushes messages into a queue."""
            try:
                import websocket  # from websocket-client
            except Exception:
                q.put(json.dumps({"error":"websocket-client not installed"}))
                return

            def on_message(wsapp, message): q.put(message)
            def on_error(wsapp, err): q.put(json.dumps({"error": str(err)}))
            def on_close(wsapp, *_): q.put(json.dumps({"info":"ws closed"}))
            def on_open(wsapp): q.put(json.dumps({"info":"ws connected"}))

            while not stop_event.is_set():
                try:
                    wsapp = websocket.WebSocketApp(
                        url,
                        on_open=on_open,
                        on_message=on_message,
                        on_error=on_error,
                        on_close=on_close
                    )
                    wsapp.run_forever()
                except Exception as e:
                    q.put(json.dumps({"error": f"reconnect: {e}"}))
                time.sleep(2)  # small backoff

        st.session_state.ws_thread = threading.Thread(
            target=ws_loop, args=(WS_URL, st.session_state.alert_queue, st.session_state.ws_stop),
            daemon=True
        )
        st.session_state.ws_thread.start()

    # Render alerts log
    log = st.session_state.get("alerts_text", "")
    while not st.session_state.alert_queue.empty():
        msg = st.session_state.alert_queue.get_nowait()
        try:
            log += json.dumps(json.loads(msg), ensure_ascii=False) + "\n"
        except Exception:
            log += str(msg) + "\n"
    st.session_state.alerts_text = log
    placeholder.code(st.session_state.alerts_text or "[waiting for alerts…]", language="json")

# ======== EXPLORE ========
with tab_explore:
    st.subheader("Browse Recent Checks")
    cL, cM, cR, cN = st.columns([1,1,2,1])
    with cL:
        f_label = st.selectbox("Label", ["", "fake_political", "hs_religious", "normal"])
    with cM:
        f_lang = st.text_input("Language (e.g., hi, en)")
    with cR:
        f_q = st.text_input("Search text")
    with cN:
        f_n = st.slider("Rows", 10, 400, 100)

    params = {"n": f_n}
    if f_label: params["label"] = f_label
    if f_lang:  params["lang"]  = f_lang
    if f_q:     params["q"]     = f_q

    recent = get_json(f"{API_BASE}/recent", params=params) or []
    df_raw = pd.DataFrame(recent)  # keep raw for charts

    if df_raw.empty:
        st.info("No results yet. Try removing filters or wait a moment.")
    else:
        # Display-friendly copy
        df = df_raw.copy()
        df.rename(columns={
            "created_at":"When",
            "top_label":"Label",
            "confidence":"Confidence",
            "url":"Link",
            "lang":"Lang",
            "snippet":"Summary",
            "img_text_sim":"Img/Text score"
        }, inplace=True)

        # Labels → emoji text (no HTML)
        if "Label" in df.columns:
            df["Label"] = df["Label"].map(LABEL_EMOJI).fillna("Unknown")

        # Confidence as %
        if "Confidence" in df.columns:
            df["Confidence"] = (pd.to_numeric(df["Confidence"], errors="coerce").fillna(0) * 100).round(1).astype(str) + "%"

        # Similarity hint
        if "Img/Text score" in df.columns:
            df["Hint"] = df["Img/Text score"].apply(sim_hint)

        # Clickable link column (Streamlit column config)
        col_cfg = {}
        if "Link" in df.columns:
            col_cfg["Link"] = st.column_config.LinkColumn("Link", display_text="open")

        # Column order
        cols = ["When","Label","Confidence","Lang","Link","Summary"]
        if "Hint" in df.columns: cols.append("Hint")

        # Sort by time if present
        if "When" in df.columns:
            df["When"] = pd.to_datetime(df["When"], errors="coerce")
            df = df.sort_values("When", ascending=False)

        st.dataframe(
            df[cols],
            use_container_width=True,
            height=480,
            column_config=col_cfg
        )

        # Quick charts (use raw labels/langs)
        st.markdown("### Quick Charts")
        cc1, cc2 = st.columns(2)
        with cc1:
            if "top_label" in df_raw.columns:
                vals = df_raw["top_label"].value_counts()
                fig = plt.figure()
                vals.plot(kind="bar")
                plt.title("Label counts")
                plt.xlabel("Label"); plt.ylabel("Count")
                st.pyplot(fig)
        with cc2:
            if "lang" in df_raw.columns:
                vals2 = df_raw["lang"].fillna("und").value_counts().head(10)
                fig2 = plt.figure()
                vals2.plot(kind="bar")
                plt.title("Top languages")
                plt.xlabel("Lang"); plt.ylabel("Count")
                st.pyplot(fig2)

# ======== ABOUT ========
with tab_help:
    st.subheader("What am I seeing?")
    st.write("""
- **Label** — our best guess about the content:
  - 🚨 **Fake (Political):** likely misinformation around politics.
  - ⚠️ **Religious Hate Speech:** harmful content targeting religion.
  - ✅ **Normal:** likely safe/benign.
- **Confidence** — how sure we are (0–100%).
- **Image/Text score** — whether an image seems to match the text (higher ≈ better match).
    """)
    st.info("This demo is for research. Decisions may be imperfect. Always double-check with trusted sources.")

# --------- Auto refresh (so you don’t need to reload) ---------
st.sidebar.markdown("---")
autolive = st.sidebar.checkbox("Auto-refresh every 2s", value=True)
if autolive:
    time.sleep(2)
    st.rerun()
