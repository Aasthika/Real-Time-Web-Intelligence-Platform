import os
import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Intelligence Dashboard", layout="wide")

# ── Read API URL from env so Docker works (falls back to localhost for dev) ──
API = os.environ.get("API_URL", "http://127.0.0.1:8000")

if "agent_history" not in st.session_state:
    st.session_state.agent_history = []
if "pending_prompt" not in st.session_state:
    st.session_state.pending_prompt = None

st_autorefresh(interval=30000, key="dashboard_refresh")

st.title("🚀 Real-Time Web Intelligence Platform")

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.header("📡 System Control")
u_topic = st.sidebar.text_input("Topic to Watch")
if st.sidebar.button("Subscribe"):
    try:
        requests.post(f"{API}/subscribe", json={"user_id": "admin", "topic": u_topic}, timeout=5)
        st.sidebar.success(f"Watching: {u_topic}")
    except Exception as e:
        st.sidebar.error(f"Subscribe failed: {e}")

# ── Search + Trends ───────────────────────────────────────────────────────────
col1, col2 = st.columns([2, 1])

with col1:
    st.header("🔍 Smart Search")
    q = st.text_input("Fuzzy Search")
    if q:
        try:
            results = requests.get(f"{API}/search?q={q}", timeout=30).json()
            if not results:
                st.info("No matching articles found.")
            for item in results:
                title = item.get('title') or item.get('post') or "Untitled Content"
                link  = item.get('link', '#')
                with st.expander(f"📄 {title}"):
                    st.write(f"**Source:** {item.get('source', 'Unknown')}")
                    st.write(f"[Read Article]({link})")
        except Exception as e:
            st.error(f"Search error: {e}")

with col2:
    st.header("🔥 Top Trends")
    try:
        data = requests.get(f"{API}/trending", timeout=30).json()
        if data:
            df = pd.DataFrame(data)
            if 'score' in df.columns and 'word' in df.columns:
                st.plotly_chart(
                    px.bar(df, x='word', y='score', color='category', title='Top Trending Words'),
                    use_container_width=True
                )

        else:
            st.info("No trending data yet.")
    except Exception as e:
        st.warning(f"Trends unavailable: {e}")

# ── Live Insights ─────────────────────────────────────────────────────────────
st.markdown("---")
st.header("📊 Live Insights")
c1, c2 = st.columns(2)

with c1:
    try:
        stats = requests.get(f"{API}/analytics", timeout=30).json()
        st.metric("Articles Processed", stats.get("total_articles_indexed", 0))
    except:
        st.metric("Articles Processed", "N/A")

with c2:
    st.subheader("🚨 Triggered Alerts")
    try:
        notifs = requests.get(f"{API}/notifications", timeout=30).json()
        if notifs:
            for n in notifs:
                with st.expander(f"🔔 '{n.get('topic','?')}' matched for {n.get('user_id','?')}"):
                    st.write(f"**Matched in:** {str(n.get('match_text', 'N/A'))[:200]}")
                    if n.get('link'):
                        st.write(f"**Link:** [Open article]({n.get('link')})")
                    if n.get('triggered_at'):
                        st.caption(f"Triggered at: {n['triggered_at']}")
        else:
            st.info("No alerts yet.")
    except:
        st.info("No alerts yet.")

# ── AI Classification ─────────────────────────────────────────────────────────
st.header("🤖 AI Content Classification")
try:
    data = requests.get(f"{API}/mix", timeout=30).json()
    if data:
        df = pd.DataFrame(data)
        if 'category' in df.columns and 'count' in df.columns:
            fig = px.pie(df, values='count', names='category')
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No classification data yet.")
except Exception as e:
    st.error(f"Classification chart error: {e}")

# ── AI Agent ──────────────────────────────────────────────────────────────────
st.markdown("---")
st.header("🤖 AI Agent Assistant")
st.caption("Ask anything about your live data stream")

for msg in st.session_state.agent_history:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])
        if msg.get("tools_used"):
            st.caption(f"🔧 Tools used: {', '.join(msg['tools_used'])}")

if st.session_state.pending_prompt:
    prompt = st.session_state.pending_prompt
    st.session_state.pending_prompt = None

    with st.chat_message("assistant"):
        with st.spinner("Agent thinking..."):
            try:
                result = requests.post(
                    f"{API}/agent",
                    json={"message": prompt},
                    timeout=60
                ).json()

                if "error" in result:
                    response_text = f"⚠️ {result['error']}"
                    tools_used = []
                else:
                    response_text = result.get("response") or "No response returned."
                    tools_used    = result.get("tools_used", [])

                st.write(response_text)
                if tools_used:
                    st.caption(f"🔧 Tools used: {', '.join(tools_used)}")

                st.session_state.agent_history.append({
                    "role": "assistant",
                    "content": response_text,
                    "tools_used": tools_used
                })
            except requests.exceptions.Timeout:
                st.error("⏱️ Agent timed out.")
            except Exception as e:
                st.error(f"Agent error: {e}")

if prompt := st.chat_input("Ask: 'What's trending in AI?' or 'Check system health'"):
    last = st.session_state.agent_history[-1] if st.session_state.agent_history else {}
    if last.get("content") != prompt or last.get("role") != "user":
        st.session_state.agent_history.append({"role": "user", "content": prompt})
        st.session_state.pending_prompt = prompt
        st.rerun()