import os
import json
import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster
import google.generativeai as genai
from dotenv import load_dotenv


load_dotenv()


app = Flask(__name__)
CORS(app)


# ── Connections (use env vars so Docker works) ───────────────────────────────
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "127.0.0.1")
ES_HOST = os.environ.get("ES_HOST", "http://localhost:9200")


es = Elasticsearch([ES_HOST])


cassandra_session = None
import time
import sys

# In cloud environments with swap, Cassandra is extremely slow to initialize.
# We will wait and retry until it succeeds.
for attempt in range(100):
   try:
       cluster = Cluster([CASSANDRA_HOST], control_connection_timeout=120.0, connect_timeout=120.0)
       # Connect without keyspace first
       cassandra_session = cluster.connect()
       cassandra_session.default_timeout = 300.0
       
       # Initialize keyspace
       cassandra_session.execute("""
           CREATE KEYSPACE IF NOT EXISTS realtime 
           WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
       """)
       cassandra_session.set_keyspace('realtime')

       def dict_factory(colnames, rows):
           return [dict(zip(colnames, row)) for row in rows]

       cassandra_session.row_factory = dict_factory

       # Initialize tables
       cassandra_session.execute("""
           CREATE TABLE IF NOT EXISTS page_metadata (
               id UUID PRIMARY KEY,
               title TEXT,
               source TEXT,
               published TEXT,
               timestamp TIMESTAMP
           );
       """)
       
       cassandra_session.execute("""
           CREATE TABLE IF NOT EXISTS inverted_index (
               word TEXT,
               id UUID,
               title TEXT,
               PRIMARY KEY (word, id)
           );
       """)
       
       cassandra_session.execute("""
           CREATE TABLE IF NOT EXISTS user_alerts (
               user_id TEXT,
               topic TEXT,
               created_at TIMESTAMP,
               PRIMARY KEY (user_id, topic)
           );
       """)
       
       cassandra_session.execute("""
           CREATE TABLE IF NOT EXISTS trending_topics (
               word text,
               category text,
               count int,
               final_score float,
               pop_score float,
               recency_score float,
               PRIMARY KEY (category, final_score, word)
           ) WITH CLUSTERING ORDER BY (final_score DESC);
       """)
       
       cassandra_session.execute("""
           CREATE TABLE IF NOT EXISTS triggered_alerts (
               alert_id uuid PRIMARY KEY,
               user_id text,
               topic text,
               match_text text,
               link text,
               triggered_at timestamp
           );
       """)

       print(f"✅ Cassandra connected and initialized at {CASSANDRA_HOST}")
       break
   except Exception as e:
       print(f"❌ Cassandra Connection Error (Attempt {attempt+1}): {e}")
       time.sleep(15)
else:
   print("🚨 Could not connect to Cassandra after 100 attempts. Exiting.")
   sys.exit(1)


def _cassandra_ready():
   """Return True if the Cassandra session is available, False otherwise."""
   return cassandra_session is not None


# ── Tool Functions for Gemini ────────────────────────────────────────────────


def search_articles(query: str):
   """
   Search for articles matching a topic or keyword in the intelligence database.
   Args:
       query: The search query string.
   """
   try:
       res = es.search(
           index="web_intelligence",
           body={
               "query": {"match": {"text": {"query": query, "fuzziness": "AUTO"}}},
               "size": 5,
               "_source": ["title", "post", "source", "link"]
           },
           request_timeout=10   # cap ES wait at 10 s for fast UI response
       )
       hits = []
       for h in res['hits']['hits']:
           src = h["_source"]
           hits.append({
               "title": src.get("title") or src.get("post", "")[:100],
               "source": src.get("source", ""),
               "link": src.get("link", "")
           })
       return hits if hits else [{"message": "No articles found for that query."}]
   except Exception as e:
       return [{"error": f"Search failed: {str(e)}"}]




def get_trending_topics(category: str = None):
   """
   Get currently trending words or topics from the live data stream.
   Args:
       category: Optional filter — one of: News, Blog, Research, Social.
   """
   if not _cassandra_ready():
       return [{"error": "Cassandra is still initializing — please try again in a moment."}]
   try:
       rows = list(cassandra_session.execute(
           "SELECT word, category, count, final_score FROM trending_topics LIMIT 100"
       ))
       if category:
           rows = [r for r in rows if r.get("category") == category]
       rows.sort(key=lambda x: x.get("final_score") or 0, reverse=True)
       result = [
           {"word": r.get("word"), "category": r.get("category"),
            "count": r.get("count"), "score": r.get("final_score")}
           for r in rows[:15]
       ]
       return result if result else [{"message": "No trending topics yet."}]
   except Exception as e:
       return [{"error": f"Trending fetch failed: {str(e)}"}]




def get_alerts():
   """Get the most recently triggered alerts for all monitored topics."""
   if not _cassandra_ready():
       return [{"error": "Cassandra is still initializing — please try again in a moment."}]
   try:
       rows = list(cassandra_session.execute(
           "SELECT user_id, topic, match_text, link, triggered_at FROM triggered_alerts LIMIT 10"
       ))
       result = [
           {"user": r.get("user_id"), "topic": r.get("topic"),
            "match": str(r.get("match_text", ""))[:150],
            "link": r.get("link", ""),
            "time": str(r.get("triggered_at", ""))}
           for r in rows
       ]
       return result if result else [{"message": "No alerts triggered yet."}]
   except Exception as e:
       return [{"error": f"Alerts fetch failed: {str(e)}"}]




def subscribe_to_topic(topic: str, user_id: str = "admin"):
   """
   Subscribe a user to get alerts when a topic appears in the live stream.
   Args:
       topic: The keyword or topic to monitor.
       user_id: The user ID to subscribe (default is admin).
   """
   if not _cassandra_ready():
       return {"error": "Cassandra is still initializing — please try again in a moment."}
   try:
       cassandra_session.execute(
           "INSERT INTO user_alerts (user_id, topic, created_at) VALUES (%s, %s, toTimestamp(now()))",
           (user_id, topic)
       )
       return {"status": "subscribed", "topic": topic, "user_id": user_id}
   except Exception as e:
       return {"error": f"Subscription failed: {str(e)}"}




def summarize_topic(topic: str):
   """
   Fetch recent articles about a topic so you can summarize or analyze them.
   Args:
       topic: The topic or keyword to look up.
   """
   try:
       res = es.search(index="web_intelligence", body={
           "query": {"match": {"text": topic}},
           "size": 8
       })
       articles = []
       for h in res['hits']['hits']:
           src = h["_source"]
           text = src.get("title") or src.get("post") or ""
           if text:
               articles.append(text[:300])
       return {"articles": articles, "count": len(articles)} if articles else {"message": "No content found."}
   except Exception as e:
       return {"error": f"Summary fetch failed: {str(e)}"}




def check_system_health():
   """Check if Elasticsearch and Cassandra are reachable and healthy."""
   return {
       "elasticsearch": "reachable" if es.ping() else "unreachable",
       "cassandra": "reachable" if cassandra_session else "unreachable",
       "es_host": ES_HOST,
       "cassandra_host": CASSANDRA_HOST
   }




# ── Gemini Agent Setup ────────────────────────────────────────────────────────
api_key = os.environ.get("GOOGLE_API_KEY")
if not api_key:
   print("⚠️  WARNING: GOOGLE_API_KEY not set — agent will not work")


genai.configure(api_key=api_key or "")


# Hardcode to gemini-1.5-flash which is the standard default that supports all native function calling
target_model = 'gemini-2.5-flash'

model = genai.GenerativeModel(
   model_name=target_model,
   tools=[
       search_articles,
       get_trending_topics,
       get_alerts,
       subscribe_to_topic,
       summarize_topic,
       check_system_health
   ],
   system_instruction=(
       "You are an intelligent assistant for a Real-Time Web Intelligence Platform. "
       "You monitor news, blogs, research, and social posts ingested via Kafka and processed by Spark. "
       "ALWAYS call the appropriate tool to fetch real live data before answering. "
       "Never make up data. Be concise, analytical, and helpful."
   )
)


# ── Flask Routes ─────────────────────────────────────────────────────────────


@app.route('/health')
def health():
   return jsonify({"status": "online", "es": es.ping(), "cassandra": cassandra_session is not None})


@app.get('/search')
def search_api():
   q = request.args.get('q', '')
   if len(q) < 2:
       return jsonify([])
   return jsonify(search_articles(q))


@app.get('/trending')
def trending_api():
   # This powers the Dashboard bar chart
   return jsonify(get_trending_topics()[:10])

@app.get('/mix')
def mix_api():
   if not _cassandra_ready():
       return jsonify([])
   try:
       # Fetch the actual grouped sum per category safely using driver
       query = "SELECT category, count(*) FROM trending_topics GROUP BY category"
       rows = cassandra_session.execute(query)
       return jsonify([{"category": r.get('category'), "count": r.get('count')} for r in rows])
   except Exception as e:
       print("Mix Error:", e)
       return jsonify([])


@app.get('/analytics')
def analytics_api():
   if not _cassandra_ready():
       return jsonify({"total_articles_indexed": 0})
   try:
       row = cassandra_session.execute("SELECT count(*) AS total FROM page_metadata").one()
       return jsonify({"total_articles_indexed": row['total'] if row else 0})
   except Exception as e:
       print("Analytics Error:", e)
       return jsonify({"total_articles_indexed": 0})


@app.get('/notifications')
def notifications_api():
   if not _cassandra_ready():
       return jsonify([])
   try:
       rows = cassandra_session.execute(
           "SELECT user_id, topic, match_text, link, triggered_at FROM triggered_alerts LIMIT 5"
       )
       return jsonify(list(rows))
   except Exception as e:
       print("Notifications Error:", e)
       return jsonify([])


@app.post('/subscribe')
def subscribe_api():
   if not _cassandra_ready():
       return jsonify({"error": "Cassandra is still initializing — please try again shortly."}), 503
   data = request.json or {}
   return jsonify(subscribe_to_topic(data.get('topic', ''), data.get('user_id', 'admin')))




@app.post('/agent')
def agent_query():
   """
   The agent endpoint. Uses Gemini with automatic function calling.
   Gemini handles the full think → call tool → observe → respond loop.
   """
   try:
       data = request.json or {}
       user_message = data.get("message", "").strip()
       if not user_message:
           return jsonify({"error": "Empty message"}), 400


       # Start a fresh chat with automatic function calling ON
       # Gemini will call tools internally until it has a final text answer
       chat = model.start_chat(enable_automatic_function_calling=True)
       response = chat.send_message(user_message)


       # Extract which tools were called by inspecting the full chat history
       # (automatic calling resolves tools internally, so we check history not response.parts)
       tools_used = []
       for message in chat.history:
           for part in message.parts:
               fc = getattr(part, 'function_call', None)
               if fc and hasattr(fc, 'name') and fc.name:
                   tools_used.append(fc.name)


       final_text = response.text if response.text else "I analysed your request using live data."


       return jsonify({
           "response": final_text,
           "tools_used": list(set(tools_used))
       })


   except Exception as e:
       import traceback
       traceback.print_exc()
       return jsonify({"error": f"Agent error: {str(e)}"}), 500




if __name__ == '__main__':
   app.run(host='0.0.0.0', port=8000, debug=True)