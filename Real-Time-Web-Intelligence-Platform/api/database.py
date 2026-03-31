from cassandra.cluster import Cluster
from datetime import datetime
from elasticsearch import Elasticsearch
import pandas as pd


# --------------------------------
# Cassandra Connection (Robust)
# --------------------------------
import time
from cassandra.cluster import Cluster

session = None

print("Connecting to Cassandra...")

while session is None:
    try:
        cluster = Cluster(["cassandra"])
        session = cluster.connect("realtime")
        print("✅ Connected to Cassandra")
    except Exception as e:
        print("⏳ Waiting for Cassandra...")
        time.sleep(5)


# --------------------------------
# Elasticsearch Connection
# --------------------------------
es = Elasticsearch("http://elasticsearch:9200")

INDEX_NAME = "web_intelligence"


# --------------------------------
# Trending (From Cassandra)
# --------------------------------
def get_trending():

    rows = session.execute(
        """
        SELECT word, count, score, timestamp
        FROM trending_topics
        """
    )

    data = []

    for row in rows:
        data.append({
            "word": row.word,
            "count": row.count,
            "score": row.score,
            "timestamp": row.timestamp
        })

    df = pd.DataFrame(data)

    if len(df) == 0:
        return []

    # Get latest row per word
    df = df.sort_values("timestamp", ascending=False)

    df = df.drop_duplicates(subset=["word"], keep="first")

    # Sort by count
    df = df.sort_values("count", ascending=False)

    return df.head(20).to_dict("records")
 


# --------------------------------
# Search (From Elasticsearch)
# --------------------------------
def search_query(keyword):

    query = {
        "size": 100,
        "query": {
            "match": {
                "word": keyword
            }
        },
        "sort": [
            {"score": {"order": "desc"}},
            {"timestamp": {"order": "desc"}}
        ]
    }

    res = es.search(
        index=INDEX_NAME,
        body=query
    )

    results = []
    seen = set()

    for hit in res["hits"]["hits"]:

        data = hit["_source"]

        if data["word"] not in seen:
            results.append(data)
            seen.add(data["word"])

    return results[:20]


# --------------------------------
# Analytics
# --------------------------------
def analytics():

    query = "SELECT COUNT(*) FROM trending_topics"

    rows = session.execute(query)

    for row in rows:
        return {
            "total_trending_words": row.count
        }


# --------------------------------
# Add Alert
# --------------------------------
def add_alert(keyword):

    session.execute(
        """
        INSERT INTO user_alerts (keyword, created_at)
        VALUES (%s, %s)
        """,
        (
            keyword,
            datetime.utcnow()
        )
    )