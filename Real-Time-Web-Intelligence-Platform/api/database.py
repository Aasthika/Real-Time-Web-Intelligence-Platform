from cassandra.cluster import Cluster
from datetime import datetime
from elasticsearch import Elasticsearch
import pandas as pd
import time


# --------------------------------
# Lazy Cassandra Connection
# --------------------------------
session = None


session = None

def get_session():

    global session

    if session is None:

        print("Connecting to Cassandra...")

        while True:
            try:
                cluster = Cluster(["cassandra"])
                session = cluster.connect("realtime")
                print("✅ Cassandra Connected")
                break

            except Exception as e:
                print("⏳ Waiting for Cassandra...", e)
                time.sleep(5)

    return session

# --------------------------------
# Elasticsearch
# --------------------------------
es = Elasticsearch("http://elasticsearch:9200")

INDEX_NAME = "web_intelligence"


# --------------------------------
# Trending
# --------------------------------
def get_trending():

    session = get_session()

    rows = session.execute(
    """
    SELECT word, count, score, timestamp
    FROM trending_topics
    ALLOW FILTERING
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

    df = df.sort_values("timestamp", ascending=False)

    df = df.drop_duplicates(subset=["word"], keep="first")

    df = df.sort_values("count", ascending=False)

    return df.head(20).to_dict("records")


# --------------------------------
# Search
# --------------------------------
def search_query(keyword):

    query = {
        "size": 100,
        "query": {
            "match": {
                "word": keyword
            }
        }
    }

    res = es.search(
        index=INDEX_NAME,
        body=query
    )

    results = []

    for hit in res["hits"]["hits"]:
        results.append(hit["_source"])

    return results


# --------------------------------
# Analytics
# --------------------------------
def analytics():

    session = get_session()

    rows = session.execute(
        "SELECT COUNT(*) FROM trending_topics"
    )

    for row in rows:
        return {
            "total_trending_words": row.count
        }


# --------------------------------
# Alerts
# --------------------------------
def add_alert(keyword):

    session = get_session()

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

def get_triggered_alerts():

    session = get_session()

    rows = session.execute(
        "SELECT word, timestamp FROM triggered_alerts ALLOW FILTERING"
    )

    data = []

    for row in rows:
        data.append({
            "word": row.word,
            "timestamp": row.timestamp
        })

    return data