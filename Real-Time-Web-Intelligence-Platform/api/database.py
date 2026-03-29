from cassandra.cluster import Cluster
from datetime import datetime
from elasticsearch import Elasticsearch
import pandas as pd


# --------------------------------
# Cassandra Connection
# --------------------------------
cluster = Cluster(["localhost"])
session = cluster.connect("realtime")


# --------------------------------
# Elasticsearch Connection
# --------------------------------
es = Elasticsearch("http://localhost:9200")

INDEX_NAME = "web_intelligence"


# --------------------------------
# Trending (From Cassandra)
# --------------------------------
def get_trending():

    rows = session.execute(
        "SELECT word, count, score FROM trending_topics"
    )

    data = []

    for row in rows:
        data.append({
            "word": row.word,
            "count": row.count,
            "score": row.score
        })

    df = pd.DataFrame(data)

    if len(df) == 0:
        return []

    df = df.sort_values("score", ascending=False)
    df = df.drop_duplicates("word")

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