from cassandra.cluster import Cluster
from datetime import datetime

cluster = Cluster(["localhost"])
session = cluster.connect("realtime")


# -----------------------------
# Trending
# -----------------------------
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

    import pandas as pd

    df = pd.DataFrame(data)

    if len(df) == 0:
        return []

    df = df.sort_values("score", ascending=False)
    df = df.drop_duplicates("word")

    return df.head(20).to_dict("records")


# -----------------------------
# Search
# -----------------------------
def search_query(keyword):

    query = """
    SELECT word, count, score
    FROM trending_topics
    WHERE word=%s
    """

    rows = session.execute(query, [keyword])

    results = []

    for row in rows:
        results.append({
            "word": row.word,
            "count": row.count,
            "score": row.score
        })

    return results


# -----------------------------
# Analytics
# -----------------------------
def analytics():

    query = "SELECT COUNT(*) FROM trending_topics"

    rows = session.execute(query)

    for row in rows:
        return {
            "total_trending_words": row.count
        }


# -----------------------------
# Add Alert
# -----------------------------
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