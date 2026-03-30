from cassandra.cluster import Cluster
import uuid
from datetime import datetime


import time
from cassandra.cluster import Cluster

session = None

for i in range(20):
    try:
        cluster = Cluster(["cassandra"])
        session = cluster.connect("realtime")
        print("Connected to Cassandra")
        break
    except:
        print("Waiting for Cassandra...")
        time.sleep(5)


# --------------------------------
# Write Trending
# --------------------------------
def write_trending(word, count, score, category="general"):

    if word is None:
        return

    try:

        session.execute(
            """
            INSERT INTO trending_topics
            (word, timestamp, count, score, category)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                str(word),
                datetime.utcnow(),
                int(count),
                float(score),
                str(category)
            )
        )

        print(f"🔥 Written: {word} → {count}")

    except Exception as e:
        print("❌ Cassandra Write Error:", e)


# --------------------------------
# Write Metadata
# --------------------------------
def write_metadata(title, source, published, timestamp):

    if title is None:
        return

    session.execute(
        """
        INSERT INTO page_metadata
        (id, title, source, published, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            uuid.uuid4(),
            str(title),
            str(source),
            str(published),
            datetime.utcnow()
        )
    )

# --------------------------------
# Add Alert
# --------------------------------
def add_alert(keyword):

    session.execute(
        """
        INSERT INTO user_alerts (keyword, created_at)
        VALUES (%s, toTimestamp(now()))
        """,
        (keyword,)
    )


# --------------------------------
# Get Alerts
# --------------------------------
def get_alerts():

    rows = session.execute(
        "SELECT keyword FROM user_alerts"
    )

    alerts = []

    for row in rows:
        alerts.append(row.keyword)

    return alerts


# --------------------------------
# Trigger Alert
# --------------------------------
def write_triggered_alert(word):

    print(f"\n🚨 ALERT TRIGGERED: {word}")