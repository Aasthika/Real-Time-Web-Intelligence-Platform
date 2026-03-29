from cassandra.cluster import Cluster
import uuid
from datetime import datetime


cluster = Cluster(["localhost"])
session = cluster.connect("realtime")


# --------------------------------
# Write Trending
# --------------------------------
# --------------------------------
# Write Trending
# --------------------------------
def write_trending(word, count, score, category="general"):

    if word is None:
        return

    # Get existing count
    rows = session.execute(
        """
        SELECT count FROM trending_topics
        WHERE word = %s
        LIMIT 1
        """,
        (word,)
    )

    existing_count = 0

    for row in rows:
        existing_count = row.count

    new_count = existing_count + int(count)

    # Update total count
    session.execute(
        """
        INSERT INTO trending_topics
        (word, count, score, category, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            word,
            new_count,
            float(score),
            str(category),
            datetime.utcnow()
        )
    )


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