from cassandra.cluster import Cluster
from datetime import datetime
import time


session = None


# --------------------------------
# Cassandra Connection
# --------------------------------
def get_session():

    global session

    if session is None:

        print("Connecting Spark to Cassandra...")

        while True:
            try:
                cluster = Cluster(["cassandra"])
                session = cluster.connect("realtime")
                print("✅ Spark Connected to Cassandra")
                break

            except Exception as e:
                print("⏳ Spark waiting for Cassandra...")
                time.sleep(5)

    return session


# --------------------------------
# Write Trending
# --------------------------------
def write_trending(word, count, score, category):

    session = get_session()

    session.execute(
        """
        INSERT INTO trending_topics
        (word, timestamp, count, score, category)
        VALUES (%s, toTimestamp(now()), %s, %s, %s)
        """,
        (
            word,
            int(count),
            float(score),
            category
        )
    )


# --------------------------------
# Write Metadata
# --------------------------------
def write_metadata(title, source, published, timestamp):

    session = get_session()

    session.execute(
        """
        INSERT INTO metadata
        (id, title, source, published, timestamp)
        VALUES (uuid(), %s, %s, %s, %s)
        """,
        (
            title,
            source,
            published,
            timestamp
        )
    )


# --------------------------------
# Alerts
# --------------------------------
def get_alerts():

    session = get_session()

    rows = session.execute(
        "SELECT keyword FROM user_alerts"
    )

    return [row.keyword for row in rows]


# --------------------------------
# Trigger Alert
# --------------------------------
def write_triggered_alert(word):

    session = get_session()

    session.execute(
        """
        INSERT INTO triggered_alerts
        (word, timestamp)
        VALUES (%s, toTimestamp(now()))
        """,
        (word,)
    )