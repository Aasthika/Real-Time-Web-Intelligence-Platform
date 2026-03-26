from cassandra.cluster import Cluster
import uuid
from datetime import datetime


cluster = Cluster(["localhost"])
session = cluster.connect("realtime")


# --------------------------------
# Write Trending
# --------------------------------
def write_trending(word, count, score):

    if word is None:
        return

    session.execute(
        """
        INSERT INTO trending_topics
        (word, count, score)
        VALUES (%s, %s, %s)
        """,
        (
            word,
            int(count),
            float(score)
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