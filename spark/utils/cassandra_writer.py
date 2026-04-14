from cassandra.cluster import Cluster
import uuid
import time
from datetime import datetime


cluster = None
session = None


def get_session():
   global cluster, session
   attempts = 0
   while session is None and attempts < 5:
       try:
           cluster = Cluster(["127.0.0.1"])
           session = cluster.connect("realtime")
           print("✅ Cassandra Connected")
       except Exception as e:
           attempts += 1
           print(f"⚠️ Cassandra attempt {attempts} failed. Retrying...")
           time.sleep(5)
   return session




# --------------------------------
# Write Trending
# --------------------------------
def write_trending(word, count):
   session = get_session()   # ✅ FIX


   if word is None:
       return


   word = str(word).strip()


   if word == "":
       return


   if count is None:
       return


   session.execute(
       """
       INSERT INTO trending_topics (word, count)
       VALUES (%s, %s)
       """,
       (word, int(count))
   )




# --------------------------------
# Write Metadata
# --------------------------------
def write_metadata(title, source, published, timestamp):
   session = get_session()   # ✅ FIX


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
