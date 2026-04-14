from elasticsearch import Elasticsearch
from datetime import datetime

# --------------------------------
# Elasticsearch Connection
# --------------------------------
es = Elasticsearch("http://elasticsearch:9200")

INDEX_NAME = "web_intelligence"


# --------------------------------
# Write to Elasticsearch
# --------------------------------
def write_to_elasticsearch(
    word,
    count,
    score,
    category,
    title=None,
    link=None,
    source=None
):

    try:

        doc = {
            "word": word,
            "count": int(count),
            "score": float(score),
            "category": category,
            "title": title,
            "link": link,
            "source": source,
            "timestamp": datetime.utcnow()
        }

        es.index(
            index=INDEX_NAME,
            document=doc
        )

    except Exception as e:
        print("Elasticsearch Write Error:", e)