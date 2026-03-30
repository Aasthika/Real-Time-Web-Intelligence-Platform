import json
import time
import feedparser
from kafka import KafkaProducer

# --------------------------------
# Kafka Connection (Retry)
# --------------------------------

KAFKA_BROKER = "kafka:9092"
TOPIC = "news-topic"

producer = None

for i in range(20):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("✅ Connected to Kafka")
        break
    except Exception:
        print("⏳ Waiting for Kafka...")
        time.sleep(5)


# --------------------------------
# RSS Feeds
# --------------------------------

RSS_FEEDS = [
    "http://feeds.bbci.co.uk/news/rss.xml",
    "http://rss.cnn.com/rss/edition.rss",
    "https://techcrunch.com/feed/"
]


# --------------------------------
# Fetch News
# --------------------------------

def fetch_news():

    for feed_url in RSS_FEEDS:

        feed = feedparser.parse(feed_url)

        for entry in feed.entries:

            data = {
                "title": entry.title,
                "link": entry.link,
                "published": entry.get("published", ""),
                "source": feed_url
            }

            print("Sending News:", data["title"])

            producer.send(TOPIC, data)


# --------------------------------
# Main
# --------------------------------

def main():

    print("🚀 News crawler started")

    while True:
        fetch_news()
        time.sleep(30)


if __name__ == "__main__":
    main()