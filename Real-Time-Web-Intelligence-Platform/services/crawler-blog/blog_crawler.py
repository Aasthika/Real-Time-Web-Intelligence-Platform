import json
import time
import feedparser
from kafka import KafkaProducer


KAFKA_BROKER = "kafka:9092"
TOPIC = "blog-topic"

producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("✅ Connected to Kafka", flush=True)
    except Exception as e:
        print("⏳ Waiting for Kafka...", e, flush=True)
        time.sleep(5)

BLOG_FEEDS = [
    "https://medium.com/feed/tag/technology",
    "https://medium.com/feed/tag/ai",
    "https://dev.to/feed"
]


def fetch_blogs():

    for feed_url in BLOG_FEEDS:

        feed = feedparser.parse(feed_url)

        for entry in feed.entries:

            data = {
                "title": entry.title,
                "link": entry.link,
                "published": entry.get("published", ""),
                "source": feed_url
            }

            print("Sending Blog:", data["title"], flush=True)
            producer.send(TOPIC, data)
            producer.flush()

            producer.send(TOPIC, data)


def main():

    print("🚀 Blog crawler started")

    while True:
        fetch_blogs()
        time.sleep(30)


if __name__ == "__main__":
    main()