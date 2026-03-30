import json
import time
import feedparser
from kafka import KafkaProducer


KAFKA_BROKER = "kafka:9092"
TOPIC = "research-topic"


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


RESEARCH_FEEDS = [
    "https://www.sciencedaily.com/rss/computers_math/artificial_intelligence.xml",
    "https://www.sciencedaily.com/rss/computers_math/machine_learning.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml"
]


def fetch_research():

    print("🚀 Fetching Research...")

    for feed_url in RESEARCH_FEEDS:

        feed = feedparser.parse(feed_url)

        for entry in feed.entries[:10]:

            data = {
                "title": entry.title,
                "link": entry.link,
                "published": entry.get("published", ""),
                "source": feed_url
            }

            print("Sending Research:", data["title"], flush=True)
            producer.send(TOPIC, data)
            producer.flush()

            producer.send(TOPIC, data)


def main():

    print("🚀 Research crawler started")

    while True:
        fetch_research()
        time.sleep(30)


if __name__ == "__main__":
    main()