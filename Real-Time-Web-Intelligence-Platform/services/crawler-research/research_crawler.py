import json
import time
import feedparser
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "research-topic"

RESEARCH_FEEDS = [
    "http://export.arxiv.org/rss/cs.AI",
    "http://export.arxiv.org/rss/cs.LG",
    "http://export.arxiv.org/rss/stat.ML"
]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def fetch_research():
    for feed_url in RESEARCH_FEEDS:
        feed = feedparser.parse(feed_url)

        for entry in feed.entries:
            data = {
                "title": entry.title,
                "link": entry.link,
                "published": entry.get("published", ""),
                "source": feed_url
            }

            print("Sending Research:", data["title"])

            producer.send(TOPIC, data)


def main():
    while True:
        fetch_research()
        time.sleep(60)


if __name__ == "__main__":
    main()