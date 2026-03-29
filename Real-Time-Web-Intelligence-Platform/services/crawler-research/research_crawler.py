import json
import time
import feedparser
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "research-topic"

# Reliable Research RSS Feeds
RESEARCH_FEEDS = [
    "https://www.sciencedaily.com/rss/computers_math/artificial_intelligence.xml",
    "https://www.sciencedaily.com/rss/computers_math/machine_learning.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml"
]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def fetch_research():

    print("\n🚀 Fetching Research Feeds...")

    for feed_url in RESEARCH_FEEDS:

        print("Checking:", feed_url)

        feed = feedparser.parse(feed_url)

        print("Entries found:", len(feed.entries))

        for entry in feed.entries[:10]:

            data = {
                "title": entry.title,
                "link": entry.link,
                "published": entry.get("published", ""),
                "source": feed_url
            }

            print("Sending Research:", data["title"])

            producer.send(TOPIC, data)

    producer.flush()


def main():

    print("🚀 Research crawler started")

    while True:
        fetch_research()
        time.sleep(30)


if __name__ == "__main__":
    main()