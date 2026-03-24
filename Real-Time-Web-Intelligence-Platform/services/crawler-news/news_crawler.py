import json
import time
import feedparser
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "news-topic"

RSS_FEEDS = [
    "http://feeds.bbci.co.uk/news/rss.xml",
    "http://rss.cnn.com/rss/edition.rss",
    "https://techcrunch.com/feed/"
]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


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

            print("Sending:", data["title"])

            producer.send(TOPIC, data)


def main():
    while True:
        fetch_news()
        time.sleep(30)


if __name__ == "__main__":
    main()