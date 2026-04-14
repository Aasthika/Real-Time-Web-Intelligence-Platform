import json
import time
import feedparser
from kafka import KafkaProducer
from spark.utils.logger_setup import get_logger
import os
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
logger = get_logger("blog-crawler")
TOPIC = "blog-topic"


BLOG_FEEDS = [
   "https://medium.com/feed/tag/technology",
   "https://medium.com/feed/tag/ai",
   "https://dev.to/feed"
]


producer = KafkaProducer(
   bootstrap_servers=KAFKA_BROKER,
   value_serializer=lambda v: json.dumps(v).encode("utf-8")
)




def fetch_blogs():
   for feed_url in BLOG_FEEDS:
       feed = feedparser.parse(feed_url)


       for entry in feed.entries:
           data = {
               "title": entry.title,
               "link": entry.link if hasattr(entry, 'link') and entry.link else None,
               "published": entry.get("published", ""),
               "source": feed_url
           }
           logger.info(f"Published to Kafka: {data['title'][:50]}")
           print("Sending Blog:", data["title"])


           producer.send(TOPIC, data)




def main():
   while True:
       fetch_blogs()
       time.sleep(30)




if __name__ == "__main__":
   main()