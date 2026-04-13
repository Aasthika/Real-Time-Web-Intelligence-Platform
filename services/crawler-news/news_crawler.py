import json
import time
import feedparser
from kafka import KafkaProducer
from spark.utils.logger_setup import get_logger
import os


KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
logger = get_logger("news-crawler")
TOPIC = "news-topic"


RSS_FEEDS = [
   "http://feeds.bbci.co.uk/news/rss.xml",
   "http://rss.cnn.com/rss/edition.rss",
   "https://techcrunch.com/feed/"
]


USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


producer = KafkaProducer(
   bootstrap_servers=KAFKA_BROKER,
   value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def fetch_news():
   for feed_url in RSS_FEEDS:
       try:
           feed = feedparser.parse(feed_url, agent=USER_AGENT)


           if not feed.entries:
               print(f"⚠️ No entries found for {feed_url} (might be temporarily blocked)")
               logger.warning(f"No entries for {feed_url}. Might be blocked.")
               continue


           for entry in feed.entries:
               data = {
                   "title": getattr(entry, 'title', 'No Title'),
                   "link": getattr(entry, 'link', ''),
                   "published": getattr(entry, 'published', ''),
                   "source": feed_url
               }
               print(f"📰 News Found: {data['title'][:50]}...")
               logger.info(f"Published to Kafka: {data['title'][:50]}")
               producer.send(TOPIC, data)
          
           time.sleep(2)


       except Exception as e:
           logger.error(f"Failed to fetch {feed_url}: {str(e)}", exc_info=True)
           print(f"❌ Error fetching {feed_url}: {e}")


def main():
   print("🚀 News Crawler started...")
   while True:
       fetch_news()
       print("🕒 Sleeping for 60 seconds before next crawl...")
       time.sleep(30) 


if __name__ == "__main__":
   main()
