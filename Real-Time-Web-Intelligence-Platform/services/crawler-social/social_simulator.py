import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "social-topic"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

users = [
    "data_scientist",
    "ml_engineer",
    "ai_researcher",
    "bigdata_dev",
    "cloud_engineer",
    "analytics_pro"
]

posts = [
    "AI is transforming healthcare",
    "Big Data pipelines are fun",
    "Kafka streaming is powerful",
    "Spark streaming performance improved",
    "Machine learning in production",
    "Cloud computing is the future",
    "Real time analytics is trending"
]


def generate_post():
    data = {
        "user": random.choice(users),
        "post": random.choice(posts),
        "timestamp": datetime.now().isoformat()
    }

    print("Sending Social:", data["post"])

    producer.send(TOPIC, data)


def main():
    while True:
        generate_post()
        time.sleep(2)


if __name__ == "__main__":
    main()