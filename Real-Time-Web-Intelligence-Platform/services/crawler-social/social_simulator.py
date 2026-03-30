import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer


KAFKA_BROKER = "kafka:9092"
TOPIC = "social-topic"


producer = None

while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("✅ Connected to Kafka", flush=True)
    except Exception as e:
        print("⏳ Waiting for Kafka...", e)
        time.sleep(5)


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

    print("Sending Social:", data["post"], flush=True)

    producer.send(TOPIC, data)
    producer.flush()


def main():

    print("🚀 Social crawler started", flush=True)

    while True:
        generate_post()
        time.sleep(2)


if __name__ == "__main__":
    main()