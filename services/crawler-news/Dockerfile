FROM python:3.11-slim
WORKDIR /app

# Only install what crawlers need — not full pyspark
COPY requirements.txt .
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    feedparser==6.0.11 \
    requests==2.32.3

# Copy the crawler script
COPY services/crawler-news/news_crawler.py ./news_crawler.py

# Copy spark/utils so logger_setup import works
COPY spark/utils/ ./spark/utils/
RUN touch ./spark/__init__.py ./spark/utils/__init__.py

ENV KAFKA_BROKER=kafka:29092
ENV PYTHONPATH=/app

CMD ["python", "news_crawler.py"]
