FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    feedparser==6.0.11 \
    requests==2.32.3

COPY services/crawler-research/research_crawler.py ./research_crawler.py
COPY spark/utils/ ./spark/utils/
RUN touch ./spark/__init__.py ./spark/utils/__init__.py

ENV KAFKA_BROKER=kafka:29092
ENV PYTHONPATH=/app

CMD ["python", "research_crawler.py"]
