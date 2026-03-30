FROM python:3.10

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir \
    kafka-python \
    feedparser \
    pandas

CMD ["python", "services/crawler-news/news_crawler.py"]