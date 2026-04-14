FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir \
    flask==3.0.3 \
    flask-cors==4.0.1 \
    "elasticsearch>=7.17.0,<8.0.0" \
    cassandra-driver==3.29.1 \
    google-generativeai==0.8.3 \
    python-dotenv==1.0.1 \
    requests==2.32.3

COPY api/main.py ./api/main.py
COPY spark/utils/ ./spark/utils/
RUN touch ./spark/__init__.py ./spark/utils/__init__.py

ENV PYTHONPATH=/app
ENV GRPC_DNS_RESOLVER=native

EXPOSE 8000
CMD ["python", "api/main.py"]
