FROM apache/spark:3.5.1

USER root

WORKDIR /app

COPY . /app

ENV PYTHONPATH=/app:/app/spark

# Fix pip timeout + upgrade pip
RUN pip install --upgrade pip && \
    pip install --default-timeout=1000 --no-cache-dir \
    pandas \
    kafka-python \
    cassandra-driver \
    elasticsearch==8.11.1

# Spark Kafka Jars
COPY docker/jars/commons-pool2-2.11.1.jar /opt/spark/jars/
COPY docker/jars/kafka-clients-3.5.1.jar /opt/spark/jars/
COPY docker/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
COPY docker/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/

# Install netcat for service wait
RUN apt-get update -o Acquire::Retries=3 \
 && apt-get install -y --no-install-recommends netcat-openbsd \
 && rm -rf /var/lib/apt/lists*

CMD ["/bin/bash", "-c", "\
echo 'Waiting for Cassandra...' && \
until nc -z cassandra 9042; do sleep 5; done && \
echo 'Cassandra Ready 🚀' && \
sleep 10 && \
echo 'Starting Spark Streaming...' && \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--conf spark.driver.host=spark \
--conf spark.driver.bindAddress=0.0.0.0 \
--jars /opt/spark/jars/commons-pool2-2.11.1.jar,\
/opt/spark/jars/kafka-clients-3.5.1.jar,\
/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,\
/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
/app/spark/streaming/spark_streaming.py && \
echo 'Spark Finished — keeping container alive' && \
tail -f /dev/null \
"]