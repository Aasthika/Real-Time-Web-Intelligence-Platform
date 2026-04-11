import uuid
import os
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.sql.functions import trim, lower as lower_fn
import logging

from spark.utils.text_processing import clean_text, tokenize
from spark.utils.stopwords import remove_stopwords
from spark.utils.ranking import apply_enhanced_ranking
from spark.utils.alerts import check_batch_for_alerts

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ── Read config from environment (Docker sets these) ─────────────────────────
KAFKA_BROKER    = os.environ.get("KAFKA_BROKER",    "localhost:9092")
CASSANDRA_HOST  = os.environ.get("CASSANDRA_HOST",  "127.0.0.1")
ES_HOST         = os.environ.get("ES_HOST",         "localhost")

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("RealTimeWebIntelligence") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints/realtime") \
    .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "spark-streaming-job")) \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Load ML Model ─────────────────────────────────────────────────────────────
try:
    model = PipelineModel.load("/app/spark/models/category_classifier")
    logger.info("✅ ML model loaded")
except Exception as e:
    logger.warning(f"⚠️ ML model not found, will skip classification: {e}")
    model = None

make_uuid = udf(lambda: str(uuid.uuid4()), StringType())

# ── Kafka Schema ──────────────────────────────────────────────────────────────
schema = StructType([
    StructField("title",     StringType(), True),
    StructField("link",      StringType(), True),
    StructField("published", StringType(), True),
    StructField("source",    StringType(), True),
    StructField("user",      StringType(), True),
    StructField("post",      StringType(), True),
    StructField("timestamp", StringType(), True),
])

# ── Kafka Source Stream ───────────────────────────────────────────────────────
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "news-topic,blog-topic,research-topic,social-topic") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 50) \
    .option("failOnDataLoss", "false") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)", "topic AS kafka_topic") \
    .select(from_json(col("value"), schema).alias("data"), "kafka_topic").select("data.*", "kafka_topic")

text_df = parsed.withColumn("text", coalesce(col("title"), col("post")))

# ── Batch Processor ───────────────────────────────────────────────────────────
def process_batch(batch_df, batch_id):
    if batch_df.limit(1).count() == 0:
        return

    batch_df = batch_df.withColumn("timestamp", current_timestamp())
    batch_df = batch_df.filter(col("text").isNotNull()).filter(col("text") != "")

    if batch_df.limit(1).count() == 0:
        return

    logger.info(f"🚀 Processing Batch {batch_id}")

    # SINK 0: Cassandra Metadata
    try:
        meta_df = batch_df.withColumn("id", make_uuid()) \
                          .select("id", "title", "source", "published", "timestamp")
        meta_df.write.format("org.apache.spark.sql.cassandra") \
               .options(table="page_metadata", keyspace="realtime") \
               .mode("append").save()
    except Exception as e:
        logger.error(f"❌ Metadata Error: {e}")

    # SINK 1: Elasticsearch
    try:
        es_data = batch_df.withColumn("stable_id",
            coalesce(
                trim(lower_fn(col("link"))),
                md5(concat_ws("||", col("source"), col("title")))
            )
        )
        es_data.write.format("org.elasticsearch.spark.sql") \
            .option("es.resource", "web_intelligence") \
            .option("es.mapping.id", "stable_id") \
            .option("es.write.operation", "upsert") \
            .option("es.nodes", ES_HOST) \
            .option("es.port", "9200") \
            .option("es.nodes.wan.only", "true") \
            .mode("append").save()
    except Exception as e:
        logger.error(f"❌ ES Error: {e}")

    # SINK 2: Perfect Classification using Native Stream Origin
    try:
        # Instead of using an ML model that gets heavily biased by the hyperactive blog_crawler,
        # we perfectly map the exact category directly from the ground-truth Kafka stream!
        results_df = batch_df.withColumn("category",
            when(col("kafka_topic") == "news-topic", "News")
            .when(col("kafka_topic") == "blog-topic", "Blog")
            .when(col("kafka_topic") == "research-topic", "Research")
            .when(col("kafka_topic") == "social-topic", "Social")
            .otherwise("News")
        )

        cleaned      = clean_text(results_df, "text")
        tokenized    = tokenize(cleaned)
        no_stopwords = remove_stopwords(tokenized)

        # Retain category per word
        exploded = no_stopwords.select(explode(col("filtered_words")).alias("word"), col("category"))
        ranked_df = apply_enhanced_ranking(exploded.groupBy("word", "category").count())

        final_df = ranked_df.filter(col("word") != "") \
            .filter(length(col("word")) > 3) \
            .filter(~col("word").rlike("^[0-9]+$"))

        final_df.select("word", "category", "count", "final_score", "pop_score", "recency_score") \
            .write.format("org.apache.spark.sql.cassandra") \
            .options(table="trending_topics", keyspace="realtime") \
            .mode("append").save()

        logger.info(f"✅ Batch categorized words successfully.")
    except Exception as e:
        logger.error(f"❌ Trends/ML Error: {e}")

    # SINK 3: Alert Engine
    try:
        subs = spark.read.format("org.apache.spark.sql.cassandra") \
                    .options(table="user_alerts", keyspace="realtime").load().collect()
        if subs:
            alerts = check_batch_for_alerts(batch_df, [r.asDict() for r in subs])
            if alerts:
                alert_rows = [{
                    "alert_id":    str(uuid.uuid4()),
                    "user_id":     a['user_id'],
                    "topic":       a['topic'],
                    "match_text":  a['match'][:200],
                    "link":        a.get('link') or '',
                    "triggered_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                } for a in alerts]
                spark.createDataFrame(alert_rows) \
                     .write.format("org.apache.spark.sql.cassandra") \
                     .options(table="triggered_alerts", keyspace="realtime") \
                     .mode("append").save()
                logger.info(f"🚨 {len(alerts)} alerts triggered!")
    except Exception as e:
        logger.error(f"❌ Alert Error: {e}")

# ── Start Streaming ───────────────────────────────────────────────────────────
query = text_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/realtime") \
    .start()

query.awaitTermination()