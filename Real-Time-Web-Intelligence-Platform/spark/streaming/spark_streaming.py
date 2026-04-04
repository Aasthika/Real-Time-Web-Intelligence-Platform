from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from spark.utils.text_processing import clean_text, tokenize
from spark.utils.stopwords import remove_stopwords
from spark.utils.tfidf import apply_tfidf
from spark.utils.trending import detect_trending

from spark.utils.ranking import (
    apply_recency_ranking,
    apply_popularity_ranking,
    combine_ranking
)

from spark.utils.ml_classifier import train_model, classify

from spark.utils.cassandra_writer import (
    write_metadata,
    write_trending,
    get_alerts,
    write_triggered_alert
)

from spark.utils.elasticsearch_writer import write_to_elasticsearch


# --------------------------------
# Spark Session (Fixed)
# --------------------------------
spark = SparkSession.builder \
    .appName("RealTimeWebIntelligence") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .config(
        "spark.sql.streaming.checkpointLocation",
        "/tmp/checkpoint"
    ) \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🚀 Spark Session Started")


# --------------------------------
# Load ML Model
# --------------------------------
print("Loading ML Model...")
model = train_model(spark)
print("✅ ML Model Loaded")


# --------------------------------
# Schema
# --------------------------------
schema = StructType([
    StructField("title", StringType(), True),
    StructField("link", StringType(), True),
    StructField("published", StringType(), True),
    StructField("source", StringType(), True),
    StructField("user", StringType(), True),
    StructField("post", StringType(), True),
    StructField("timestamp", StringType(), True),
])


# --------------------------------
# Kafka Stream
# --------------------------------
print("Connecting to Kafka...")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option(
        "subscribe",
        "news-topic,blog-topic,research-topic,social-topic"
    ) \
    .option("startingOffsets", "latest") \
    .load()

print("✅ Kafka Connected")


json_df = df.selectExpr(
    "CAST(value AS STRING)"
)


parsed = json_df.select(
    from_json(
        col("value"),
        schema
    ).alias("data")
).select("data.*")


text_df = parsed.withColumn(
    "text",
    coalesce(col("title"), col("post"))
)


# --------------------------------
# Production Batch Processing
# --------------------------------
def process_batch(batch_df, batch_id):

    print(f"\n🚀 Processing batch {batch_id}")

    # Skip empty batch
    if batch_df.rdd.isEmpty():
        print("Empty batch — skipping")
        return


    # --------------------------------
    # Text Processing
    # --------------------------------
    cleaned = clean_text(batch_df, "text")

    tokenized = tokenize(cleaned)

    filtered = remove_stopwords(tokenized)

    if filtered.rdd.isEmpty():
        print("No tokens — skipping")
        return


    # --------------------------------
    # TF-IDF
    # --------------------------------
    tfidf = apply_tfidf(filtered)


    # --------------------------------
    # Trending Detection
    # --------------------------------
    trending = detect_trending(filtered)

    trending = trending.filter(
        (col("word").isNotNull()) &
        (col("word") != "")
    )


    # --------------------------------
    # Ranking
    # --------------------------------
    ranked = apply_popularity_ranking(trending)

    ranked = apply_recency_ranking(ranked)

    ranked = combine_ranking(ranked)

    ranked = ranked.orderBy(
        "final_score",
        ascending=False
    )


    # --------------------------------
    # Classification
    # --------------------------------
    classified = classify(ranked, model)

    print("✅ Classification completed")


    # --------------------------------
    # Write Trending
    # --------------------------------
    rows = classified.limit(50).collect()

    print(f"Writing {len(rows)} trending words...")

    for row in rows:

        try:

            write_trending(
                row["word"],
                row["count"],
                row["final_score"],
                row["category"]
            )

            write_to_elasticsearch(
                row["word"],
                row["count"],
                row["final_score"],
                row["category"]
            )

            print(f"🔥 Written: {row['word']} → {row['count']}")

        except Exception as e:
            print("Write Error:", e)


    # --------------------------------
    # Write Metadata
    # --------------------------------
    meta_rows = batch_df.limit(100).collect()

    for row in meta_rows:

        try:

            write_metadata(
                row["title"],
                row["source"],
                row["published"],
                row["timestamp"]
            )

        except Exception as e:
            print("Metadata Write Error:", e)


    # --------------------------------
    # Alert Detection
    # --------------------------------
    alerts = get_alerts()

    alert_words = classified \
        .select("word") \
        .limit(50) \
        .collect()

    for row in alert_words:

        if row["word"] in alerts:

            write_triggered_alert(
                row["word"]
            )

            print(
                f"🚨 ALERT TRIGGERED: {row['word']}"
            )


# --------------------------------
# Streaming Query
# --------------------------------
print("🚀 Starting Streaming Query...")

query = text_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="8 seconds") \
    .start()


print("✅ Streaming Started")

query.awaitTermination()