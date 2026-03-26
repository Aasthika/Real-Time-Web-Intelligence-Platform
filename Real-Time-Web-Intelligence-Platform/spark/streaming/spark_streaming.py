from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from spark.utils.text_processing import clean_text, tokenize
from spark.utils.stopwords import remove_stopwords
from spark.utils.tfidf import apply_tfidf
from spark.utils.trending import detect_trending

from spark.utils.cassandra_writer import write_metadata, write_trending

from spark.utils.ranking import (
    apply_recency_ranking,
    apply_popularity_ranking,
    combine_ranking
)

from spark.utils.cassandra_writer import (
    write_metadata,
    write_trending,
    get_alerts,
    write_triggered_alert
)

# --------------------------------
# Spark Session
# --------------------------------
spark = SparkSession.builder \
    .appName("RealTimeWebIntelligence") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


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
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "news-topic,blog-topic,research-topic,social-topic") \
    .option("startingOffsets", "latest") \
    .load()


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
    if batch_df.count() == 0:
        print("Empty batch — skipping")
        return

    # -----------------------------
    # Text Processing
    # -----------------------------
    cleaned = clean_text(batch_df, "text")

    tokenized = tokenize(cleaned)

    filtered = remove_stopwords(tokenized)

    if filtered.count() == 0:
        print("No tokens — skipping")
        return


    # -----------------------------
    # TF-IDF
    # -----------------------------
    tfidf = apply_tfidf(filtered)


    # -----------------------------
    # Trending Detection
    # -----------------------------
    trending = detect_trending(filtered)

    trending = trending.filter(
        (col("word").isNotNull()) &
        (col("word") != "")
    )


    # -----------------------------
    # Ranking System
    # -----------------------------
    ranked = apply_popularity_ranking(trending)

    ranked = apply_recency_ranking(ranked)

    ranked = combine_ranking(ranked)

    ranked = ranked.orderBy("final_score", ascending=False)


    # Show ranked output
    ranked.show(truncate=False)


    # -----------------------------
    # Write Trending to Cassandra
    # -----------------------------
    rows = ranked.collect()

    for row in rows:
        try:
            write_trending(
                row["word"],
                row["count"],
                row["final_score"]
            )
        except Exception as e:
            print("Cassandra Write Error:", e)


    # -----------------------------
    # Write Metadata
    # -----------------------------
    meta_rows = batch_df.collect()

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
    # -----------------------------
    # Alert Detection
    # -----------------------------
    alerts = get_alerts()

    alert_words = ranked.select("word").collect()

    for row in alert_words:
        if row["word"] in alerts:
            write_triggered_alert(row["word"])

# --------------------------------
# Streaming Query
# --------------------------------
query = text_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()


query.awaitTermination()