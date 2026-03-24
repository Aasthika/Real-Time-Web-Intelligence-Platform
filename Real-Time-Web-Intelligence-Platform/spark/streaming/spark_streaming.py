from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from spark.utils.text_processing import clean_text, tokenize
from spark.utils.stopwords import remove_stopwords
from spark.utils.tfidf import apply_tfidf
from spark.utils.trending import detect_trending


spark = SparkSession.builder \
    .appName("RealTimeWebIntelligence") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    ) \
    .getOrCreate()


schema = StructType([
    StructField("title", StringType(), True),
    StructField("link", StringType(), True),
    StructField("published", StringType(), True),
    StructField("source", StringType(), True),
    StructField("user", StringType(), True),
    StructField("post", StringType(), True),
    StructField("timestamp", StringType(), True),
])


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


# 🚀 Production foreachBatch processing

def process_batch(batch_df, batch_id):

    print(f"Processing batch {batch_id}")

    # Skip empty batch
    if batch_df.count() == 0:
        print("Empty batch — skipping")
        return

    cleaned = clean_text(batch_df, "text")

    tokenized = tokenize(cleaned)

    filtered = remove_stopwords(tokenized)

    # Skip empty tokens
    if filtered.count() == 0:
        print("No tokens — skipping")
        return

    tfidf = apply_tfidf(filtered)

    trending = detect_trending(filtered)

    trending.show(truncate=False)

query = text_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()


query.awaitTermination()