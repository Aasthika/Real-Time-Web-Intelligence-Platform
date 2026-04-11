from pyspark.sql import SparkSession
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.sql.functions import when, col

import os

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")

spark = SparkSession.builder \
    .appName("TrainClassifier") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .getOrCreate()

data_df = spark.read.format("org.apache.spark.sql.cassandra") \
               .options(table="page_metadata", keyspace="realtime").load()

# Drop nulls — Tokenizer crashes on null titles
data_df = data_df.filter(col("title").isNotNull()).filter(col("title") != "")

# Label by source
data_df = data_df.withColumn("label",
    when(col("source").contains("arxiv"), 2.0)
    .when(col("source").contains("medium") | col("source").contains("dev.to"), 1.0)
    .when(col("source") == "social", 3.0)
    .otherwise(0.0)
)

count = data_df.count()
print(f"✅ Training on {count} records")
if count < 10:
    print("❌ Not enough data! Run crawlers first.")
    spark.stop()
    exit()

tokenizer = RegexTokenizer(inputCol="title", outputCol="words", pattern="\\W", minTokenLength=2)
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)

pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, rf])
model = pipeline.fit(data_df)
model.write().overwrite().save("spark/models/category_classifier")
print("✅ Model trained and saved successfully!")