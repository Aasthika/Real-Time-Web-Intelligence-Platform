from pyspark.sql.functions import explode, col, first, count


def detect_trending(df):

    words = df.select(
        explode(col("filtered_words")).alias("word"),
        col("title"),
        col("link"),
        col("source"),
        col("timestamp")
    )

    trending = words.groupBy("word").agg(
        count("*").alias("count"),
        first("title").alias("title"),
        first("link").alias("link"),
        first("source").alias("source"),
        first("timestamp").alias("timestamp")
    )

    return trending