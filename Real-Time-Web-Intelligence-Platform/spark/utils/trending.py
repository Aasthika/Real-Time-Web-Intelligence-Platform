from pyspark.sql.functions import explode, col


def detect_trending(df):

    words = df.select(
        explode(col("filtered_words")).alias("word")
    )

    trending = words.groupBy("word").count()

    return trending