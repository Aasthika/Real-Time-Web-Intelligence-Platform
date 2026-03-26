from pyspark.sql.functions import col, current_timestamp, unix_timestamp


# --------------------------------
# Popularity Ranking
# --------------------------------
def apply_popularity_ranking(df):

    return df.withColumn(
        "popularity_score",
        col("count").cast("double") * 1.0
    )


# --------------------------------
# Recency Ranking
# --------------------------------
def apply_recency_ranking(df):

    return df.withColumn(
        "recency_score",
        unix_timestamp(current_timestamp())
    )


# --------------------------------
# Final Ranking
# --------------------------------
def combine_ranking(df):

    return df.withColumn(
        "final_score",
        col("popularity_score") + col("recency_score") * 0.0001
    )