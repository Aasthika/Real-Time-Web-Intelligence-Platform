from pyspark.sql.functions import col, lit
from pyspark.sql import DataFrame

def apply_enhanced_ranking(df: DataFrame) -> DataFrame:
    # A basic ranking algorithm calculating popularity and recency scores based on count.
    df = df.withColumn("pop_score", col("count").cast("float") * 1.5)
    df = df.withColumn("recency_score", lit(1.0).cast("float"))
    df = df.withColumn("final_score", col("pop_score") + col("recency_score"))
    return df