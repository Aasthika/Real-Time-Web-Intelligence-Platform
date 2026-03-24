import re
from pyspark.sql.functions import col, lower, regexp_replace, split


def clean_text(df, column):
    """
    Clean raw text column
    """
    return df.withColumn(
        "clean_text",
        lower(
            regexp_replace(
                col(column),
                "[^a-zA-Z0-9\\s]",
                ""
            )
        )
    )


def tokenize(df):
    """
    Tokenize cleaned text
    """
    return df.withColumn(
        "tokens",
        split(col("clean_text"), " ")
    )