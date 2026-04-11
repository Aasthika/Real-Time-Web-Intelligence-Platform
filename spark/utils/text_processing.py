import re
from pyspark.sql.functions import col, lower, regexp_replace, split
from pyspark.sql.functions import col, lower, regexp_replace, split, coalesce, lit




def clean_text(df, column):
   df = df.withColumn(
       "clean_text",
       lower(
           regexp_replace(
               coalesce(col(column), lit("")),
               "[^a-zA-Z0-9\\s]",
               ""
           )
       )
   )
   return df.filter(col("clean_text") != "")  # remove rows that became empty




def tokenize(df):
   """
   Tokenize cleaned text
   """
   return df.withColumn(
       "tokens",
       split(col("clean_text"), " ")
   )
 