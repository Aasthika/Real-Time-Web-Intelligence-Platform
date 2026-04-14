def write_to_es(df, index_name):
   df.write \
       .format("org.elasticsearch.spark.sql") \
       .option("es.resource", index_name) \
       .mode("append") \
       .save() 
