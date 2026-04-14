from pyspark.ml.feature import HashingTF, IDF




def apply_tfidf(df):


   hashing_tf = HashingTF(
       inputCol="filtered_words",
       outputCol="raw_features",
       numFeatures=1000
   )


   featurized = hashing_tf.transform(df)


   idf = IDF(
       inputCol="raw_features",
       outputCol="features"
   )


   idf_model = idf.fit(featurized)


   return idf_model.transform(featurized)