from pyspark.ml.feature import StopWordsRemover

def remove_stopwords(df):
    custom_stops = StopWordsRemover.loadDefaultStopWords("english") + [
        "said", "says", "new", "use", "using", "used", "year", "years",
        "http", "https", "www", "com", "via", "also", "may", "will"
    ]
    remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="filtered_words",
        stopWords=custom_stops
    )
    return remover.transform(df)