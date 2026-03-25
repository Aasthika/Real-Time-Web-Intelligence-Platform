from pyspark.ml.feature import StopWordsRemover


def remove_stopwords(df):
    remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="filtered_words"
    )

    return remover.transform(df)