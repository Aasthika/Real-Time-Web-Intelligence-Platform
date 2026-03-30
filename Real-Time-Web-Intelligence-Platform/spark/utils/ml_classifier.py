from pyspark.ml.feature import HashingTF, StringIndexer, Tokenizer, IndexToString
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.sql.functions import col


from cassandra.cluster import Cluster
import uuid
from datetime import datetime
import time


# --------------------------------
# Cassandra Connection
# --------------------------------
import time
from cassandra.cluster import Cluster

session = None

for i in range(20):
    try:
        cluster = Cluster(["cassandra"])
        session = cluster.connect("realtime")
        print("Connected to Cassandra")
        break
    except:
        print("Waiting for Cassandra...")
        time.sleep(5)


# --------------------------------
# Train Model
# --------------------------------
def train_model(spark):

    data = [
        ("ai machine learning deep learning", "AI"),
        ("big data hadoop spark", "BIGDATA"),
        ("cloud aws azure gcp", "CLOUD"),
        ("analytics dashboard reporting", "ANALYTICS"),
        ("neural networks ai", "AI"),
        ("spark streaming kafka", "BIGDATA"),
        ("aws cloud infrastructure", "CLOUD"),
        ("data analytics visualization", "ANALYTICS"),
    ]

    df = spark.createDataFrame(
        data,
        ["text", "label"]
    )

    tokenizer = Tokenizer(
        inputCol="text",
        outputCol="words"
    )

    hashingTF = HashingTF(
        inputCol="words",
        outputCol="features",
        numFeatures=1000
    )

    indexer = StringIndexer(
        inputCol="label",
        outputCol="labelIndex"
    )

    rf = RandomForestClassifier(
        labelCol="labelIndex",
        featuresCol="features",
        numTrees=20
    )

    labelConverter = IndexToString(
        inputCol="prediction",
        outputCol="category",
        labels=indexer.fit(df).labels
    )

    pipeline = Pipeline(
        stages=[
            tokenizer,
            hashingTF,
            indexer,
            rf,
            labelConverter
        ]
    )

    model = pipeline.fit(df)

    return model


# --------------------------------
# Classify
# --------------------------------
def classify(df, model):

    df = df.withColumn(
        "text",
        col("word")
    )

    predictions = model.transform(df)

    return predictions


# --------------------------------
# Write Metadata
# --------------------------------
def write_metadata(title, source, published, timestamp):

    if title is None:
        return

    session.execute(
        """
        INSERT INTO page_metadata
        (id, title, source, published, timestamp)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            uuid.uuid4(),
            str(title),
            str(source),
            str(published),
            datetime.utcnow()
        )
    )


# --------------------------------
# Add Alert
# --------------------------------
def add_alert(keyword):

    session.execute(
        """
        INSERT INTO user_alerts (keyword, created_at)
        VALUES (%s, toTimestamp(now()))
        """,
        (keyword,)
    )


# --------------------------------
# Get Alerts
# --------------------------------
def get_alerts():

    rows = session.execute(
        "SELECT keyword FROM user_alerts"
    )

    alerts = []

    for row in rows:
        alerts.append(row.keyword)

    return alerts


# --------------------------------
# Trigger Alert
# --------------------------------
def write_triggered_alert(word):

    print(f"\n🚨 ALERT TRIGGERED: {word}")