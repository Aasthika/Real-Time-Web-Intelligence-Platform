from pyspark.sql.functions import col


def check_batch_for_alerts(batch_df, subscriptions):
   """
   batch_df: The current Spark DataFrame (cleaned/tokenized)
   subscriptions: List of dicts from Cassandra [{'user_id': '...', 'topic': '...'}]
   """
   triggered = []
  
   # We collect the titles/posts to check them against topics
   articles = batch_df.select("text", "link").collect()
  
   for article in articles:
       content = article['text'].lower()
       for sub in subscriptions:
           topic = sub['topic'].lower()
           if topic in content:
               triggered.append({
                   "user_id": sub['user_id'],
                   "topic": sub['topic'],
                   "match": article['text'],
                   "link": article['link']
               })
   return triggered