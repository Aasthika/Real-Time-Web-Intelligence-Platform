from cassandra.cluster import Cluster

cluster = Cluster(["localhost"])
session = cluster.connect("realtime")


def get_trending():

    query = """
    SELECT word, count, score
    FROM trending_topics
    LIMIT 50
    """

    rows = session.execute(query)

    results = []

    for row in rows:
        results.append({
            "word": row.word,
            "count": row.count,
            "score": row.score
        })

    return results


def search_query(keyword):

    query = """
    SELECT word, count, score
    FROM trending_topics
    WHERE word=%s
    """

    rows = session.execute(query, [keyword])

    results = []

    for row in rows:
        results.append({
            "word": row.word,
            "count": row.count,
            "score": row.score
        })

    return results


def analytics():

    query = """
    SELECT COUNT(*) FROM trending_topics
    """

    rows = session.execute(query)

    for row in rows:
        return {
            "total_trending_words": row.count
        }