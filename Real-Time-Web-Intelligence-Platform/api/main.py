from fastapi import FastAPI
from api.database import (
    get_trending,
    search_query,
    analytics
)

from api.models import SearchRequest, AlertRequest


app = FastAPI(
    title="Real Time Web Intelligence API",
    version="1.0"
)


# --------------------------------
# Search API
# --------------------------------
@app.post("/search")
def search(data: SearchRequest):

    try:

        results = search_query(data.query)

        return {
            "status": "success",
            "results": results
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }

# --------------------------------
# Trending API
# --------------------------------
@app.get("/trending")
def trending():

    results = get_trending()

    return {
        "status": "success",
        "trending": results
    }


# --------------------------------
# Alerts API
# --------------------------------
alerts = []


from api.database import add_alert


@app.post("/alerts")
def create_alert(data: AlertRequest):

    add_alert(data.keyword)

    return {
        "status": "alert added",
        "keyword": data.keyword
    }

# --------------------------------
# Analytics API
# --------------------------------
@app.get("/analytics")
def get_analytics():

    stats = analytics()

    return {
        "status": "success",
        "analytics": stats
    }