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

    results = search_query(data.query)

    return {
        "status": "success",
        "results": results
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


@app.post("/alerts")
def create_alert(data: AlertRequest):

    alerts.append(data.keyword)

    return {
        "status": "alert added",
        "alerts": alerts
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