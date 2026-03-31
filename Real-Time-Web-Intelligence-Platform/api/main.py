from fastapi import FastAPI
from api.database import (
    get_trending,
    search_query,
    analytics,
    add_alert
)

from api.models import SearchRequest, AlertRequest


app = FastAPI(
    title="Real Time Web Intelligence API",
    version="1.0"
)


# --------------------------------
# Health API (Important for Render)
# --------------------------------
@app.get("/health")
def health():
    return {
        "status": "ok"
    }


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

    try:

        results = get_trending()

        return {
            "status": "success",
            "trending": results
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }


# --------------------------------
# Alerts API
# --------------------------------
@app.post("/alerts")
def create_alert(data: AlertRequest):

    try:

        add_alert(data.keyword)

        return {
            "status": "alert added",
            "keyword": data.keyword
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }


# --------------------------------
# Analytics API
# --------------------------------
@app.get("/analytics")
def get_analytics():

    try:

        stats = analytics()

        return {
            "status": "success",
            "analytics": stats
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }