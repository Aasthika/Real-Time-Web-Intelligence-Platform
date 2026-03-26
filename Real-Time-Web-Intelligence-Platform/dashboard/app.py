import streamlit as st
import requests
import pandas as pd

API_URL = "http://127.0.0.1:8000"

st.set_page_config(
    page_title="Real Time Web Intelligence",
    layout="wide"
)

st.title("🚀 Real Time Web Intelligence Dashboard")


# --------------------------------
# Sidebar Navigation
# --------------------------------
menu = st.sidebar.selectbox(
    "Navigation",
    [
        "Trending",
        "Search",
        "Alerts",
        "Analytics"
    ]
)


# --------------------------------
# Trending Section
# --------------------------------
if menu == "Trending":

    st.header("🔥 Trending Topics")

    try:
        response = requests.get(
            f"{API_URL}/trending"
        )

        data = response.json()

        df = pd.DataFrame(
            data["trending"]
        )

        st.dataframe(
            df,
            use_container_width=True
        )

    except Exception as e:
        st.error(
            f"Error loading trending: {e}"
        )


# --------------------------------
# Search Section
# --------------------------------
elif menu == "Search":

    st.header("🔍 Search")

    query = st.text_input(
        "Enter keyword"
    )

    if st.button("Search"):

        try:

            response = requests.post(
                f"{API_URL}/search",
                json={"query": query}
            )

            data = response.json()

            df = pd.DataFrame(
                data["results"]
            )

            st.dataframe(
                df,
                use_container_width=True
            )

        except Exception as e:
            st.error(
                f"Search error: {e}"
            )


# --------------------------------
# Alerts Section
# --------------------------------
elif menu == "Alerts":

    st.header("🚨 Create Alert")

    keyword = st.text_input(
        "Alert keyword"
    )

    if st.button("Create Alert"):

        try:

            response = requests.post(
                f"{API_URL}/alerts",
                json={"keyword": keyword}
            )

            st.success(
                f"Alert created for: {keyword}"
            )

        except Exception as e:
            st.error(
                f"Alert error: {e}"
            )


# --------------------------------
# Analytics Section
# --------------------------------
elif menu == "Analytics":

    st.header("📊 Analytics")

    try:

        response = requests.get(
            f"{API_URL}/analytics"
        )

        data = response.json()

        st.metric(
            "Total Trending Words",
            data["analytics"][
                "total_trending_words"
            ]
        )

    except Exception as e:
        st.error(
            f"Analytics error: {e}"
        )


# --------------------------------
# Auto Refresh
# --------------------------------
st.sidebar.markdown("---")

refresh = st.sidebar.checkbox(
    "Auto Refresh (5s)"
)

if refresh:
    st.experimental_rerun()