import streamlit as st
import requests
import pandas as pd
import time

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

        col1, col2 = st.columns([2,1])

        with col1:
            st.subheader("Trending Table")
            st.dataframe(df, width="stretch")

        with col2:

            st.subheader("Top Trending Chart")

            if len(df) > 0:
                chart_df = df.head(10)

                st.bar_chart(
                    chart_df.set_index("word")["count"]
                )

    except Exception as e:
        st.error(
            f"Error loading trending: {e}"
        )


# --------------------------------
# Search Section
# --------------------------------
elif menu == "Search":

    st.header("🔍 Search Intelligence")

    col1, col2 = st.columns([3,1])

    with col1:
        query = st.text_input(
            "Enter keyword"
        )

    with col2:
        search_btn = st.button("Search")

    if search_btn:

        try:

            response = requests.post(
                f"{API_URL}/search",
                json={"query": query}
            )

            data = response.json()

            df = pd.DataFrame(
                data["results"]
            )

            st.subheader("Search Results")

            st.dataframe(
                df,
                use_container_width=True
            )

            # Category Distribution
            if len(df) > 0:

                st.subheader("Category Distribution")

                cat_df = df["category"].value_counts()

                st.bar_chart(cat_df)

        except Exception as e:
            st.error(
                f"Search error: {e}"
            )


# --------------------------------
# Alerts Section
# --------------------------------
elif menu == "Alerts":

    st.header("🚨 Alert Engine")

    col1, col2 = st.columns([3,1])

    with col1:
        keyword = st.text_input(
            "Alert keyword"
        )

    with col2:
        alert_btn = st.button("Create Alert")

    if alert_btn:

        try:

            response = requests.post(
                f"{API_URL}/alerts",
                json={"keyword": keyword}
            )

            st.success(
                f"✅ Alert created for: {keyword}"
            )

        except Exception as e:
            st.error(
                f"Alert error: {e}"
            )

    st.info(
        "Alerts trigger automatically when keyword appears in real-time stream"
    )


# --------------------------------
# Analytics Section
# --------------------------------
elif menu == "Analytics":

    st.header("📊 Analytics Intelligence")

    try:

        response = requests.get(
            f"{API_URL}/analytics"
        )

        data = response.json()

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Total Trending Words",
                data["analytics"][
                    "total_trending_words"
                ]
            )

        with col2:
            st.metric(
                "System Status",
                "Running"
            )

        with col3:
            st.metric(
                "Streaming",
                "Active"
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
    time.sleep(5)
    st.rerun()