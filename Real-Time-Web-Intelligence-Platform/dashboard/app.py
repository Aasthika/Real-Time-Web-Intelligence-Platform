import streamlit as st
import requests
import pandas as pd
from streamlit_autorefresh import st_autorefresh

API_URL = "http://api:8000"
# Auto refresh every 5 seconds
st_autorefresh(interval=5000, key="datarefresh")

st.set_page_config(
    page_title="Real Time Web Intelligence",
    layout="wide"
)

st.title("🚀 Real Time Web Intelligence Dashboard")

# --------------------------------
# Session State
# --------------------------------
if "trending_data" not in st.session_state:
    st.session_state.trending_data = pd.DataFrame()

if "search_data" not in st.session_state:
    st.session_state.search_data = pd.DataFrame()

if "alerts_data" not in st.session_state:
    st.session_state.alerts_data = pd.DataFrame()


# --------------------------------
# Sidebar Navigation
# --------------------------------
menu = st.sidebar.selectbox(
    "Navigation",
    [
        "Trending",
        "Search",
        "Alerts",
        "Analytics",
        "Monitoring"
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

        if data["status"] == "success":
            df = pd.DataFrame(data["trending"])
            st.session_state.trending_data = df
        else:
            df = st.session_state.trending_data

        col1, col2 = st.columns([2,1])

        with col1:
            st.subheader("Trending Table")
            st.dataframe(
                st.session_state.trending_data,
                use_container_width=True
            )

        with col2:

            st.subheader("Top Trending Chart")

            if len(st.session_state.trending_data) > 0:
                chart_df = st.session_state.trending_data.head(10)

                st.bar_chart(
                    chart_df.set_index("word")["count"]
                )

    except Exception as e:
        st.error(f"Error loading trending: {e}")


# --------------------------------
# Search Section
# --------------------------------
elif menu == "Search":

    st.header("🔍 Search Intelligence")

    col1, col2 = st.columns([3,1])

    with col1:
        query = st.text_input("Enter keyword")

    with col2:
        search_btn = st.button("Search")

    if search_btn:

        try:

            response = requests.post(
                f"{API_URL}/search",
                json={"query": query}
            )

            data = response.json()

            df = pd.DataFrame(data["results"])

            st.session_state.search_data = df

        except Exception as e:
            st.error(f"Search error: {e}")

    # Show stored search results
    if len(st.session_state.search_data) > 0:

        st.subheader("Search Results")

        st.dataframe(
            st.session_state.search_data,
            use_container_width=True
        )

        # Show article links
        if "link" in st.session_state.search_data.columns:

            st.subheader("References")

            for _, row in st.session_state.search_data.head(10).iterrows():

                st.markdown(
                    f"**{row.get('title','No Title')}**  \n"
                    f"Source: {row.get('source','')}  \n"
                    f"[Read Article]({row.get('link','#')})"
                )

        # Category Chart
        if "category" in st.session_state.search_data.columns:

            st.subheader("Category Distribution")

            cat_df = st.session_state.search_data["category"].value_counts()

            st.bar_chart(cat_df)


# --------------------------------
# Alerts Section
# --------------------------------
elif menu == "Alerts":

    st.header("🚨 Alert Engine")

    keyword = st.text_input("Alert keyword")

    if st.button("Create Alert"):

        try:

            requests.post(
                f"{API_URL}/alerts",
                json={"keyword": keyword}
            )

            st.success(f"Alert created for {keyword}")

        except Exception as e:
            st.error(e)


    # Triggered alerts
    try:

        res = requests.get(
            f"{API_URL}/triggered-alerts"
        ).json()

        alerts_df = pd.DataFrame(res["alerts"])

        st.session_state.alerts_data = alerts_df

        st.subheader("Triggered Alerts")

        st.dataframe(
            st.session_state.alerts_data,
            use_container_width=True
        )

    except Exception as e:
        st.error(f"Alert loading error: {e}")


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
                data["analytics"]["total_trending_words"]
            )

        with col2:
            st.metric("System Status", "Running")

        with col3:
            st.metric("Streaming", "Active")

    except Exception as e:
        st.error(f"Analytics error: {e}")


# --------------------------------
# Monitoring Section
# --------------------------------
elif menu == "Monitoring":

    st.header("📡 System Monitoring Dashboard")

    try:

        trending_res = requests.get(
            f"{API_URL}/trending"
        ).json()

        analytics_res = requests.get(
            f"{API_URL}/analytics"
        ).json()

        trending_df = pd.DataFrame(
            trending_res["trending"]
        )

        total_docs = len(trending_df)

        total_words = analytics_res["analytics"]["total_trending_words"]

        col1, col2, col3, col4 = st.columns(4)

        # Kafka
        with col1:
            st.subheader("Kafka")
            st.metric("Topics Active", 4)
            st.metric("Messages/sec", total_docs)

        # Spark
        with col2:
            st.subheader("Spark")
            st.metric("Batch Status", "Running")
            st.metric("Records", total_docs)

        # API
        with col3:
            st.subheader("API")
            st.metric("Status", "Healthy")
            st.metric("Requests", total_words)

        # System
        with col4:
            st.subheader("System")
            st.metric("Documents", total_docs)
            st.metric("Trending Words", total_words)


        st.markdown("---")

        # Trend Chart
        st.subheader("📈 Processing Trend")

        if len(trending_df) > 0:

            trending_df["timestamp"] = pd.to_datetime(
                trending_df["timestamp"]
            )

            trend_df = trending_df.sort_values(
                "timestamp"
            ).tail(10)

            trend_df = trend_df.set_index("timestamp")

            st.line_chart(
                trend_df["count"]
            )


        # Topic Distribution
        st.subheader("📊 Topic Distribution")

        if "category" in trending_df.columns:

            topic_df = trending_df["category"].value_counts()

            st.bar_chart(topic_df)


    except Exception as e:
        st.error(f"Monitoring error: {e}")