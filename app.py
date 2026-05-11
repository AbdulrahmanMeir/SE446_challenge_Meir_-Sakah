"""Streamlit dashboard for News Pulse."""

import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from streamlit import st_autorefresh

from llm_summary import generate_summary

st.set_page_config(page_title="News Pulse - Live", layout="wide")
st.title("News Pulse - Live")
st.markdown("Live Spark streaming counts from incoming RSS headlines.")

st_autorefresh(interval=7000, key="news_pulse_refresh")

spark = (
    SparkSession.builder
    .appName("NewsPulseApp")
    .master("local[1]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

schema = "source STRING, title STRING, url STRING, ts TIMESTAMP"
stream = spark.readStream.schema(schema).json("data/incoming")

STOP_WORDS = [
    "the", "and", "for", "with", "from", "that", "this", "have",
    "will", "their", "about", "which", "news", "your", "more",
]


def start_queries():
    if st.session_state.get("queries_started"):
        return

    by_source = stream.groupBy("source").count()
    query_by_source = (
        by_source.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("by_source")
        .trigger(processingTime="10 seconds")
        .start()
    )

    by_window = (
        stream.withWatermark("ts", "2 hours")
        .groupBy(F.window("ts", "1 hour"))
        .count()
    )
    query_by_window = (
        by_window.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("by_window")
        .trigger(processingTime="10 seconds")
        .start()
    )

    normalized = F.lower(F.regexp_replace(F.col("title"), "[^a-z0-9\\s]", ""))
    words = F.explode(F.split(normalized, "\\s+"))
    top_words = (
        stream.select(words.alias("word"))
        .filter(F.length("word") > 3)
        .filter(~F.col("word").isin(STOP_WORDS))
        .groupBy("word")
        .count()
    )
    query_top_words = (
        top_words.writeStream
        .outputMode("complete")
        .format("memory")
        .queryName("top_words")
        .trigger(processingTime="10 seconds")
        .start()
    )

    st.session_state["queries_started"] = True
    st.session_state["query_names"] = ["by_source", "by_window", "top_words"]


def load_table(name):
    if not spark.catalog.tableExists(name):
        return None
    return spark.sql(f"SELECT * FROM {name}")


start_queries()

source_table = load_table("by_source")
window_table = load_table("by_window")
top_words_table = load_table("top_words")

if source_table is None or window_table is None or top_words_table is None:
    st.info("Waiting for streaming data... Start the ingester and wait for the first batch.")
    st.write("Run `python ingester.py` in one terminal and `python streaming_job.py` in another.")
    st.stop()

by_source_pd = source_table.orderBy(F.desc("count")).toPandas()
by_window_pd = window_table.select("window.start", "window.end", "count").orderBy("start").toPandas()
top_words_pd = top_words_table.orderBy(F.desc("count")).limit(20).toPandas()

keywords = top_words_pd["word"].head(7).tolist()
summary_key = tuple(keywords)
if st.session_state.get("last_keywords") != summary_key:
    st.session_state["last_keywords"] = summary_key
    st.session_state["summary_text"] = generate_summary(keywords)

summary_text = st.session_state.get("summary_text", "Waiting for a summary...")

with st.container():
    st.markdown("### Live summary")
    st.info(summary_text)

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Headlines by source")
    if by_source_pd.empty:
        st.write("No source counts available yet.")
    else:
        st.bar_chart(by_source_pd.set_index("source"))

with col2:
    st.markdown("### Trending headline words")
    if top_words_pd.empty:
        st.write("No word counts available yet.")
    else:
        st.table(top_words_pd)

st.markdown("### Hourly headline volume")
if by_window_pd.empty:
    st.write("No window data available yet.")
else:
    by_window_pd["start"] = by_window_pd["start"].astype(str)
    st.line_chart(by_window_pd.rename(columns={"start": "timestamp"}).set_index("timestamp")["count"])
