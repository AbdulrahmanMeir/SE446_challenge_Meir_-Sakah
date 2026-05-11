"""Streamlit dashboard for News Pulse."""

import os
import time
from pathlib import Path

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from llm_summary import generate_summary


# Windows Hadoop fix
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

BASE_DIR = Path(__file__).resolve().parent
INCOMING_PATH = BASE_DIR / "data" / "incoming"
INCOMING_PATH.mkdir(parents=True, exist_ok=True)

SPARK_INCOMING_PATH = INCOMING_PATH.as_uri()

STOP_WORDS = [
    "the", "and", "for", "with", "from", "that", "this", "have",
    "will", "their", "about", "which", "news", "your", "more",
    "you", "are", "was", "were", "has", "had", "been", "into",
    "after", "over", "under", "than", "they", "them", "his",
    "her", "its", "our", "out", "not", "but", "who", "what",
    "when", "where", "why", "how", "said", "says"
]


@st.cache_resource
def start_spark_stream():
    """Start one Spark Structured Streaming query into a memory table."""

    spark = (
        SparkSession.builder
        .appName("NewsPulseApp")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # Stop old query if Streamlit was restarted
    for query in spark.streams.active:
        if query.name == "news_raw":
            query.stop()

    schema = "source STRING, title STRING, url STRING, ts TIMESTAMP"

    stream = (
        spark.readStream
        .schema(schema)
        .option("maxFilesPerTrigger", 1)
        .json(SPARK_INCOMING_PATH)
    )

    query = (
        stream.writeStream
        .outputMode("append")
        .format("memory")
        .queryName("news_raw")
        .trigger(processingTime="5 seconds")
        .start()
    )

    return spark, query


def safe_sql_to_pandas(spark, query):
    """Run Spark SQL safely."""
    try:
        return spark.sql(query).toPandas()
    except Exception:
        return pd.DataFrame()


st.set_page_config(page_title="News Pulse - Live", layout="wide")

st.title("News Pulse - Live Dashboard")
st.markdown("RSS headlines → Spark Structured Streaming → LLM summary → Streamlit dashboard")

st.caption(f"Reading files from: `{INCOMING_PATH}`")
st.caption(f"Spark path: `{SPARK_INCOMING_PATH}`")

spark, stream_query = start_spark_stream()

status_box = st.empty()
summary_box = st.empty()
source_box = st.empty()
words_box = st.empty()
window_box = st.empty()
debug_box = st.empty()

last_keywords = None
last_summary = "Waiting for summary..."

while True:
    if stream_query.exception() is not None:
        status_box.error("Spark streaming query failed.")
        debug_box.code(str(stream_query.exception()))
        st.stop()

    # Raw streamed records
    raw_df = safe_sql_to_pandas(
        spark,
        """
        SELECT source, title, url, ts
        FROM news_raw
        """
    )

    if raw_df.empty:
        files = list(INCOMING_PATH.glob("*"))
        status_box.warning(
            f"Waiting for Spark micro-batch... Files found: {len(files)}"
        )

        debug_box.code(
            f"""
Query: {stream_query.name}
Active: {stream_query.isActive}
Status: {stream_query.status}
Recent progress count: {len(stream_query.recentProgress)}
Files in incoming folder: {len(files)}
"""
        )

        time.sleep(5)
        continue

    status_box.success(f"Live streaming data is running. Records loaded: {len(raw_df)}")
    debug_box.empty()

    # Source chart
    by_source_pd = safe_sql_to_pandas(
        spark,
        """
        SELECT source, COUNT(*) AS count
        FROM news_raw
        GROUP BY source
        ORDER BY count DESC
        """
    )

    # Window chart
    by_window_pd = safe_sql_to_pandas(
        spark,
        """
        SELECT 
            window(ts, '1 hour').start AS start,
            COUNT(*) AS count
        FROM news_raw
        GROUP BY window(ts, '1 hour')
        ORDER BY start
        """
    )

    # Top words from raw title table
    titles_df = spark.sql("SELECT title FROM news_raw")

    normalized = F.lower(
        F.regexp_replace(F.col("title"), r"[^a-zA-Z0-9\s]", "")
    )

    words_spark_df = (
        titles_df
        .select(
            F.explode(
                F.split(normalized, r"\s+")
            ).alias("word")
        )
        .filter(F.length("word") > 3)
        .filter(~F.col("word").isin(STOP_WORDS))
        .groupBy("word")
        .count()
        .orderBy(F.desc("count"))
        .limit(20)
    )

    top_words_pd = words_spark_df.toPandas()

    # Summary
    with summary_box.container():
        st.markdown("### Live LLM Summary")

        if top_words_pd.empty:
            st.info("Waiting for keywords...")
        else:
            keywords = top_words_pd["word"].head(15).tolist()

            if keywords != last_keywords:
                last_keywords = keywords
                last_summary = generate_summary(keywords)

            st.info(last_summary)

    col1, col2 = st.columns(2)

    with source_box.container():
        with col1:
            st.markdown("### Headlines by Source")
            if by_source_pd.empty:
                st.write("No source data yet.")
            else:
                st.bar_chart(by_source_pd.set_index("source")["count"])

        with col2:
            st.markdown("### Trending Headline Words")
            if top_words_pd.empty:
                st.write("No word data yet.")
            else:
                st.dataframe(top_words_pd, use_container_width=True)

    with window_box.container():
        st.markdown("### Hourly Headline Volume")
        if by_window_pd.empty:
            st.write("No hourly volume data yet.")
        else:
            by_window_pd["start"] = by_window_pd["start"].astype(str)
            st.line_chart(by_window_pd.set_index("start")["count"])

    time.sleep(5)