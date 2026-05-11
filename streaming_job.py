"""Spark Structured Streaming job for News Pulse."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

STOP_WORDS = [
    "the", "and", "for", "with", "from", "that", "this", "have",
    "will", "their", "about", "which", "news", "will", "have",
    "your", "more", "than", "what", "when", "been", "were",
]


def main():
    spark = (
        SparkSession.builder
        .appName("NewsPulseStreaming")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("title", StringType(), True),
            StructField("url", StringType(), True),
            StructField("ts", TimestampType(), True),
        ]
    )

    stream = spark.readStream.schema(schema).json("data/incoming")

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

    print("Streaming queries started. Open the dashboard in another terminal.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
