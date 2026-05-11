"""News Pulse RSS ingester.

This script pulls headlines from public RSS feeds every 45 seconds
and writes each batch as JSON-lines files to data/incoming/.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import feedparser

OUTPUT_DIR = Path("data/incoming")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FEEDS = [
    "http://feeds.bbci.co.uk/news/rss.xml",
    "https://feeds.reuters.com/reuters/topNews",
    "https://rss.cnn.com/rss/edition.rss",
    "https://www.aljazeera.com/xml/rss/all.xml",
]


def parse_feed(url):
    """Download one RSS feed and return clean headline records."""
    records = []

    feed = feedparser.parse(url)
    source = feed.feed.get("title", url)

    for entry in feed.entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()

        if not title or not link:
            continue

        records.append(
            {
                "source": source,
                "title": title,
                "url": link,
                "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )

    return records


def write_batch(records):
    """Write a batch of records as a JSON-lines file for Spark to read."""
    if not records:
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Write temporary file first so Spark does not read a half-written file
    temp_file = OUTPUT_DIR / f"batch_{timestamp}.tmp"

    # Spark reads this final file after it is fully written
    final_file = OUTPUT_DIR / f"batch_{timestamp}.json"

    with temp_file.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")

    temp_file.replace(final_file)

    print(f"Wrote {len(records)} records to {final_file}")


def main():
    """Continuously pull RSS headlines and write them into data/incoming."""
    print("Starting News Pulse ingester. Press Ctrl+C to stop.")

    while True:
        all_records = []

        for feed_url in FEEDS:
            try:
                records = parse_feed(feed_url)
                all_records.extend(records)
                print(f"Pulled {len(records)} records from {feed_url}")
            except Exception as exc:
                print(f"Failed to read feed {feed_url}: {exc}")

        if all_records:
            write_batch(all_records)
        else:
            print("No records found in this cycle.")

        print("Sleeping for 45 seconds before the next poll.\n")
        time.sleep(45)


if __name__ == "__main__":
    main()