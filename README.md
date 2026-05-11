# News Pulse

News Pulse is a local demo pipeline that ingests RSS headlines, transforms them with PySpark Structured Streaming, creates a simple LLM summary, and displays live results in Streamlit.

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Make sure the incoming directory exists:

```bash
mkdir -p data/incoming
```

On Windows, if Spark reports native Hadoop access errors, install `winutils.exe` and set `HADOOP_HOME`:

- Download `winutils.exe` for Hadoop 3.x
- Place it in `C:\hadoop\bin\winutils.exe`
- Set the environment variables in PowerShell:

```powershell
setx HADOOP_HOME "C:\hadoop"
setx PATH "%PATH%;C:\hadoop\bin"
```

3. (Optional) If you want LLM summaries, create an OpenAI key and set `OPENAI_API_KEY` before running Streamlit.

## Run the pipeline

Open three terminals:

- Terminal 1: `python ingester.py`
- Terminal 2: `python streaming_job.py`
- Terminal 3: `streamlit run app.py`

If you want LLM summaries, set `OPENAI_API_KEY` in your environment before starting the dashboard. If the key is missing, the app still works and shows a keyword-based fallback summary.

## Expected outputs

- `ingester.py` writes JSON-lines files into `data/incoming/` with `source`, `title`, `url`, and `ts` fields.
- `streaming_job.py` builds three live memory tables: `by_source`, `by_window`, and `top_words`.
- `app.py` displays:
  - a bar chart of headline counts by source,
  - a line chart of hourly headline volume,
  - a table of top headline words,
  - a live summary text panel.
- If the dashboard loads before data arrives, it shows a friendly waiting message instead of an error.

## Pipeline explanation

- `ingester.py` polls public RSS feeds every 45 seconds and writes batches as JSON-lines files into `data/incoming/`.
- The ingester uses feeds from BBC, Reuters, CNN, and Al Jazeera.
- `streaming_job.py` uses `spark.readStream` and `writeStream` to build live aggregations in Spark memory tables.
- `app.py` starts a local Streamlit dashboard that reads those memory tables and visualizes the live counts.
- `llm_summary.py` wraps OpenAI with a safe fallback so the dashboard never crashes if the API is unavailable.

## Demo checklist

- [ ] `python ingester.py` is running and writing JSON-line files into `data/incoming/`
- [ ] `python streaming_job.py` is running and producing Spark memory tables `by_source`, `by_window`, `top_words`
- [ ] `streamlit run app.py` opens the dashboard and refreshes every few seconds
- [ ] The dashboard shows source counts, hourly window counts, top words, and a summary panel

## Reflection summary

If the input grew 1000×, the first step to break would be the local in-memory aggregations and ingest process on a single laptop. The fix is to use Spark Structured Streaming with checkpointing and a scalable sink like Delta Lake or Kafka so state and input scale beyond local memory.
