# Snowflake Query Monitor (Streamlit)

A lightweight Streamlit app to monitor Snowflake queries with analytics for top queries, long-running queries, query status, and live running queries.

## Features

- Status breakdown (success/failed/canceled/running) from `ACCOUNT_USAGE.QUERY_HISTORY`
- Top 10 queries by runtime, bytes scanned, and cloud services credits
- Long-running queries (>10 minutes)
- Live running queries via `SHOW QUERIES IN WAREHOUSE ...`
- Filters: time window, warehouse(s), user, query tag

## Requirements

- Python 3.9+
- Access to Snowflake `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`
- Permission to run `SHOW WAREHOUSES` and `SHOW QUERIES IN WAREHOUSE ...` (for live view)

Note: `ACCOUNT_USAGE` can lag by ~45–90 minutes.

## Setup

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configure

Option A: `config.json` (recommended for local dev)

Create a `config.json` file in the repo root (it is gitignored). You can copy `config.example.json`:

```json
{
  "account": "xxx",
  "user": "xxx",
  "password": "xxx",
  "warehouse": "FIVETRAN_WAREHOUSE",
  "role": "xxx",
  "database": "xxx",
  "schema": "xxx"
}
```

Option B: environment variables (minimum required: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`):

```
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_WAREHOUSE=FIVETRAN_WAREHOUSE
# optional
export SNOWFLAKE_ROLE=...
export SNOWFLAKE_DATABASE=...
export SNOWFLAKE_SCHEMA=...
```

Environment variables override values in `config.json` when both are provided.

Option C: Streamlit secrets (useful for Streamlit Cloud or local `.streamlit/secrets.toml`):

Create `.streamlit/secrets.toml`:

```toml
[snowflake]
account = "xxx"
user = "xxx"
password = "xxx"
warehouse = "FIVETRAN_WAREHOUSE"
role = "xxx"
database = "xxx"
schema = "xxx"
```

Priority order: environment variables > Streamlit secrets > `config.json`.

## Run

```
streamlit run app.py
```

## Notes

- For multi-warehouse analytics, select multiple warehouses or `ALL` in the sidebar.
- The live running view requires a single warehouse selection.
- The running fallback window (15/30/60/120 minutes) is used only when `SHOW QUERIES` is unavailable.
- The Cost Monitor tab shows hourly/daily warehouse credits and estimated per-query credits (allocated by each query’s share of total elapsed time within its warehouse-hour).
- Consider using a dedicated, least-privilege role for production use.
