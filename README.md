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

Set environment variables (minimum required: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`):

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

## Run

```
streamlit run app.py
```

## Notes

- For multi-warehouse analytics, select multiple warehouses or `ALL` in the sidebar.
- The live running view requires a single warehouse selection.
- Consider using a dedicated, least-privilege role for production use.
