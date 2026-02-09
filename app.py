import json
import os
from datetime import date, datetime, time, timedelta

import pandas as pd
import streamlit as st
import snowflake.connector
from snowflake.connector import errors as sf_errors

DEFAULT_WAREHOUSE = "FIVETRAN_WAREHOUSE"
LONG_RUNNING_MS = 10 * 60 * 1000


st.set_page_config(page_title="Snowflake Query Monitor", layout="wide")


@st.cache_resource
def get_connection():
    cfg = load_config()
    missing = [k for k, v in cfg.items() if k in {"account", "user", "password"} and not v]
    if missing:
        st.error("Missing Snowflake credentials (config.json or env vars): " + ", ".join(missing))
        st.stop()
    return snowflake.connector.connect(**{k: v for k, v in cfg.items() if v})


def load_config() -> dict:
    cfg = {}
    secrets_cfg = {}
    config_path = os.path.join(os.getcwd(), "config.json")
    if os.path.isfile(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f) or {}
        except (OSError, json.JSONDecodeError):
            st.warning("Could not read config.json. Falling back to environment variables.")
            cfg = {}

    try:
        secrets_cfg = dict(st.secrets.get("snowflake", {}))
    except Exception:
        secrets_cfg = {}

    env_override = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
        "user": os.getenv("SNOWFLAKE_USER", ""),
        "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
        "database": os.getenv("SNOWFLAKE_DATABASE", ""),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", ""),
        "role": os.getenv("SNOWFLAKE_ROLE", ""),
    }
    merged = {}
    merged.update(cfg)
    merged.update(secrets_cfg)
    for key, value in env_override.items():
        if value:
            merged[key] = value

    if "warehouse" not in merged or not merged["warehouse"]:
        merged["warehouse"] = DEFAULT_WAREHOUSE
    return merged

@st.cache_data(ttl=300)
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, params or {})
        df = cur.fetch_pandas_all()
    finally:
        cur.close()
    return df


def run_show_queries(warehouse: str) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()
    try:
        try:
            cur.execute(
                "SHOW QUERIES IN WAREHOUSE IDENTIFIER(%(warehouse)s)",
                {"warehouse": warehouse},
            )
            cur.execute("SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))")
            df = cur.fetch_pandas_all()
        except sf_errors.ProgrammingError as exc:
            st.warning(
                "Live running queries are unavailable. "
                "Ensure your role has MONITOR privilege and that SHOW QUERIES "
                "is supported in your account."
            )
            st.caption(f"Snowflake error: {exc}")
            df = pd.DataFrame()
    finally:
        cur.close()
    return df


@st.cache_data(ttl=300)
def list_warehouses() -> list[str]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SHOW WAREHOUSES")
        cur.execute("SELECT NAME FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ORDER BY NAME")
        rows = cur.fetchall()
    finally:
        cur.close()
    return [r[0] for r in rows]


@st.cache_data(ttl=60)
def run_running_queries_fallback(warehouse: str, minutes: int) -> pd.DataFrame:
    sql = """
    SELECT
      QUERY_ID,
      USER_NAME,
      WAREHOUSE_NAME,
      START_TIME,
      TOTAL_ELAPSED_TIME,
      EXECUTION_STATUS,
      QUERY_TEXT
    FROM TABLE(
      INFORMATION_SCHEMA.QUERY_HISTORY(
        END_TIME_RANGE_START=>DATEADD('minute', -%(minutes)s, CURRENT_TIMESTAMP())
      )
    )
    WHERE WAREHOUSE_NAME = %(warehouse)s
      AND END_TIME IS NULL
    ORDER BY START_TIME DESC
    """
    return run_query(sql, {"warehouse": warehouse, "minutes": minutes})


def to_ts_range(start_d: date, end_d: date) -> tuple[str, str]:
    start_ts = datetime.combine(start_d, time.min)
    end_ts = datetime.combine(end_d, time.max)
    return start_ts.isoformat(sep=" "), end_ts.isoformat(sep=" ")


st.title("Snowflake Query Monitor")

with st.sidebar:
    st.header("Filters")
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()
    preset = st.selectbox(
        "Time window",
        ["24h", "7d", "30d", "Custom"],
        index=1,
        help="ACCOUNT_USAGE can lag by ~45–90 minutes.",
    )

    today = date.today()
    if preset == "24h":
        start_date = today - timedelta(days=1)
        end_date = today
    elif preset == "7d":
        start_date = today - timedelta(days=7)
        end_date = today
    elif preset == "30d":
        start_date = today - timedelta(days=30)
        end_date = today
    else:
        start_date, end_date = st.date_input("Date range", value=(today - timedelta(days=7), today))

    default_wh = os.getenv("SNOWFLAKE_WAREHOUSE", DEFAULT_WAREHOUSE)
    wh_options = ["ALL"]
    try:
        wh_options.extend(list_warehouses())
    except Exception:
        wh_options.append(default_wh)
    if default_wh not in wh_options:
        wh_options.append(default_wh)
    wh_options = list(dict.fromkeys(wh_options))
    warehouses = st.multiselect(
        "Warehouses",
        options=wh_options,
        default=[default_wh] if default_wh in wh_options else [wh_options[0]],
        help="Select one or more for analytics. Live running view needs a single warehouse.",
    )
    fallback_window = st.selectbox(
        "Running fallback window",
        options=[15, 30, 60, 120],
        index=2,
        help="Used only when SHOW QUERIES is unavailable.",
    )
    user_filter = st.text_input("User (optional)")
    query_tag = st.text_input("Query tag (optional)")

start_ts, end_ts = to_ts_range(start_date, end_date)

base_filters = [
    "START_TIME >= %(start_ts)s",
    "START_TIME <= %(end_ts)s",
]
params = {"start_ts": start_ts, "end_ts": end_ts}

if warehouses and "ALL" not in warehouses:
    wh_placeholders = []
    for idx, wh in enumerate(warehouses):
        key = f"wh_{idx}"
        params[key] = wh
        wh_placeholders.append(f"%({key})s")
    base_filters.append(f"WAREHOUSE_NAME IN ({', '.join(wh_placeholders)})")

if user_filter:
    base_filters.append("USER_NAME = %(user_name)s")
    params["user_name"] = user_filter
if query_tag:
    base_filters.append("QUERY_TAG = %(query_tag)s")
    params["query_tag"] = query_tag

base_where = " AND ".join(base_filters)
wh_filter_sql = ""
wh_params: dict[str, str] = {}
if warehouses and "ALL" not in warehouses:
    wh_placeholders = []
    for idx, wh in enumerate(warehouses):
        key = f"wh_{idx}"
        wh_params[key] = wh
        wh_placeholders.append(f"%({key})s")
    wh_filter_sql = f"AND WAREHOUSE_NAME IN ({', '.join(wh_placeholders)})"

tab_overview, tab_cost = st.tabs(["Overview", "Cost Monitor"])

with tab_overview:
    st.subheader("Status Overview")
    status_sql = f"""
    SELECT
      EXECUTION_STATUS,
      COUNT(*) AS QUERY_COUNT
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE {base_where}
    GROUP BY 1
    ORDER BY QUERY_COUNT DESC
    """
    status_df = run_query(status_sql, params)
    status_col1, status_col2 = st.columns([1, 2])
    with status_col1:
        st.dataframe(status_df, use_container_width=True)
    with status_col2:
        if not status_df.empty:
            st.bar_chart(status_df.set_index("EXECUTION_STATUS"))

    st.subheader("Top 10 Queries")

    metric_sql = f"""
    SELECT
      QUERY_ID,
      USER_NAME,
      WAREHOUSE_NAME,
      START_TIME,
      TOTAL_ELAPSED_TIME,
      BYTES_SCANNED,
      CREDITS_USED_CLOUD_SERVICES,
      QUERY_TEXT
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE {base_where}
    """
    base_df = run_query(metric_sql, params)

    st.caption("Top 10 by total runtime (ms)")
    df = base_df.sort_values("TOTAL_ELAPSED_TIME", ascending=False).head(10)
    st.dataframe(df, use_container_width=True)
    st.divider()

    st.caption("Top 10 by bytes scanned")
    df = base_df.sort_values("BYTES_SCANNED", ascending=False).head(10)
    st.dataframe(df, use_container_width=True)
    st.divider()

    st.caption("Top 10 by cloud services credits")
    df = base_df.sort_values("CREDITS_USED_CLOUD_SERVICES", ascending=False).head(10)
    st.dataframe(df, use_container_width=True)

    st.subheader("Long-Running Queries (>10 minutes)")
    long_sql = f"""
    SELECT
      QUERY_ID,
      USER_NAME,
      WAREHOUSE_NAME,
      START_TIME,
      TOTAL_ELAPSED_TIME,
      EXECUTION_STATUS,
      QUERY_TEXT
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE {base_where}
      AND TOTAL_ELAPSED_TIME >= {LONG_RUNNING_MS}
    ORDER BY TOTAL_ELAPSED_TIME DESC
    """
    st.dataframe(run_query(long_sql, params), use_container_width=True)

    st.subheader("Currently Running (SHOW QUERIES)")
    if warehouses and "ALL" not in warehouses and len(warehouses) == 1:
        running_df = run_show_queries(warehouses[0])
        if "execution_status" in running_df.columns:
            running_df = running_df[running_df["execution_status"] == "RUNNING"]
        if running_df.empty:
            st.info("SHOW QUERIES unavailable or empty. Using INFORMATION_SCHEMA fallback.")
            running_df = run_running_queries_fallback(warehouses[0], fallback_window)
        st.dataframe(running_df, use_container_width=True)
    else:
        st.info("Select a single warehouse to view running queries.")

    st.subheader("Raw Query History")
    raw_sql = f"""
    SELECT
      QUERY_ID,
      USER_NAME,
      WAREHOUSE_NAME,
      DATABASE_NAME,
      SCHEMA_NAME,
      START_TIME,
      END_TIME,
      TOTAL_ELAPSED_TIME,
      BYTES_SCANNED,
      ROWS_PRODUCED,
      EXECUTION_STATUS,
      QUERY_TAG,
      QUERY_TEXT
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE {base_where}
    ORDER BY START_TIME DESC
    LIMIT 500
    """
    st.dataframe(run_query(raw_sql, params), use_container_width=True)

    st.caption("Tip: ACCOUNT_USAGE data can lag 45–90 minutes. Use SHOW QUERIES for near real-time running queries.")

with tab_cost:
    st.subheader("Warehouse Credits (Hourly)")
    cost_params = params.copy()
    cost_params.update(wh_params)
    wh_hour_sql = f"""
    SELECT
      DATE_TRUNC('hour', START_TIME) AS HOUR,
      WAREHOUSE_NAME,
      SUM(CREDITS_USED) AS CREDITS_USED
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= %(start_ts)s
      AND START_TIME <= %(end_ts)s
      {wh_filter_sql}
    GROUP BY 1, 2
    ORDER BY 1 DESC
    """
    wh_hour_df = run_query(wh_hour_sql, cost_params)
    if not wh_hour_df.empty:
        st.line_chart(
            wh_hour_df.pivot(index="HOUR", columns="WAREHOUSE_NAME", values="CREDITS_USED")
        )
    st.dataframe(wh_hour_df, use_container_width=True)

    st.subheader("Warehouse Credits (Daily)")
    wh_day_sql = f"""
    SELECT
      DATE_TRUNC('day', START_TIME) AS DAY,
      WAREHOUSE_NAME,
      SUM(CREDITS_USED) AS CREDITS_USED
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= %(start_ts)s
      AND START_TIME <= %(end_ts)s
      {wh_filter_sql}
    GROUP BY 1, 2
    ORDER BY 1 DESC
    """
    wh_day_df = run_query(wh_day_sql, cost_params)
    if not wh_day_df.empty:
        st.line_chart(
            wh_day_df.pivot(index="DAY", columns="WAREHOUSE_NAME", values="CREDITS_USED")
        )
    st.dataframe(wh_day_df, use_container_width=True)

    st.subheader("Estimated Query Cost (Credits)")
    st.caption("Estimated by allocating warehouse credits by total elapsed time per warehouse-hour.")
    cost_sql = f"""
    WITH q AS (
      SELECT
        QUERY_ID,
        USER_NAME,
        WAREHOUSE_NAME,
        START_TIME,
        DATE_TRUNC('hour', START_TIME) AS HOUR,
        TOTAL_ELAPSED_TIME,
        QUERY_TEXT
      FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
      WHERE {base_where}
        AND WAREHOUSE_NAME IS NOT NULL
    ),
    wh AS (
      SELECT
        DATE_TRUNC('hour', START_TIME) AS HOUR,
        WAREHOUSE_NAME,
        SUM(CREDITS_USED) AS CREDITS_USED
      FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
      WHERE START_TIME >= %(start_ts)s
        AND START_TIME <= %(end_ts)s
        {wh_filter_sql}
      GROUP BY 1, 2
    ),
    totals AS (
      SELECT HOUR, WAREHOUSE_NAME, SUM(TOTAL_ELAPSED_TIME) AS TOTAL_MS
      FROM q
      GROUP BY 1, 2
    )
    SELECT
      q.QUERY_ID,
      q.USER_NAME,
      q.WAREHOUSE_NAME,
      q.START_TIME,
      q.TOTAL_ELAPSED_TIME,
      q.QUERY_TEXT,
      wh.CREDITS_USED,
      (q.TOTAL_ELAPSED_TIME / NULLIF(t.TOTAL_MS, 0)) * wh.CREDITS_USED AS ESTIMATED_CREDITS
    FROM q
    LEFT JOIN totals t
      ON q.HOUR = t.HOUR AND q.WAREHOUSE_NAME = t.WAREHOUSE_NAME
    LEFT JOIN wh
      ON q.HOUR = wh.HOUR AND q.WAREHOUSE_NAME = wh.WAREHOUSE_NAME
    ORDER BY ESTIMATED_CREDITS DESC
    LIMIT 50
    """
    st.dataframe(run_query(cost_sql, cost_params), use_container_width=True)
