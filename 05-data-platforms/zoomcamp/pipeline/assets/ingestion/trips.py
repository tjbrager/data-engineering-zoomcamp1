"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python 3.12

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
# columns:
#   - name: TODO_col1
#     type: TODO_type
#     description: TODO

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    import os
    import json
    import tempfile
    from datetime import date
    from dateutil import parser
    from dateutil.relativedelta import relativedelta
    import time
    import requests
    import pandas as pd

    # Helper: month iterator (inclusive of start, exclusive of end)
    def months_in_range(start_date: date, end_date: date):
        cur = date(start_date.year, start_date.month, 1)
        while cur < end_date:
            yield cur.year, cur.month
            cur = cur + relativedelta(months=1)

    BRUIN_START = os.getenv("BRUIN_START_DATE")
    BRUIN_END = os.getenv("BRUIN_END_DATE")
    if not BRUIN_START or not BRUIN_END:
        raise RuntimeError(
            "BRUIN_START_DATE and BRUIN_END_DATE must be provided (YYYY-MM-DD)"
        )

    start = parser.isoparse(BRUIN_START).date()
    end = parser.isoparse(BRUIN_END).date()

    # Pipeline variables may be passed in BRUIN_VARS as JSON (or you can set BRUIN_TAXI_TYPES)
    taxi_types = None
    bruin_vars = os.getenv("BRUIN_VARS")
    if bruin_vars:
        try:
            vars_json = json.loads(bruin_vars)
            taxi_types = vars_json.get("taxi_types")
        except Exception:
            taxi_types = None

    if not taxi_types:
        env_tt = os.getenv("BRUIN_TAXI_TYPES")
        if env_tt:
            try:
                taxi_types = json.loads(env_tt)
            except Exception:
                # allow comma separated list
                taxi_types = [t.strip() for t in env_tt.split(",") if t.strip()]

    # sensible default
    if not taxi_types:
        taxi_types = ["yellow"]

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    rows = []
    dfs = []
    now_ts = pd.Timestamp.utcnow()

    for taxi in taxi_types:
        for yy, mm in months_in_range(start, end):
            filename = f"{taxi}_tripdata_{yy}-{mm:02d}.parquet"
            url = base_url + filename
            print(f"Fetching {url}")
            try:
                resp = requests.get(url, stream=True, timeout=60)
                if resp.status_code != 200:
                    print(
                        f"Warning: {url} returned status {resp.status_code}, skipping"
                    )
                    continue
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
                    for chunk in resp.iter_content(chunk_size=1_048_576):
                        if chunk:
                            tmp.write(chunk)
                    tmp.flush()
                    try:
                        df = pd.read_parquet(tmp.name, engine="pyarrow")
                    except Exception as e:
                        print(f"Failed to read parquet {filename}: {e}")
                        continue

                if df.empty:
                    print(f"No rows in {filename}")
                    continue

                # annotate with extraction metadata
                df = df.reset_index(drop=True)
                df["bruin_extracted_at"] = now_ts
                df["bruin_source_file"] = filename
                df["taxi_type"] = taxi
                dfs.append(df)
                # small pause to be nice to remote host
                time.sleep(0.2)
            except Exception as e:
                print(f"Error fetching {url}: {e}")
                continue

    if not dfs:
        print("No data downloaded for the requested window/taxi types")
        return pd.DataFrame()

    result = pd.concat(dfs, ignore_index=True)
    return result
