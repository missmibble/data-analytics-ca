"""
CMHC ETL transform — reads raw CMHC CSVs from S3, maps to dim table IDs,
and writes COPY-ready CSVs back to S3 under the transformed/ prefix.

CMHC surveys are conducted in October — date_id = YYYY * 100 + 10.

Usage:
    python -m src.transform.cmhc
    python -m src.transform.cmhc --year 2023
"""

import argparse
import io
import logging
import re
import time

import boto3
import pandas as pd
from dotenv import load_dotenv

from src.config import AWS_REGION, REDSHIFT_DATABASE, REDSHIFT_WORKGROUP, S3_BUCKET_RAW

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

s3 = boto3.client("s3", region_name=AWS_REGION)
redshift = boto3.client("redshift-data", region_name=AWS_REGION)

# (data_type, bedroom_type) → indicator_name matching dim_indicator
INDICATOR_MAP: dict[tuple[str, str], str] = {
    ("vacancy", "Bachelor"):    "Vacancy rate - Bachelor",
    ("vacancy", "1 Bedroom"):   "Vacancy rate - 1 Bedroom",
    ("vacancy", "2 Bedroom"):   "Vacancy rate - 2 Bedroom",
    ("vacancy", "3 Bedroom +"): "Vacancy rate - 3 Bedroom +",
    ("vacancy", "Total"):       "Vacancy rate - Total",
    ("rent",    "Bachelor"):    "Avg rent - Bachelor",
    ("rent",    "1 Bedroom"):   "Avg rent - 1 Bedroom",
    ("rent",    "2 Bedroom"):   "Avg rent - 2 Bedroom",
    ("rent",    "3 Bedroom +"): "Avg rent - 3 Bedroom +",
    ("rent",    "Total"):       "Avg rent - Total",
}


# ---------------------------------------------------------------------------
# Redshift helpers
# ---------------------------------------------------------------------------

def _run_query(sql: str) -> list[dict]:
    resp = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP, Database=REDSHIFT_DATABASE, Sql=sql
    )
    sid = resp["Id"]
    while True:
        desc = redshift.describe_statement(Id=sid)
        if desc["Status"] == "FINISHED":
            break
        if desc["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Query failed: {desc.get('Error')}")
        time.sleep(2)
    result = redshift.get_statement_result(Id=sid)
    cols = [c["name"] for c in result["ColumnMetadata"]]
    return [dict(zip(cols, [list(v.values())[0] for v in row])) for row in result["Records"]]


def fetch_geography_ids() -> dict[str, int]:
    rows = _run_query("SELECT geography_id, cma_name FROM dim_geography")
    return {r["cma_name"]: r["geography_id"] for r in rows}


def fetch_indicator_ids() -> dict[str, int]:
    rows = _run_query("SELECT indicator_id, indicator_name FROM dim_indicator WHERE source = 'CMHC'")
    return {r["indicator_name"]: r["indicator_id"] for r in rows}


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _read_raw_csv(key: str) -> pd.DataFrame | None:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_RAW, Key=key)
        return pd.read_csv(io.BytesIO(obj["Body"].read()))
    except s3.exceptions.NoSuchKey:
        return None


def _list_raw_years() -> list[int]:
    objects = s3.list_objects_v2(Bucket=S3_BUCKET_RAW, Prefix="cmhc/raw/").get("Contents", [])
    years = set()
    for o in objects:
        m = re.search(r"(\d{4})", o["Key"])
        if m:
            years.add(int(m.group(1)))
    return sorted(years)


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def transform_year(year: int, geo_ids: dict[str, int], ind_ids: dict[str, int]) -> None:
    out_rows = []

    for data_type in ("vacancy", "rent"):
        df = _read_raw_csv(f"cmhc/raw/{year}_{data_type}.csv")
        if df is None:
            log.warning("No raw file for %d %s — skipping", year, data_type)
            continue
        log.info("Processing %d %s (%d rows)", year, data_type, len(df))

        for _, row in df.iterrows():
            geo_id = geo_ids.get(row["centre"])
            if not geo_id:
                continue
            indicator_name = INDICATOR_MAP.get((data_type, row["bedroom_type"]))
            if not indicator_name:
                continue
            ind_id = ind_ids.get(indicator_name)
            if not ind_id:
                log.warning("Indicator not found in dim_indicator: %s", indicator_name)
                continue
            date_id = int(row["year"]) * 100 + 10  # October survey
            out_rows.append({
                "geography_id": geo_id,
                "date_id": date_id,
                "indicator_id": ind_id,
                "value": row["value"],
            })

    if not out_rows:
        log.warning("No transformed rows for year %d", year)
        return

    out = pd.DataFrame(out_rows).astype({"geography_id": int, "date_id": int, "indicator_id": int})
    key = f"transformed/cmhc/{year}.csv"
    buf = io.BytesIO()
    out.to_csv(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET_RAW, Key=key, Body=buf.read(), ContentType="text/csv")
    log.info("Wrote → s3://%s/%s (%d rows)", S3_BUCKET_RAW, key, len(out))


def main(year_filter: int | None = None) -> None:
    log.info("Fetching dim table IDs from Redshift …")
    geo_ids = fetch_geography_ids()
    ind_ids = fetch_indicator_ids()
    log.info("  %d geographies, %d CMHC indicators loaded.", len(geo_ids), len(ind_ids))

    years = [year_filter] if year_filter else _list_raw_years()
    if not years:
        log.warning("No raw CMHC files found in s3://%s/cmhc/raw/", S3_BUCKET_RAW)
        return

    errors = []
    for year in years:
        try:
            transform_year(year, geo_ids, ind_ids)
        except Exception as exc:
            log.error("Transform failed for year %d: %s", year, exc)
            errors.append(str(year))

    if errors:
        raise SystemExit(f"CMHC transform failed for years: {', '.join(errors)}")
    log.info("CMHC transform complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform raw CMHC CSVs to Redshift-ready format")
    parser.add_argument("--year", type=int, help="Process a single year")
    args = parser.parse_args()
    main(args.year)
