"""
Mortgage rates transform — reads the normalized raw CSV from S3, looks up
dim_geography and dim_indicator IDs, and writes a COPY-ready CSV back to S3.

Output columns: geography_id, date_id, indicator_id, value
All rows use geography_id for 'Canada' (national-level data, no CMA breakdown).

Usage:
    python -m src.transform.mortgage_rates
"""

import io
import logging
import time
from datetime import date

import boto3
import pandas as pd
from dotenv import load_dotenv

from src.config import AWS_REGION, REDSHIFT_DATABASE, REDSHIFT_WORKGROUP, S3_BUCKET_RAW

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

s3 = boto3.client("s3", region_name=AWS_REGION)
redshift = boto3.client("redshift-data", region_name=AWS_REGION)

RAW_KEY = "cmhc/raw/mortgage_rates.csv"

TERM_TO_INDICATOR = {
    "1 year": "Mortgage rate - 1 year",
    "3 year": "Mortgage rate - 3 year",
    "5 year": "Mortgage rate - 5 year",
}


def _run_query(sql: str) -> list[dict]:
    resp = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=sql,
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


def fetch_canada_geography_id() -> int:
    rows = _run_query("SELECT geography_id FROM dim_geography WHERE cma_name = 'Canada'")
    if not rows:
        raise RuntimeError(
            "'Canada' not found in dim_geography. "
            "Run infra/setup.py or apply the schema migration to add it."
        )
    return int(rows[0]["geography_id"])


def fetch_indicator_ids() -> dict[str, int]:
    names = ", ".join(f"'{n}'" for n in TERM_TO_INDICATOR.values())
    rows = _run_query(
        f"SELECT indicator_id, indicator_name FROM dim_indicator "
        f"WHERE indicator_name IN ({names}) AND source = 'CMHC'"
    )
    return {r["indicator_name"]: int(r["indicator_id"]) for r in rows}


def transform() -> None:
    log.info("Reading raw mortgage rates from S3 …")
    obj = s3.get_object(Bucket=S3_BUCKET_RAW, Key=RAW_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    geography_id = fetch_canada_geography_id()
    indicator_ids = fetch_indicator_ids()

    missing = set(TERM_TO_INDICATOR.values()) - set(indicator_ids)
    if missing:
        raise RuntimeError(f"Missing dim_indicator entries: {missing}. Run schema migration.")

    df["geography_id"] = geography_id
    df["date_id"] = df["year"] * 100 + df["month"]
    df["indicator_id"] = df["term"].map(TERM_TO_INDICATOR).map(indicator_ids)
    df = df.rename(columns={"rate": "value"})
    df = df[["geography_id", "date_id", "indicator_id", "value"]].dropna()

    key = f"transformed/mortgage_rates/{date.today().isoformat()}.csv"
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    s3.put_object(Bucket=S3_BUCKET_RAW, Key=key, Body=buf.getvalue(), ContentType="text/csv")
    log.info("Uploaded %d rows → s3://%s/%s", len(df), S3_BUCKET_RAW, key)


if __name__ == "__main__":
    transform()
