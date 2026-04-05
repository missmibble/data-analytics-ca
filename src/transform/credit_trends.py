"""
Credit trends transform — reads the normalized raw CSV from S3, looks up
dim_geography and dim_indicator IDs, and writes a COPY-ready CSV to S3.

Quarter-to-month mapping: Q1→3, Q2→6, Q3→9, Q4→12 (end-of-quarter month).
date_id = year * 100 + month, matching existing dim_date entries.

Usage:
    python -m src.transform.credit_trends
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

RAW_KEY = "cmhc/raw/credit_trends.csv"
QUARTER_TO_MONTH = {1: 3, 2: 6, 3: 9, 4: 12}

INDICATORS = [
    "Mortgage delinquency rate",
    "HELOC delinquency rate",
    "Credit card delinquency rate",
    "Auto loan delinquency rate",
    "LOC delinquency rate",
    "Avg credit score - Without mortgage",
    "Avg credit score - With mortgage",
    "Avg credit score - With new mortgage",
    "Avg monthly mortgage payment - Existing",
    "Avg monthly mortgage payment - New",
]


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


def fetch_geography_ids() -> dict[str, int]:
    rows = _run_query("SELECT geography_id, cma_name FROM dim_geography")
    return {r["cma_name"]: int(r["geography_id"]) for r in rows}


def fetch_indicator_ids() -> dict[str, int]:
    names = ", ".join(f"'{n}'" for n in INDICATORS)
    rows = _run_query(
        f"SELECT indicator_id, indicator_name FROM dim_indicator "
        f"WHERE indicator_name IN ({names}) AND source = 'CMHC'"
    )
    return {r["indicator_name"]: int(r["indicator_id"]) for r in rows}


def transform() -> None:
    log.info("Reading raw credit trends from S3 …")
    obj = s3.get_object(Bucket=S3_BUCKET_RAW, Key=RAW_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    geo_ids = fetch_geography_ids()
    ind_ids = fetch_indicator_ids()

    missing_geos = set(df["geography"].unique()) - set(geo_ids)
    if missing_geos:
        raise RuntimeError(f"Geographies not in dim_geography: {missing_geos}")

    missing_inds = set(df["indicator_name"].unique()) - set(ind_ids)
    if missing_inds:
        raise RuntimeError(f"Indicators not in dim_indicator: {missing_inds}. Run schema migration.")

    df["geography_id"] = df["geography"].map(geo_ids)
    df["month"] = df["quarter"].map(QUARTER_TO_MONTH)
    df["date_id"] = df["year"] * 100 + df["month"]
    df["indicator_id"] = df["indicator_name"].map(ind_ids)
    df = df.rename(columns={"value": "value"})
    df = df[["geography_id", "date_id", "indicator_id", "value"]].dropna()

    key = f"transformed/credit_trends/{date.today().isoformat()}.csv"
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    s3.put_object(Bucket=S3_BUCKET_RAW, Key=key, Body=buf.getvalue(), ContentType="text/csv")
    log.info("Uploaded %d rows → s3://%s/%s", len(df), S3_BUCKET_RAW, key)


if __name__ == "__main__":
    transform()
