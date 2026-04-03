"""
Redshift loader — reads normalized CSVs from S3 and loads into the star schema
using the Redshift Data API (no direct JDBC/psycopg2 connection needed).

Usage:
    python -m src.load.redshift_loader --source all
    python -m src.load.redshift_loader --source statcan
    python -m src.load.redshift_loader --source cmhc
"""

import argparse
import logging
import os
import time
from datetime import date

import boto3
from dotenv import load_dotenv

from src.config import AWS_REGION, REDSHIFT_DATABASE, REDSHIFT_WORKGROUP, S3_BUCKET_RAW

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

redshift = boto3.client("redshift-data", region_name=AWS_REGION)


# ---------------------------------------------------------------------------
# Redshift Data API helpers
# ---------------------------------------------------------------------------

def execute_sql(sql: str, wait: bool = True) -> str:
    resp = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=sql,
    )
    statement_id = resp["Id"]
    if wait:
        _wait_for_statement(statement_id)
    return statement_id


def _wait_for_statement(statement_id: str, poll_interval: int = 3) -> None:
    while True:
        desc = redshift.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status == "FINISHED":
            return
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift statement {statement_id} {status}: {desc.get('Error')}")
        time.sleep(poll_interval)


REDSHIFT_IAM_ROLE = os.getenv(
    "REDSHIFT_IAM_ROLE",
    f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/ForesiteRedshiftS3Role"
)


def copy_from_s3(table: str, s3_key: str, column_list: str) -> None:
    """Load a CSV from S3 into a Redshift table using COPY."""
    s3_path = f"s3://{S3_BUCKET_RAW}/{s3_key}"
    sql = f"""
        COPY {table} ({column_list})
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS CSV
        IGNOREHEADER 1
        EMPTYASNULL
        BLANKSASNULL
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto'
    """
    log.info("COPY %s ← %s", table, s3_path)
    execute_sql(sql)
    log.info("COPY complete: %s", table)


# ---------------------------------------------------------------------------
# Staging helpers — upsert via temp table to avoid duplicates
# ---------------------------------------------------------------------------

def upsert_fact_monthly(s3_key: str) -> None:
    """
    Load a normalized StatCan CSV into fact_monthly via a permanent staging table.
    Permanent staging tables are required because the Redshift Data API opens a
    new session per execute_statement call, so TEMP tables do not persist between calls.
    """
    execute_sql("TRUNCATE stg_fact_monthly")
    copy_from_s3("stg_fact_monthly", s3_key, "geography_id, date_id, indicator_id, value")
    execute_sql("""
        DELETE FROM fact_monthly
        USING stg_fact_monthly
        WHERE fact_monthly.geography_id = stg_fact_monthly.geography_id
          AND fact_monthly.date_id      = stg_fact_monthly.date_id
          AND fact_monthly.indicator_id = stg_fact_monthly.indicator_id
    """)
    execute_sql("""
        INSERT INTO fact_monthly (geography_id, date_id, indicator_id, value)
        SELECT geography_id, date_id, indicator_id, value FROM stg_fact_monthly
    """)
    log.info("Upserted fact_monthly from %s", s3_key)


def upsert_annual_income(s3_key: str) -> None:
    execute_sql("TRUNCATE stg_fact_annual_income")
    copy_from_s3(
        "stg_fact_annual_income", s3_key,
        "geography_id, date_id, income_source, age_group, sex, median_income, avg_income, num_persons"
    )
    execute_sql("""
        DELETE FROM fact_annual_income
        USING stg_fact_annual_income
        WHERE fact_annual_income.geography_id = stg_fact_annual_income.geography_id
          AND fact_annual_income.date_id       = stg_fact_annual_income.date_id
          AND fact_annual_income.income_source = stg_fact_annual_income.income_source
          AND fact_annual_income.age_group     = stg_fact_annual_income.age_group
          AND fact_annual_income.sex           = stg_fact_annual_income.sex
    """)
    execute_sql("""
        INSERT INTO fact_annual_income
            (geography_id, date_id, income_source, age_group, sex, median_income, avg_income, num_persons)
        SELECT geography_id, date_id, income_source, age_group, sex, median_income, avg_income, num_persons
        FROM stg_fact_annual_income
    """)
    log.info("Upserted fact_annual_income from %s", s3_key)


# ---------------------------------------------------------------------------
# Source-specific loaders
# ---------------------------------------------------------------------------

def load_statcan(pid: str) -> None:
    """Load the most recent transformed StatCan CSV for a given PID."""
    prefix = f"transformed/statcan/{pid}/"
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    objects = s3_client.list_objects_v2(Bucket=S3_BUCKET_RAW, Prefix=prefix).get("Contents", [])
    if not objects:
        log.warning("No transformed file for pid=%s — run src.transform.statcan first.", pid)
        return
    latest = sorted(objects, key=lambda o: o["LastModified"], reverse=True)[0]["Key"]
    log.info("Loading StatCan pid=%s from %s", pid, latest)
    if pid == "1110023901":
        upsert_annual_income(latest)
    else:
        upsert_fact_monthly(latest)


def load_cmhc(year: int | None = None) -> None:
    """Load transformed CMHC CSV(s) from S3."""
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    prefix = f"transformed/cmhc/{year}.csv" if year else "transformed/cmhc/"
    objects = s3_client.list_objects_v2(Bucket=S3_BUCKET_RAW, Prefix=prefix).get("Contents", [])
    if not objects:
        log.warning("No transformed CMHC files found — run src.transform.cmhc first.")
        return
    for obj in objects:
        upsert_fact_monthly(obj["Key"])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(source: str) -> None:
    from src.config import STATCAN_TABLES

    if source in ("all", "statcan"):
        for name, cfg in STATCAN_TABLES.items():
            try:
                load_statcan(cfg["pid"])
            except Exception as exc:
                log.error("Failed to load %s: %s", name, exc)

    if source in ("all", "cmhc"):
        try:
            load_cmhc()
        except Exception as exc:
            log.error("Failed to load CMHC: %s", exc)

    log.info("Redshift load complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load normalized CSVs from S3 into Redshift")
    parser.add_argument("--source", choices=["all", "statcan", "cmhc"], default="all")
    args = parser.parse_args()
    main(args.source)
