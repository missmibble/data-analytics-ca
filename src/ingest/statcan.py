"""
Statistics Canada WDS ingestion — fetches full-table CSV ZIPs for all configured tables
and uploads raw CSVs to S3.

Usage:
    python -m src.ingest.statcan                  # fetch all tables
    python -m src.ingest.statcan --table cpi_cma  # fetch one table
"""

import argparse
import io
import logging
import zipfile
from datetime import date
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv

from src.config import AWS_REGION, S3_BUCKET_RAW, STATCAN_TABLES, WDS_BASE_URL

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

s3 = boto3.client("s3", region_name=AWS_REGION)


def s3_key(pid: str) -> str:
    return f"statcan/{pid}/{date.today().isoformat()}.csv"


def already_fetched(pid: str) -> bool:
    key = s3_key(pid)
    try:
        s3.head_object(Bucket=S3_BUCKET_RAW, Key=key)
        log.info("Already fetched today — skipping pid=%s key=%s", pid, key)
        return True
    except s3.exceptions.ClientError:
        return False


def get_download_url(pid: str) -> str:
    url = f"{WDS_BASE_URL}/getFullTableDownloadCSV/{pid}/en"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    # WDS returns {"status": "SUCCESS", "object": "<zip-url>"}
    if payload.get("status") != "SUCCESS":
        raise RuntimeError(f"WDS returned non-success for pid={pid}: {payload}")
    return payload["object"]


def fetch_and_upload(name: str, config: dict) -> None:
    pid = config["pid"]
    log.info("Fetching %s (pid=%s) …", name, pid)

    if already_fetched(pid):
        return

    zip_url = get_download_url(pid)
    log.info("Downloading ZIP from %s", zip_url)
    resp = requests.get(zip_url, timeout=120, stream=True)
    resp.raise_for_status()

    zip_bytes = io.BytesIO(resp.content)
    with zipfile.ZipFile(zip_bytes) as zf:
        # The main data file is always named <PID>.csv inside the ZIP
        csv_name = next(
            (n for n in zf.namelist() if n.endswith(".csv") and not n.endswith("_MetaData.csv")),
            None,
        )
        if csv_name is None:
            raise RuntimeError(f"No data CSV found in ZIP for pid={pid}. Contents: {zf.namelist()}")

        csv_bytes = zf.read(csv_name)

    key = s3_key(pid)
    s3.put_object(Bucket=S3_BUCKET_RAW, Key=key, Body=csv_bytes, ContentType="text/csv")
    log.info("Uploaded %s → s3://%s/%s (%d bytes)", name, S3_BUCKET_RAW, key, len(csv_bytes))


def main(table_filter: str | None = None) -> None:
    tables = (
        {table_filter: STATCAN_TABLES[table_filter]}
        if table_filter
        else STATCAN_TABLES
    )
    errors: list[str] = []
    for name, config in tables.items():
        try:
            fetch_and_upload(name, config)
        except Exception as exc:
            log.error("Failed to fetch %s: %s", name, exc)
            errors.append(name)

    if errors:
        raise SystemExit(f"Ingestion failed for: {', '.join(errors)}")
    log.info("StatCan ingestion complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Statistics Canada tables via WDS API")
    parser.add_argument("--table", choices=list(STATCAN_TABLES.keys()), help="Fetch a single table")
    args = parser.parse_args()
    main(args.table)
