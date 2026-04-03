"""
CMHC Real Median and Average Household After-tax Income by Tenure transform.

Reads two Excel files from data/cmhc/, unpivots years from columns to rows,
combines the three tenure sheets (All Households, Renter, Owner), merges
median and average income into a single output, maps geography names to
dim_geography IDs, and writes a COPY-ready CSV to S3.

Source files:
  real-median-household-income-after-tax-tenure-2006-2023-en.xlsx
  average-household-after-tax-income-by-tenure-2006-2023-en.xlsx

Date convention: date_id = YYYY * 100 (e.g. 202300) — annual, same as fact_annual_income.

Usage:
    python -m src.transform.cmhc_income_tenure
"""

import io
import logging
import time
from datetime import date
from pathlib import Path

import boto3
import openpyxl
import pandas as pd
from dotenv import load_dotenv

from src.config import AWS_REGION, CMA_NAME_MAP, REDSHIFT_DATABASE, REDSHIFT_WORKGROUP, S3_BUCKET_RAW

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

s3 = boto3.client("s3", region_name=AWS_REGION)
redshift = boto3.client("redshift-data", region_name=AWS_REGION)

MEDIAN_EXCEL = Path("data/cmhc/real-median-household-income-after-tax-tenure-2006-2023-en.xlsx")
AVERAGE_EXCEL = Path("data/cmhc/average-household-after-tax-income-by-tenure-2006-2023-en.xlsx")

TENURE_SHEETS = {
    "All Households": "All",
    "Renter":         "Renter",
    "Owner":          "Owner",
}

# Header row index (0-based) in each data sheet — both files use row 8 (index 7)
DATA_HEADER_ROW = 7


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


def _parse_sheet(ws, tenure_label: str, value_col: str) -> pd.DataFrame:
    """
    Extract rows from one tenure sheet. Layout:
      Row 8 (index 7): header — geography label, year, data quality, year, ...
      Row 9+:          data rows — geography name, value, quality code, value, ...

    Years appear in every other column starting at column index 1.
    Data quality columns are discarded.
    """
    rows = list(ws.iter_rows(values_only=True))
    header = rows[DATA_HEADER_ROW]

    year_cols = []
    for i, cell in enumerate(header):
        if isinstance(cell, (int, str)):
            try:
                yr = int(str(cell).strip())
                if 2000 <= yr <= 2100:
                    year_cols.append((i, yr))
            except (ValueError, TypeError):
                pass

    records = []
    for row in rows[DATA_HEADER_ROW + 1:]:
        if not row or not row[0]:
            continue
        geo_raw = str(row[0]).strip()
        if geo_raw in ("End of worksheet", "Georgraphy", "Geography"):
            continue
        canonical = CMA_NAME_MAP.get(geo_raw)
        if not canonical:
            continue
        for col_idx, year in year_cols:
            val = row[col_idx] if len(row) > col_idx else None
            if val is None or val == "":
                continue
            try:
                income = float(val)
            except (TypeError, ValueError):
                continue
            records.append({
                "canonical": canonical,
                "year":      year,
                "tenure":    tenure_label,
                value_col:   income,
            })

    return pd.DataFrame(records)


def _read_excel(path: Path, value_col: str) -> pd.DataFrame:
    """Read all tenure sheets from one Excel file into a single DataFrame."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    frames = []
    for sheet_name, tenure_label in TENURE_SHEETS.items():
        ws = wb[sheet_name]
        df = _parse_sheet(ws, tenure_label, value_col)
        log.info("  %s / '%s': %d rows, %d CMAs",
                 path.name, sheet_name, len(df), df["canonical"].nunique() if len(df) else 0)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def transform() -> str:
    for path in (MEDIAN_EXCEL, AVERAGE_EXCEL):
        if not path.exists():
            raise FileNotFoundError(f"Excel file not found: {path}")

    log.info("Fetching geography IDs from Redshift …")
    geo_ids = fetch_geography_ids()
    log.info("  %d geographies loaded.", len(geo_ids))

    log.info("Reading median income file …")
    median_df = _read_excel(MEDIAN_EXCEL, "median_income")

    log.info("Reading average income file …")
    avg_df = _read_excel(AVERAGE_EXCEL, "avg_income")

    merged = median_df.merge(avg_df, on=["canonical", "year", "tenure"], how="outer")

    merged["geography_id"] = merged["canonical"].map(geo_ids)
    merged["date_id"] = merged["year"] * 100  # annual convention: YYYY00

    out = (
        merged[["geography_id", "date_id", "tenure", "median_income", "avg_income"]]
        .dropna(subset=["geography_id"])
        .astype({"geography_id": int, "date_id": int})
        .sort_values(["geography_id", "date_id", "tenure"])
    )

    log.info("Transformed %d rows (%d CMA × year × tenure combinations).",
             len(out), len(out))

    key = f"transformed/cmhc_income_tenure/{date.today().isoformat()}.csv"
    buf = io.BytesIO()
    out.to_csv(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET_RAW, Key=key, Body=buf.read(), ContentType="text/csv")
    log.info("Wrote → s3://%s/%s", S3_BUCKET_RAW, key)
    return key


if __name__ == "__main__":
    transform()
