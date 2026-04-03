"""
CMHC Rental Market Survey ingestion — parses annual Excel files and uploads
normalized CSVs to S3.

Place CMHC Excel files in data/cmhc/ (e.g. data/cmhc/2024.xlsx) before running.

Usage:
    python -m src.ingest.cmhc                       # process all files in data/cmhc/
    python -m src.ingest.cmhc --file data/cmhc/2024.xlsx  # process one file
"""

import argparse
import io
import logging
import re
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

from src.config import (
    AWS_REGION,
    CMA_NAME_MAP,
    CMHC_BEDROOM_TYPES,
    CMHC_RENT_SHEET,
    CMHC_VACANCY_SHEET,
    S3_BUCKET_RAW,
    TARGET_CMAS,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

s3 = boto3.client("s3", region_name=AWS_REGION)

CMHC_DROP_FOLDER = Path("data/cmhc")


def extract_year(path: Path) -> int:
    match = re.search(r"(20\d{2})", path.stem)
    if not match:
        raise ValueError(f"Cannot determine year from filename: {path.name}. Name files as YYYY.xlsx")
    return int(match.group(1))


def log_sheets(path: Path) -> None:
    """Log all sheet names — useful on first run to verify sheet naming."""
    xl = pd.ExcelFile(path)
    log.info("Sheets in %s: %s", path.name, xl.sheet_names)


def normalize_cma(name: str) -> str | None:
    """Return canonical CMA name or None if not a target CMA."""
    name = str(name).strip()
    return CMA_NAME_MAP.get(name)


def parse_sheet(path: Path, sheet: str, value_col_name: str, year: int) -> pd.DataFrame:
    """
    Parse a wide-format CMHC sheet into a long-format DataFrame.

    CMHC tables have geography in rows and bedroom types in columns.
    Row structure varies by edition — we locate the header row by scanning
    for the first row that contains bedroom type keywords.
    """
    raw = pd.read_excel(path, sheet_name=sheet, header=None)
    log.info("Parsing sheet '%s' from %s (%d rows raw)", sheet, path.name, len(raw))

    # Find the header row — first row containing any bedroom type keyword
    header_row_idx = None
    for i, row in raw.iterrows():
        row_str = " ".join(str(v) for v in row.values).lower()
        if any(bt.lower() in row_str for bt in ["bachelor", "bedroom", "total"]):
            header_row_idx = i
            break

    if header_row_idx is None:
        raise ValueError(f"Could not locate header row in sheet '{sheet}' of {path.name}")

    df = pd.read_excel(path, sheet_name=sheet, header=header_row_idx)
    df.columns = [str(c).strip() for c in df.columns]

    # First column is geography
    geo_col = df.columns[0]
    df = df.rename(columns={geo_col: "geography_raw"})

    # Keep only bedroom-type columns that exist in this edition
    value_cols = [c for c in df.columns if any(bt.lower() in c.lower() for bt in CMHC_BEDROOM_TYPES)]
    if not value_cols:
        raise ValueError(f"No bedroom type columns found in sheet '{sheet}'. Columns: {df.columns.tolist()}")

    df = df[["geography_raw"] + value_cols].copy()
    df["cma"] = df["geography_raw"].apply(normalize_cma)
    df = df[df["cma"].notna()].copy()

    # Melt wide → long
    df_long = df.melt(id_vars=["cma"], value_vars=value_cols, var_name="bedroom_type", value_name=value_col_name)
    df_long["year"] = year
    df_long["source"] = "CMHC"
    df_long["bedroom_type"] = df_long["bedroom_type"].str.strip()

    # Coerce values to numeric — CMHC uses "**" and "--" for suppressed/unavailable
    df_long[value_col_name] = pd.to_numeric(df_long[value_col_name], errors="coerce")

    return df_long[["year", "cma", "bedroom_type", value_col_name, "source"]]


def process_file(path: Path) -> None:
    year = extract_year(path)
    log.info("Processing %s (year=%d)", path.name, year)
    log_sheets(path)

    vacancy_df = parse_sheet(path, CMHC_VACANCY_SHEET, "vacancy_rate_pct", year)
    rent_df = parse_sheet(path, CMHC_RENT_SHEET, "avg_rent_cad", year)

    # Merge vacancy and rent on common keys
    merged = vacancy_df.merge(rent_df, on=["year", "cma", "bedroom_type", "source"], how="outer")

    log.info(
        "  %d vacancy rows, %d rent rows → %d merged rows for %d CMAs",
        len(vacancy_df),
        len(rent_df),
        len(merged),
        merged["cma"].nunique(),
    )

    csv_bytes = merged.to_csv(index=False).encode()
    key = f"cmhc/{year}.csv"
    s3.put_object(Bucket=S3_BUCKET_RAW, Key=key, Body=csv_bytes, ContentType="text/csv")
    log.info("Uploaded → s3://%s/%s", S3_BUCKET_RAW, key)


def main(file_path: str | None = None) -> None:
    if file_path:
        paths = [Path(file_path)]
    else:
        paths = sorted(CMHC_DROP_FOLDER.glob("*.xlsx")) + sorted(CMHC_DROP_FOLDER.glob("*.xls"))

    if not paths:
        log.warning("No Excel files found in %s — place CMHC files there and re-run.", CMHC_DROP_FOLDER)
        return

    errors: list[str] = []
    for path in paths:
        try:
            process_file(path)
        except Exception as exc:
            log.error("Failed to process %s: %s", path.name, exc)
            errors.append(path.name)

    if errors:
        raise SystemExit(f"CMHC ingestion failed for: {', '.join(errors)}")
    log.info("CMHC ingestion complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse CMHC Excel files and upload to S3")
    parser.add_argument("--file", help="Path to a single CMHC Excel file")
    args = parser.parse_args()
    main(args.file)
