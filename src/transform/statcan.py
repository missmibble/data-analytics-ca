"""
StatCan ETL transform — reads raw StatCan CSVs from S3, maps to dim table IDs,
and writes COPY-ready CSVs back to S3 under the transformed/ prefix.

Usage:
    python -m src.transform.statcan --table cpi_cma
    python -m src.transform.statcan --table all
"""

import argparse
import io
import logging
import time
from datetime import date

import boto3
import pandas as pd
from dotenv import load_dotenv

from src.config import (
    AWS_REGION, CMA_NAME_MAP, REDSHIFT_DATABASE,
    REDSHIFT_WORKGROUP, S3_BUCKET_RAW, STATCAN_TABLES,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

s3 = boto3.client("s3", region_name=AWS_REGION)
redshift = boto3.client("redshift-data", region_name=AWS_REGION)

# ---------------------------------------------------------------------------
# CPI product group → dim_indicator name mapping
# ---------------------------------------------------------------------------
CPI_PRODUCT_MAP = {
    "All-items":      "CPI - All-items",
    "Food":           "CPI - Food",
    "Shelter":        "CPI - Shelter",
    "Transportation": "CPI - Transportation",
    "Energy":         "CPI - Energy",
}

GAS_FUEL_TYPE = "Regular unleaded gasoline at self service filling stations"

FOOD_PRODUCT_MAP = {
    "White bread, 675 grams":  "Food price - Bread (per 675g)",
    "Milk, 2 litres":          "Food price - Milk (per 2L)",
    "Eggs, 1 dozen":           "Food price - Eggs (per dozen)",
}


# ---------------------------------------------------------------------------
# Redshift dim table lookups
# ---------------------------------------------------------------------------

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
    """Return {canonical_cma_name: geography_id}"""
    rows = _run_query("SELECT geography_id, cma_name FROM dim_geography")
    return {r["cma_name"]: r["geography_id"] for r in rows}


def fetch_indicator_ids() -> dict[tuple, int]:
    """Return {(indicator_name, source): indicator_id}"""
    rows = _run_query("SELECT indicator_id, indicator_name, source FROM dim_indicator")
    return {(r["indicator_name"], r["source"]): r["indicator_id"] for r in rows}


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _latest_raw_key(pid: str) -> str | None:
    prefix = f"statcan/{pid}/"
    objects = s3.list_objects_v2(Bucket=S3_BUCKET_RAW, Prefix=prefix).get("Contents", [])
    if not objects:
        return None
    return sorted(objects, key=lambda o: o["LastModified"], reverse=True)[0]["Key"]


def _read_raw_csv(key: str) -> pd.DataFrame:
    log.info("Reading raw CSV: s3://%s/%s", S3_BUCKET_RAW, key)
    obj = s3.get_object(Bucket=S3_BUCKET_RAW, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()), encoding="utf-8-sig", low_memory=False)


def _write_transformed(df: pd.DataFrame, pid: str) -> str:
    key = f"transformed/statcan/{pid}/{date.today().isoformat()}.csv"
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET_RAW, Key=key, Body=buf.read(), ContentType="text/csv")
    log.info("Wrote transformed CSV → s3://%s/%s (%d rows)", S3_BUCKET_RAW, key, len(df))
    return key


# ---------------------------------------------------------------------------
# Table-specific transforms
# ---------------------------------------------------------------------------

def transform_cpi(geo_ids: dict, ind_ids: dict) -> str:
    pid = STATCAN_TABLES["cpi_cma"]["pid"]
    key = _latest_raw_key(pid)
    if not key:
        raise FileNotFoundError(f"No raw file found for pid={pid}. Run ingest first.")

    df = _read_raw_csv(key)

    # Filter: 2002=100 base year, target CMAs, target product groups
    df = df[df["UOM"] == "2002=100"].copy()
    df["cma_canonical"] = df["GEO"].str.strip().map(CMA_NAME_MAP)
    df = df[df["cma_canonical"].notna()]
    df["indicator_name"] = df["Products and product groups"].str.strip().map(CPI_PRODUCT_MAP)
    df = df[df["indicator_name"].notna()]
    df = df[df["VALUE"].notna() & (df["VALUE"] != "")]

    df["geography_id"] = df["cma_canonical"].map(geo_ids)
    df["date_id"] = df["REF_DATE"].str.replace("-", "").astype(int)
    df["indicator_id"] = df["indicator_name"].map(lambda n: ind_ids.get((n, "StatCan")))
    df["value"] = pd.to_numeric(df["VALUE"], errors="coerce")

    out = df[["geography_id", "date_id", "indicator_id", "value"]].dropna()
    out = out.astype({"geography_id": int, "date_id": int, "indicator_id": int})
    return _write_transformed(out, pid)


def transform_gasoline(geo_ids: dict, ind_ids: dict) -> str:
    pid = STATCAN_TABLES["gasoline_prices"]["pid"]
    key = _latest_raw_key(pid)
    if not key:
        raise FileNotFoundError(f"No raw file found for pid={pid}.")

    df = _read_raw_csv(key)
    df = df[df["Type of fuel"] == GAS_FUEL_TYPE].copy()
    df["cma_canonical"] = df["GEO"].str.strip().map(CMA_NAME_MAP)
    df = df[df["cma_canonical"].notna()]
    df = df[df["VALUE"].notna() & (df["VALUE"] != "")]

    ind_id = ind_ids.get(("Gasoline price (per litre)", "StatCan"))
    df["geography_id"] = df["cma_canonical"].map(geo_ids)
    df["date_id"] = df["REF_DATE"].str.replace("-", "").astype(int)
    df["indicator_id"] = ind_id
    df["value"] = pd.to_numeric(df["VALUE"], errors="coerce")

    out = df[["geography_id", "date_id", "indicator_id", "value"]].dropna()
    out = out.astype({"geography_id": int, "date_id": int, "indicator_id": int})
    return _write_transformed(out, pid)


PROVINCE_TO_CMAS: dict[str, list[str]] = {
    "Alberta":                      ["Calgary", "Edmonton"],
    "British Columbia":             ["Vancouver", "Victoria"],
    "Manitoba":                     ["Winnipeg"],
    "New Brunswick":                ["Saint John"],
    "Newfoundland and Labrador":    ["St. John's"],
    "Nova Scotia":                  ["Halifax"],
    "Ontario":                      ["Toronto", "Ottawa-Gatineau", "Thunder Bay"],
    "Prince Edward Island":         ["Charlottetown"],
    "Quebec":                       ["Montreal", "Quebec City"],
    "Saskatchewan":                 ["Regina", "Saskatoon"],
}


def transform_food(geo_ids: dict, ind_ids: dict) -> str:
    """
    Food prices are only available at the provincial level — no CMA breakdown exists.
    Provincial values are replicated to all target CMAs within that province.
    """
    pid = STATCAN_TABLES["food_prices"]["pid"]
    key = _latest_raw_key(pid)
    if not key:
        raise FileNotFoundError(f"No raw file found for pid={pid}.")

    df = _read_raw_csv(key)
    df["indicator_name"] = df["Products"].str.strip().map(FOOD_PRODUCT_MAP)
    df = df[df["indicator_name"].notna()]
    df = df[df["GEO"].str.strip().isin(PROVINCE_TO_CMAS.keys())]
    df = df[df["VALUE"].notna() & (df["VALUE"] != "")]

    df["date_id"] = df["REF_DATE"].str.replace("-", "").astype(int)
    df["indicator_id"] = df["indicator_name"].map(lambda n: ind_ids.get((n, "StatCan")))
    df["value"] = pd.to_numeric(df["VALUE"], errors="coerce")

    # Expand each provincial row into one row per CMA in that province
    rows = []
    for _, row in df.iterrows():
        province = row["GEO"].strip()
        for cma in PROVINCE_TO_CMAS.get(province, []):
            geo_id = geo_ids.get(cma)
            if geo_id:
                rows.append({
                    "geography_id": geo_id,
                    "date_id": row["date_id"],
                    "indicator_id": row["indicator_id"],
                    "value": row["value"],
                })

    out = pd.DataFrame(rows).dropna()
    out = out.astype({"geography_id": int, "date_id": int, "indicator_id": int})
    return _write_transformed(out, pid)


def transform_income(geo_ids: dict) -> str:
    """Income goes to fact_annual_income — different schema."""
    pid = STATCAN_TABLES["median_income"]["pid"]
    key = _latest_raw_key(pid)
    if not key:
        raise FileNotFoundError(f"No raw file found for pid={pid}.")

    df = _read_raw_csv(key)

    # Filter to median and average income statistics only
    df = df[df["Statistics"].isin(["Median income (excluding zeros)", "Average income (excluding zeros)",
                                    "Number of persons with income"])].copy()
    df["cma_canonical"] = df["GEO"].str.strip().map(CMA_NAME_MAP)
    df = df[df["cma_canonical"].notna()]
    df = df[df["VALUE"].notna() & (df["VALUE"] != "")]

    df["geography_id"] = df["cma_canonical"].map(geo_ids)
    df["date_id"] = (df["REF_DATE"].astype(str) + "00").astype(int)  # annual: YYYY00
    df["income_source"] = df["Income source"].str.strip()
    df["age_group"] = df["Age group"].str.strip()
    df["sex"] = df["Sex"].str.strip()
    df["value_num"] = pd.to_numeric(df["VALUE"], errors="coerce")

    # Pivot statistics into columns
    pivoted = df.pivot_table(
        index=["geography_id", "date_id", "income_source", "age_group", "sex"],
        columns="Statistics",
        values="value_num",
        aggfunc="first",
    ).reset_index()
    pivoted.columns.name = None

    # Normalise column names regardless of which statistics are present
    col_map = {
        "Median income (excluding zeros)":          "median_income",
        "Average income (excluding zeros)":         "avg_income",
        "Number of persons with income":            "num_persons",
    }
    pivoted = pivoted.rename(columns=col_map)
    for col in ("median_income", "avg_income", "num_persons"):
        if col not in pivoted.columns:
            pivoted[col] = None

    out = pivoted[["geography_id", "date_id", "income_source", "age_group", "sex",
                   "median_income", "avg_income", "num_persons"]].dropna(subset=["geography_id"])
    out = out.astype({"geography_id": int, "date_id": int})
    return _write_transformed(out, pid)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

TRANSFORM_MAP = {
    "cpi_cma":        ("fact_monthly",        transform_cpi),
    "gasoline_prices": ("fact_monthly",       transform_gasoline),
    "food_prices":    ("fact_monthly",         transform_food),
    "median_income":  ("fact_annual_income",   None),  # handled separately
}


def main(table_filter: str | None = None) -> None:
    log.info("Fetching dim table IDs from Redshift …")
    geo_ids = fetch_geography_ids()
    ind_ids = fetch_indicator_ids()
    log.info("  %d geographies, %d indicators loaded.", len(geo_ids), len(ind_ids))

    tables = (
        {table_filter: TRANSFORM_MAP[table_filter]}
        if table_filter and table_filter != "all"
        else TRANSFORM_MAP
    )

    errors = []
    for name, (target, fn) in tables.items():
        try:
            if name == "median_income":
                transform_income(geo_ids)
            else:
                fn(geo_ids, ind_ids)
        except Exception as exc:
            log.error("Transform failed for %s: %s", name, exc)
            errors.append(name)

    if errors:
        raise SystemExit(f"Transform failed for: {', '.join(errors)}")
    log.info("StatCan transform complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform StatCan raw CSVs to Redshift-ready format")
    parser.add_argument("--table", choices=list(TRANSFORM_MAP.keys()) + ["all"], default="all")
    args = parser.parse_args()
    main(args.table)
