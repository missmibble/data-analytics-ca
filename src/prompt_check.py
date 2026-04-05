"""
ForeSite Analytics — data availability prompt checker.

Queries Redshift for actual date ranges per indicator and prints the suggested
'Data Availability' section for SYSTEM_PROMPT in src/config.py.

Run after every data load to see whether the system prompt needs updating.

Usage:
    uv run python -m src.prompt_check
"""

import logging
import time

import boto3
from dotenv import load_dotenv

from src.config import AWS_REGION, REDSHIFT_DATABASE, REDSHIFT_WORKGROUP

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

redshift = boto3.client("redshift-data", region_name=AWS_REGION)


# ---------------------------------------------------------------------------
# Redshift Data API helpers (minimal — read-only queries only)
# ---------------------------------------------------------------------------

def _execute(sql: str) -> list[dict]:
    resp = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=sql,
    )
    stmt_id = resp["Id"]
    while True:
        desc = redshift.describe_statement(Id=stmt_id)
        if desc["Status"] == "FINISHED":
            break
        if desc["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Query failed: {desc.get('Error')}")
        time.sleep(2)

    result = redshift.get_statement_result(Id=stmt_id)
    cols = [c["label"] for c in result["ColumnMetadata"]]
    rows = []
    for record in result["Records"]:
        row = {}
        for col, field in zip(cols, record):
            row[col] = list(field.values())[0] if field else None
        rows.append(row)
    return rows


def _range(rows: list[dict], min_col: str = "min_yr", max_col: str = "max_yr") -> tuple[str, str]:
    if not rows or rows[0][min_col] is None:
        return ("(no data)", "(no data)")
    return (str(rows[0][min_col]), str(rows[0][max_col]))


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def fetch_ranges() -> dict:
    ranges = {}

    # CPI (monthly — year range only)
    rows = _execute("""
        SELECT MIN(d.year) AS min_yr, MAX(d.year) AS max_yr
        FROM fact_monthly fm
        JOIN dim_indicator i ON fm.indicator_id = i.indicator_id
        JOIN dim_date d ON fm.date_id = d.date_id
        WHERE i.indicator_name LIKE 'CPI%'
    """)
    ranges["cpi"] = _range(rows)

    # Gasoline prices (monthly)
    rows = _execute("""
        SELECT MIN(d.year) AS min_yr, MAX(d.year) AS max_yr
        FROM fact_monthly fm
        JOIN dim_indicator i ON fm.indicator_id = i.indicator_id
        JOIN dim_date d ON fm.date_id = d.date_id
        WHERE i.indicator_name = 'Gasoline price (per litre)'
    """)
    ranges["gasoline"] = _range(rows)

    # Food prices (monthly)
    rows = _execute("""
        SELECT MIN(d.year) AS min_yr, MAX(d.year) AS max_yr
        FROM fact_monthly fm
        JOIN dim_indicator i ON fm.indicator_id = i.indicator_id
        JOIN dim_date d ON fm.date_id = d.date_id
        WHERE i.indicator_name LIKE 'Food price%'
    """)
    ranges["food"] = _range(rows)

    # Average rents (annual October survey — month=10)
    rows = _execute("""
        SELECT MIN(d.year) AS min_yr, MAX(d.year) AS max_yr
        FROM fact_monthly fm
        JOIN dim_indicator i ON fm.indicator_id = i.indicator_id
        JOIN dim_date d ON fm.date_id = d.date_id
        WHERE i.indicator_name LIKE 'Avg rent%'
          AND d.month = 10
    """)
    ranges["rent"] = _range(rows)

    # Vacancy rates (annual October survey — month=10)
    rows = _execute("""
        SELECT MIN(d.year) AS min_yr, MAX(d.year) AS max_yr
        FROM fact_monthly fm
        JOIN dim_indicator i ON fm.indicator_id = i.indicator_id
        JOIN dim_date d ON fm.date_id = d.date_id
        WHERE i.indicator_name LIKE 'Vacancy rate%'
          AND d.month = 10
    """)
    ranges["vacancy"] = _range(rows)

    # NHPI (monthly)
    rows = _execute("""
        SELECT MIN(d.year) AS min_yr, MAX(d.year) AS max_yr
        FROM fact_monthly fm
        JOIN dim_indicator i ON fm.indicator_id = i.indicator_id
        JOIN dim_date d ON fm.date_id = d.date_id
        WHERE i.indicator_name LIKE 'NHPI%'
    """)
    ranges["nhpi"] = _range(rows)

    # Income (annual — fact_annual_income)
    rows = _execute("""
        SELECT MIN(year) AS min_yr, MAX(year) AS max_yr
        FROM fact_annual_income
    """)
    ranges["income"] = _range(rows)

    return ranges


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_suggested_section(ranges: dict) -> None:
    cpi_min,      cpi_max      = ranges["cpi"]
    gas_min,      gas_max      = ranges["gasoline"]
    food_min,     food_max     = ranges["food"]
    rent_min,     rent_max     = ranges["rent"]
    vac_min,      vac_max      = ranges["vacancy"]
    nhpi_min,     nhpi_max     = ranges["nhpi"]
    inc_min,      inc_max      = ranges["income"]

    print()
    print("=" * 70)
    print("Suggested DATA AVAILABILITY section for SYSTEM_PROMPT")
    print("File: src/config.py")
    print("=" * 70)
    print()
    print("## Data Availability — What Is Actually Loaded")
    print(f"- **CPI**: All-items and Shelter only ({cpi_min}–{cpi_max}). Food, Energy,")
    print("  and Transportation CPI are not published at the CMA level by StatCan")
    print("  — only national/provincial breakdowns exist for those categories.")
    print(f"- **Gasoline prices**: {gas_min}–{gas_max} monthly.")
    print(f"- **Food prices** (bread, milk, eggs): {food_min}–{food_max} monthly.")
    print(f"- **Average rents**: {rent_min}–{rent_max} annual (CMHC October survey).")
    print(f"- **Vacancy rates**: {vac_min}–{vac_max} annual.")
    print("- **Income**: Statistics Canada table 11-10-0239-01, segmented by income")
    print("  source (employment, CPP/QPP, government transfers, child benefits, etc.),")
    print("  age group, and sex. There is no single \"median household income\" field")
    print("  — queries must specify the income source or the result will be partial.")
    print(f"  Range: {inc_min}–{inc_max}.")
    print(f"- **NHPI**: {nhpi_min}–{nhpi_max} monthly. Index base period: December 2016 = 100.")
    print("  Three series: Total (house and land), House only, Land only.")
    print("  Available for 15 CMAs (no Thunder Bay).")
    print()
    print("=" * 70)
    print("Compare this output with the current SYSTEM_PROMPT in src/config.py")
    print("and update any ranges that differ before deploying.")
    print("=" * 70)
    print()


if __name__ == "__main__":
    log.info("Querying Redshift for data ranges …")
    ranges = fetch_ranges()
    print_suggested_section(ranges)
