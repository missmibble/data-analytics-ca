"""
Central configuration: CMA name mapping, StatCan table registry, S3 paths, agent system prompt.
"""

import os

# ---------------------------------------------------------------------------
# AWS / S3
# ---------------------------------------------------------------------------
AWS_REGION = os.getenv("AWS_REGION", "ca-central-1")
S3_BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "foresite-raw-ca-383429078788")
S3_BUCKET_DOCS = os.getenv("S3_BUCKET_DOCS", "foresite-docs-ca-383429078788")
REDSHIFT_WORKGROUP = os.getenv("REDSHIFT_WORKGROUP", "foresite-wg")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE", "foresite")
STRUCTURED_KB_ID = os.getenv("STRUCTURED_KB_ID", "")
VECTOR_KB_ID = os.getenv("VECTOR_KB_ID", "")

# ---------------------------------------------------------------------------
# Statistics Canada WDS table registry
# pid: table product ID used in WDS API calls
# ---------------------------------------------------------------------------
STATCAN_TABLES = {
    "cpi_cma": {
        "pid": "1810000412",  # Table 18-10-0004-12: CPI by CMA (corrected from 1810000401 in reference list)
        "description": "Consumer Price Index by CMA",
        "frequency": "monthly",
    },
    "median_income": {
        "pid": "1110023901",
        "description": "Median income by CMA, age, sex, and income source",
        "frequency": "annual",
    },
    "food_prices": {
        "pid": "1810024501",
        "description": "Average retail food prices by city",
        "frequency": "monthly",
    },
    "gasoline_prices": {
        "pid": "1810000101",
        "description": "Average gasoline pump prices by province and select cities",
        "frequency": "monthly",
    },
    "nhpi": {
        "pid": "1810020501",  # New Housing Price Index — CMA level (table 18-10-0205-01)
        "description": "New Housing Price Index by CMA",
        "frequency": "monthly",
    },
}

WDS_BASE_URL = "https://www.statcan.gc.ca/en/json"

# ---------------------------------------------------------------------------
# CMA canonical name mapping
# Each source uses different naming conventions; all map to the canonical form.
# ---------------------------------------------------------------------------
CMA_NAME_MAP: dict[str, str] = {
    # Toronto variants
    "Toronto": "Toronto",
    "Toronto, Ontario": "Toronto",
    "Toronto CMA": "Toronto",
    # Vancouver variants
    "Vancouver": "Vancouver",
    "Vancouver, British Columbia": "Vancouver",
    "Vancouver CMA": "Vancouver",
    "Metro Vancouver": "Vancouver",
    # Calgary variants
    "Calgary": "Calgary",
    "Calgary, Alberta": "Calgary",
    "Calgary CMA": "Calgary",
    # Montreal variants
    "Montréal": "Montreal",
    "Montreal": "Montreal",
    "Montréal, Quebec": "Montreal",
    "Montreal CMA": "Montreal",
    # Ottawa variants
    "Ottawa": "Ottawa-Gatineau",
    "Ottawa-Gatineau": "Ottawa-Gatineau",
    "Ottawa–Gatineau": "Ottawa-Gatineau",
    "Ottawa - Gatineau": "Ottawa-Gatineau",
    "Ottawa-Gatineau, Ontario part": "Ottawa-Gatineau",
    "Ottawa-Gatineau, Ontario part, Ontario/Quebec": "Ottawa-Gatineau",
    # Edmonton variants
    "Edmonton": "Edmonton",
    "Edmonton, Alberta": "Edmonton",
    "Edmonton CMA": "Edmonton",
    # Winnipeg variants
    "Winnipeg": "Winnipeg",
    "Winnipeg, Manitoba": "Winnipeg",
    # Quebec City variants
    "Québec": "Quebec City",
    "Quebec City": "Quebec City",
    "Québec, Quebec": "Quebec City",
    # Halifax variants
    "Halifax": "Halifax",
    "Halifax, Nova Scotia": "Halifax",
    # Victoria variants
    "Victoria": "Victoria",
    "Victoria, British Columbia": "Victoria",
    # Regina variants
    "Regina": "Regina",
    "Regina, Saskatchewan": "Regina",
    # Saskatoon variants
    "Saskatoon": "Saskatoon",
    "Saskatoon, Saskatchewan": "Saskatoon",
    # Saint John variants
    "Saint John": "Saint John",
    "Saint John, New Brunswick": "Saint John",
    # Charlottetown variants
    "Charlottetown": "Charlottetown",
    "Charlottetown-Summerside": "Charlottetown",
    "Charlottetown, Prince Edward Island": "Charlottetown",
    # St. John's variants
    "St. John's": "St. John's",
    "St. John's, Newfoundland and Labrador": "St. John's",
    # Thunder Bay variants
    "Thunder Bay": "Thunder Bay",
    "Thunder Bay, Ontario": "Thunder Bay",
}

# Canonical list of target CMAs
TARGET_CMAS = sorted(set(CMA_NAME_MAP.values()))

# ---------------------------------------------------------------------------
# CMHC Excel sheet targets
# ---------------------------------------------------------------------------
CMHC_VACANCY_SHEET = "1.1.1"       # Vacancy rates by bedroom type
CMHC_RENT_SHEET = "3.1.1"          # Average rents by bedroom type

CMHC_BEDROOM_TYPES = ["Bachelor", "1 Bedroom", "2 Bedroom", "3 Bedroom +", "Total"]

# ---------------------------------------------------------------------------
# Strands agent system prompt
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are ForeSite Analytics, an AI assistant for Canadian HR and workforce planning.

You help HR professionals, recruiters, and business leaders make data-driven decisions about talent acquisition and workforce planning across major Canadian cities.

## Your Data
You have access to Canadian economic indicators for these census metropolitan areas (CMAs):
Toronto, Vancouver, Calgary, Montreal, Ottawa-Gatineau, Edmonton, Winnipeg, Quebec City, Halifax, Victoria, Regina, Saskatoon, Saint John, Charlottetown, St. John's, Thunder Bay.

Data sources:
- **Statistics Canada**: Consumer Price Index (CPI) by category, median household income, average food prices, gasoline prices — monthly and annual
- **CMHC**: Average rents by bedroom type, vacancy rates — annual
- **Statistics Canada NHPI**: New Housing Price Index by CMA — monthly

## Your Tools
- Use **query_structured_kb** for specific numeric questions: rents, CPI values, income figures, vacancy rates, price indices, comparisons across cities or time periods.
- Use **retrieve** for contextual or methodological questions: how indicators are calculated, data definitions, survey methodology.
- For complex questions (e.g. "best city for a $90k salary"), use both tools to combine numeric data with context.

## Data Availability — What Is Actually Loaded
- **CPI**: All-items and Shelter only (2015–2026). Food, Energy, and Transportation CPI are not published at the CMA level by StatCan — only national/provincial breakdowns exist for those categories.
- **Gasoline prices**: 2015–2026 monthly.
- **Food prices** (bread, milk, eggs): 2017–2026 monthly.
- **Average rents**: 2019–2020 only (CMHC data after 2020 not available in parseable format).
- **Vacancy rates**: 2020–2023 annual.
- **Income**: Statistics Canada table 11-10-0239-01, segmented by income source (employment, CPP/QPP, government transfers, child benefits, etc.), age group, and sex. There is no single "median household income" field — queries must specify the income source or the result will be partial.
- **NHPI**: 1981–2025 monthly. Index base period: December 2016 = 100. Three series: Total (house and land), House only, Land only. Available for 15 CMAs (no Thunder Bay).

## Tool Use Rules
- Make **at most 3 tool calls** per response. If the first query does not return data, report what is available rather than retrying with variations.
- If data is not found, tell the user what IS available (e.g. different year range, different indicator) rather than making additional queries.
- Do not retry the same query with minor wording changes.

## Response Guidelines
- Always cite the data source (e.g. "Statistics Canada CPI, March 2024") and time period.
- When comparing cities, present data in a table where possible.
- Flag when data is annual vs. monthly, as this affects recency.
- If a query is outside the available data scope, say so clearly rather than guessing.
"""
