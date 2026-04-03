# HR Recruitment Tool - Data Sources

Hello,

Below are the data sources used in the HR Recruitment Tool (ForeSite Analytics). You can access the original data directly from these authoritative government and industry sources:

---

> **Note:** This document was updated by Claude (Anthropic) on 2026-04-03 to reflect the actual data files ingested into the system. The original version listed planned sources; this revision documents what was actually loaded into the Redshift database and the date ranges available.

---

## **1. Statistics Canada**

### Consumer Price Index (CPI) by Census Metropolitan Area
- **What it provides:** Monthly CPI by city across 5 categories: All-items, Food, Shelter, Transportation, Energy
- **Table:** 18-10-0004-12 (CMA-level)
- **Access:** https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810000412
- **Frequency:** Monthly
- **Format:** CSV (downloaded via StatCan WDS API)
- **Date range loaded:** 2015–2026
- **CMAs covered:** All 16 target CMAs
- **Categories available at CMA level:** `CPI - All-items` and `CPI - Shelter` only. StatCan table 18-10-0004-12 does not publish Food, Energy, or Transportation CPI at the CMA level — these breakdowns are only available nationally or provincially (not in this table).

### Median and Average Income
- **What it provides:** Annual median income, average income, and number of earners by CMA, income source, age group, and sex
- **Table:** 11-10-0239-01
- **Access:** https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1110023901
- **Frequency:** Annual
- **Format:** CSV (downloaded via StatCan WDS API)
- **Date range loaded:** 2015–2022
- **CMAs covered:** All 16 target CMAs

### Food Prices (Provincial proxy)
- **What it provides:** Average retail prices for bread, milk, and eggs by province. No CMA-level breakdown exists — provincial values are replicated to all target CMAs within each province.
- **Table:** 18-10-0245-01
- **Access:** https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810024501
- **Frequency:** Monthly
- **Format:** CSV (downloaded via StatCan WDS API)
- **Date range loaded:** 2017–2024
- **Note:** Provincial-level only. Values are assigned to CMAs by province.

### Gasoline Prices
- **What it provides:** Average pump prices (regular unleaded, self-serve) by city
- **Table:** 18-10-0001-01
- **Access:** https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810000101
- **Frequency:** Monthly
- **Format:** CSV (downloaded via StatCan WDS API)
- **Date range loaded:** 2015–2024
- **CMAs covered:** All 16 target CMAs

---

## **2. Canada Mortgage and Housing Corporation (CMHC)**

### Rental Market Survey — Vacancy Rates by Bedroom Type
- **What it provides:** Annual vacancy rates by bedroom type (Bachelor, 1 Bedroom, 2 Bedroom, 3 Bedroom+, Total) for private apartments
- **Source table:** Table 1.1.1 — Vacancy Rates by Bedroom Type and Census Subdivision
- **Access:** https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/rental-market
- **Frequency:** Annual (October survey)
- **Format:** Excel (.xlsx) — manual download required; no public API
- **File format used:** CSD sheet with Census Subdivision breakdown; CMA totals extracted from rows where Census Subdivision = "Total" and Dwelling Type = "Total"
- **Years loaded:** 2020, 2021, 2022, 2023
- **CMAs covered:** All 16 target CMAs (including Charlottetown)

### Rental Market Survey — Average Rents by Bedroom Type
- **What it provides:** Average occupied-unit rents by bedroom type (Bachelor, 1 Bedroom, 2 Bedroom, 3 Bedroom+, Total)
- **Source table:** Average Rents of Vacant and Occupied Units by Zone and Bedroom Type
- **Access:** https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/rental-market
- **Frequency:** Annual (October survey)
- **Format:** Excel (.xlsx) — manual download required; no public API
- **File format used:** ARent_Vac_Occ sheet; CMA rows identified by "CMA" suffix; Occupied Units column used
- **Years loaded:** 2019, 2020
- **CMAs covered:** 15 target CMAs (excludes Charlottetown — CA, not CMA)
- **⚠️ Data only available to 2020.** CMHC discontinued the ARent_Vac_Occ sheet format after 2020; later RMR releases do not include average rent by bedroom type in a parseable format. 2019 and 2020 are the full extent of available data.

---

## **3. Canadian Real Estate Association (CREA) — Not Used**

CREA's MLS Home Price Index was originally considered but requires REALTOR® membership for detailed data. It was replaced by Statistics Canada's New Housing Price Index (NHPI), which is publicly available.

---

## **4. Statistics Canada — New Housing Price Index (NHPI)**
- **What it provides:** Monthly new house price index by CMA (Total, House only, Land only). Base period: December 2016 = 100
- **Table:** 18-10-0205-01
- **Access:** https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810020501
- **Frequency:** Monthly
- **Format:** CSV (StatCan WDS API)
- **Date range loaded:** 1981–2025
- **CMAs covered:** 15 target CMAs (Thunder Bay not included in StatCan NHPI coverage)

---

## **Additional Notes:**

- **Data Grain:** Monthly data (CPI, rent, gasoline, food prices) uses date_id format YYYYMM. Annual data (income) uses YYYY00. CMHC rental data is an October annual survey, stored as YYYYMM where MM=10.
- **Geographic Coverage:** 16 major Canadian CMAs: Toronto, Vancouver, Calgary, Montreal, Ottawa-Gatineau, Edmonton, Winnipeg, Quebec City, Halifax, Victoria, Regina, Saskatoon, Saint John, Charlottetown, St. John's, Thunder Bay.
- **Data Cleaning:** City names are standardized via a canonical CMA name map (see `src/config.py`). Each source uses different naming conventions (e.g. "Montréal" → "Montreal", "Ottawa" → "Ottawa-Gatineau").
- **Architecture:** AWS Redshift Serverless star schema → Amazon Bedrock Structured Knowledge Base (NL-to-SQL) + Vector Knowledge Base → Strands AI agent → FastAPI + Lambda + API Gateway.

---

## **Quick Links Summary:**

| Source | Link |
|--------|------|
| Statistics Canada - CPI by CMA (18-10-0004-12) | https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810000412 |
| Statistics Canada - Income (11-10-0239-01) | https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1110023901 |
| Statistics Canada - Food Prices (18-10-0245-01) | https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810024501 |
| Statistics Canada - Gasoline (18-10-0001-01) | https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810000101 |
| Statistics Canada - NHPI (18-10-0205-01) | https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810020501 |
| CMHC - Housing Market Data Tables | https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/rental-market |

---

If you have any questions about accessing or interpreting this data, feel free to reach out.

Best regards,
**ForeSite Analytics**
Kristen • Guy • Natalia • Jenna
SAIT Data Analytics Capstone | April 2026
