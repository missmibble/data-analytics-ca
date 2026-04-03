# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**ForeSite Analytics** — SAIT Data Analytics Capstone project (April 2026) by Kristen, Guy, Natalia, and Jenna.

An HR recruitment analytics tool that combines Canadian economic indicators (cost of living, housing, income) to support workforce planning decisions. Covers major Canadian census metropolitan areas (CMAs): Toronto, Vancouver, Calgary, Montreal, Ottawa, Edmonton, etc.

## Architecture

**Stack:** Microsoft Fabric (Delta Lake) → PySpark (cleaning) → Star schema (15 relationships) → Power BI (Direct Lake mode)

**Data pipeline:**
1. Ingest raw CSVs/Excel from three sources into Microsoft Fabric Delta Lake
2. Clean with PySpark: standardize city names, handle missing values, align geography keys across sources (each uses different naming conventions)
3. Model as star schema for Power BI Direct Lake mode

**Data sources:**
| Source | Data | Frequency | Format |
|--------|------|-----------|--------|
| Statistics Canada | CPI by CMA/category (table 18-10-0004-12), median income by CMA, food prices, gasoline prices | Monthly (CPI/food/gas), Annual (income) | CSV |
| CMHC | Average rent by bedroom type, vacancy rates | Monthly + Annual | Excel, PDF |
| StatCan NHPI | New housing price index by CMA (replaces CREA HPI — REALTOR® access required) | Monthly | CSV |

**Temporal alignment:** Monthly data (CPI, rent, HPI) is joined with annual income data via a date dimension table.

## AI Agent Layer

A Strands SDK agent (`strands-agents` + `strands-agents-tools`) will provide a natural language interface over two Amazon Bedrock Knowledge Bases in `ca-central-1`:

- **`foresite-structured-kb`** — NL-to-SQL via Redshift Serverless. All StatCan and CMHC tabular data. Used for precise numeric/time-series queries.
- **`foresite-vector-kb`** — S3-backed vector KB. CMHC narrative PDFs, methodology docs, data definitions. Used for contextual/semantic queries.

All StatCan sources use the WDS REST API (no auth, Open Government Licence). CMHC requires annual batch ETL from Excel. See `KB_Architecture_Plan.md` for full ingestion pipeline and open items.

## Current State

The repository is in early documentation phase. `Data_Sources_Reference_List.md` contains data source URLs; `KB_Architecture_Plan.md` contains the KB and agent architecture decisions. No ETL code, notebooks, or Power BI files exist yet.
