# ForeSite Analytics

HR recruitment analytics tool providing a natural language interface to Canadian economic indicators across major census metropolitan areas (CMAs). Built on AWS with a Strands AI agent backed by Amazon Bedrock Knowledge Bases.

**Team:** Kristen · Guy · Natalia · Jenna — SAIT Data Analytics Capstone, April 2026

---

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) — Python package manager
- AWS CLI configured with credentials for `ca-central-1`
- Python 3.11+

---

## 1. Python Environment

**One-time setup only** — skip this step on subsequent sessions.

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install all dependencies
uv venv --python 3.11
uv pip install -e ".[dev]"
```

After this, the `.venv` folder in the project directory persists. All subsequent commands use `uv run`, which finds it automatically — no setup needed each session.

---

## 2. Configuration

```bash
cp .env.example .env
```

Open `.env` and fill in:

| Variable | Description |
|----------|-------------|
| `AWS_REGION` | `ca-central-1` (pre-filled) |
| `S3_BUCKET_RAW` | S3 bucket for raw ingested data |
| `S3_BUCKET_DOCS` | S3 bucket for CMHC docs (archival, not queried by agent) |
| `REDSHIFT_WORKGROUP` | Redshift Serverless workgroup name |
| `REDSHIFT_DATABASE` | Redshift database name |
| `STRUCTURED_KB_ID` | Populated automatically by `infra/setup.py` |
| `ALLOWED_ORIGINS` | `*` during development; set to external site URL when known |
| `API_GATEWAY_URL` | Populated automatically by `infra/setup.py` |
| `STREAM_URL` | Lambda Function URL for streaming chat (populated automatically) |

---

## 3. Provision AWS Infrastructure

Provisions S3 buckets, Redshift Serverless, Bedrock Knowledge Bases, Lambda, and API Gateway in a single idempotent script. Safe to re-run.

```bash
uv run python infra/setup.py
```

You will be prompted for a Redshift admin password (min 8 chars, must include uppercase, lowercase, and a digit).

On completion, the script prints `STRUCTURED_KB_ID`, `API_GATEWAY_URL`, and `STREAM_URL` — copy these into your `.env` file.

To provision infrastructure only (skip Lambda/API Gateway):

```bash
uv run python infra/setup.py --skip-lambda
```

---

## 4. Ingest Data

### Statistics Canada (automated)

Fetches all five tables from the Statistics Canada WDS API and uploads raw CSVs to S3. Idempotent — skips tables already fetched today.

```bash
# All tables
uv run python -m src.ingest.statcan

# Single table
uv run python -m src.ingest.statcan --table cpi_cma
```

Available table keys: `cpi_cma`, `median_income`, `food_prices`, `gasoline_prices`, `nhpi`

### CMHC Rental Market Survey (manual + batch)

CMHC data has no public API. Download the annual Excel files from [CMHC Housing Data Tables](https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/rental-market) and place them in `data/cmhc/`, named by year (e.g. `2024.xlsx`).

```bash
# All files in data/cmhc/
uv run python -m src.ingest.cmhc

# Single file
uv run python -m src.ingest.cmhc --file data/cmhc/2024.xlsx
```

---

## 5. Transform

Reads raw CSVs from S3, maps geography/indicator names to Redshift dim table IDs, and writes COPY-ready CSVs back to S3 under the `transformed/` prefix. Must run before loading.

```bash
# All StatCan tables
uv run python -m src.transform.statcan

# Single table
uv run python -m src.transform.statcan --table cpi_cma
```

Available table keys: `cpi_cma`, `gasoline_prices`, `food_prices`, `median_income`

**Note:** Food prices (`1810024501`) are only available at the provincial level. The transform replicates provincial values to all target CMAs within each province.

---

## 6. Load into Redshift

Reads transformed CSVs from S3 and upserts into the star schema. Run after Step 5.

```bash
# All sources
uv run python -m src.load.redshift_loader

# Single source
uv run python -m src.load.redshift_loader --source statcan
uv run python -m src.load.redshift_loader --source cmhc
```

---

## 6b. Update System Prompt After Each Load

The agent's **Data Availability** section in `src/config.py` lists the date ranges for each indicator. These must be kept in sync with what's actually in Redshift — an outdated prompt causes the agent to misreport what data is available.

**Run this after every data load:**

```bash
uv run python -m src.prompt_check
```

The script queries Redshift and prints the suggested Data Availability text. Compare it with the current `SYSTEM_PROMPT` in `src/config.py` (the `## Data Availability — What Is Actually Loaded` section) and update any ranges that differ.

After editing `src/config.py`, redeploy the Lambda to pick up the change:

```bash
uv run python infra/setup.py --skip-infra
```

---

## 7. Run the Agent

### Local CLI

```bash
uv run python -m src.agent
```

### Local API server

```bash
uv run python -m src.api
# API available at http://localhost:8000
```

Test with curl:

```bash
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "What was the average 2-bedroom rent in Calgary in 2024?"}'
```

Health check:

```bash
curl http://localhost:8000/health
```

---

## 8. Update CORS for External Website

When the external site URL is available, no code changes are needed — update two settings:

1. **Lambda environment variable** — set `ALLOWED_ORIGINS` to the external URL (e.g. `https://client-site.com`)
2. **API Gateway CORS config** — update the allowed origins to match

For multiple origins, use a comma-separated list: `https://site-a.com,https://site-b.com`

---

## 9. Teardown

To permanently destroy all AWS resources created by this project:

```bash
uv run python infra/teardown.py
```

You will be prompted to type `destroy` to confirm. This removes — in order — the API Gateway, Lambda, Bedrock Structured KB, Redshift Serverless, S3 buckets (including all data), and IAM roles.

To skip the confirmation prompt:

```bash
uv run python infra/teardown.py --yes
```

To remove only the OpenSearch Serverless collection and Bedrock Vector KB (if previously provisioned), without affecting Redshift or the API:

```bash
uv run python infra/teardown.py --vector-kb-only
```

---

## Validation Queries

Run these against the deployed API Gateway endpoint to confirm end-to-end function:

| Query | Expected behaviour |
|-------|-------------------|
| "What was the average 2-bedroom rent in Calgary in 2024?" | `query_structured_kb` tool call |
| "Compare CPI shelter index for Toronto and Vancouver from 2022 to 2024." | `query_structured_kb` tool call |
| "Which city has the lowest vacancy rate?" | `query_structured_kb` tool call |
| "How is the NHPI calculated?" | Answered from inline methodology context — no tool call |
| "For a mid-level analyst earning $85,000, which CMA offers the best affordability?" | `query_structured_kb` + inline context |

---

## Architecture

```
Statistics Canada WDS API  ──→ S3 (raw CSVs)  ──→ ETL ──→ Redshift Serverless
CMHC Excel (annual batch)  ──→ S3 (raw Excel) ──→ ETL ──→ Redshift Serverless
CMHC docs / methodology    ──→ S3 (archival only — not queried by agent)

Redshift Serverless ──→ Bedrock Structured KB (NL-to-SQL)
                        ↓
                    Strands agent (query_structured_kb tool)
                    + inline methodology context (src/config.py SYSTEM_PROMPT)
                        ↓
                    FastAPI (src/api.py)
                        ↓
                    AWS Lambda + API Gateway (ca-central-1)
                        ↓
                    External website (CORS-controlled)
```

See `KB_Architecture_Plan.md` for full data source assessment and KB design decisions.
