# ForeSite Analytics — Build Plan

## Context
Architecture and data source decisions are finalized in `KB_Architecture_Plan.md`. The next phase is to build the actual system: data ingestion scripts, AWS infrastructure (S3, Redshift Serverless, Bedrock KBs), and the Strands agent. The goal is a working natural language interface over Canadian HR/economic data for major CMAs, exposed as a REST API for embedding in an external website.

All work targets AWS `ca-central-1`. The Strands agent is the primary deliverable.

---

## Phases

### Phase 1 — Project Scaffold
**Files to create:**
- `pyproject.toml` — Python 3.11+, dependencies: `strands-agents`, `strands-agents-tools`, `boto3`, `pandas`, `openpyxl`, `requests`, `python-dotenv`, `fastapi`, `mangum`, `uvicorn`
- `.env.example` — `AWS_REGION`, `S3_BUCKET_RAW`, `S3_BUCKET_DOCS`, `REDSHIFT_WORKGROUP`, `REDSHIFT_DATABASE`, `STRUCTURED_KB_ID`, `VECTOR_KB_ID`, `ALLOWED_ORIGINS` (set to `*` initially; update to external site URL when known)
- `src/config.py` — CMA name canonical mapping (each source uses different naming conventions), StatCan table PID registry, S3 paths
- `.gitignore`

---

### Phase 2 — StatCan Ingestion (`src/ingest/statcan.py`)
Uses the WDS REST API — no auth, no rate-limit concern with full-table ZIP downloads.

**Tables to fetch:**
| Table | PID | Freq |
|-------|-----|------|
| CPI by CMA | `1810000412` | Monthly |
| Median Income | `1110023901` | Annual |
| Food Prices | `1810024501` | Monthly |
| Gasoline Prices | `1810000101` | Monthly |
| NHPI | `324befd1` (Open Gov Portal) | Monthly |

**Logic:**
1. `GET https://www.statcan.gc.ca/en/json/getFullTableDownloadCSV/{pid}/en` → returns ZIP download URL
2. Download ZIP → extract CSV → upload raw file to `s3://{S3_BUCKET_RAW}/statcan/{pid}/YYYY-MM-DD.csv`
3. Idempotent — skip if today's file already exists in S3

---

### Phase 3 — CMHC Ingestion (`src/ingest/cmhc.py`)
Annual batch. Excel files downloaded manually (no API) — script assumes files are placed in a local `data/cmhc/` drop folder or uploaded directly to S3.

**Logic:**
1. For each Excel file in drop folder: parse with `openpyxl`
2. Extract sheets: `1.1.1` (vacancy rates) and `3.1.1` (average rents)
3. Normalize: melt wide-format tables to long (CMA, bedroom_type, year, value)
4. Apply CMA name mapping from `config.py` to standardize geography keys
5. Upload normalized CSV to `s3://{S3_BUCKET_RAW}/cmhc/YYYY.csv`

**Note:** Sheet name discovery needed on first run — script will log all sheet names on first parse.

---

### Phase 4 — Redshift Schema (`infra/redshift_schema.sql`)
Star schema DDL matching the 15-relationship model in the architecture plan.

**Core tables:**
- `dim_geography` — CMA name, province, CMA code
- `dim_date` — year, month, quarter (bridges monthly ↔ annual data)
- `dim_indicator` — indicator name, source, unit, category
- `fact_monthly` — geography_id, date_id, indicator_id, value
- `fact_annual` — geography_id, year, median_income, income_source, age_group, sex

**Load script:** `src/load/redshift_loader.py`
- Reads normalized CSVs from S3
- Uses `COPY` command via `boto3` Redshift Data API (no direct JDBC needed)
- Upserts on (geography_id, date_id, indicator_id)

---

### Phase 5 — AWS Infrastructure (`infra/setup.py`)
Boto3 script to provision (idempotent, safe to re-run):

1. **S3:** Create `foresite-raw-ca` and `foresite-docs-ca` buckets in `ca-central-1`
2. **Redshift Serverless:** Create namespace `foresite-ns` + workgroup `foresite-wg`; run `redshift_schema.sql`
3. **Bedrock Vector KB:** Create `foresite-vector-kb` backed by `foresite-docs-ca` S3 bucket; trigger initial sync
4. **Bedrock Structured KB:** Create `foresite-structured-kb` connected to Redshift workgroup; register fact/dim tables
5. **Lambda function:** Package `src/api.py` + dependencies as a ZIP; deploy with IAM role granting Bedrock + Redshift Data API access
6. **API Gateway (HTTP API):** Route `POST /chat` and `GET /health` to Lambda; enable CORS at the gateway level with `ALLOWED_ORIGINS=*` placeholder — update to external site URL when provided
7. Output KB IDs and API Gateway invoke URL to `.env`

---

### Phase 6 — Strands Agent (`src/agent.py`)
Core agent logic, kept infrastructure-agnostic so it can be invoked from both the API handler and locally.

```python
from strands import Agent
from strands_tools import retrieve
import boto3, os

bedrock_runtime = boto3.client("bedrock-agent-runtime", region_name="ca-central-1")

def query_structured_kb(query: str) -> str:
    """Precise numeric/time-series lookup via NL-to-SQL."""
    response = bedrock_runtime.retrieve_and_generate(
        input={"text": query},
        retrieveAndGenerateConfiguration={
            "type": "KNOWLEDGE_BASE",
            "knowledgeBaseConfiguration": {
                "knowledgeBaseId": os.environ["STRUCTURED_KB_ID"],
                "modelArn": "arn:aws:bedrock:ca-central-1::foundation-model/anthropic.claude-sonnet-4-5-20250929-v1:0",
            }
        }
    )
    return response["output"]["text"]

def get_agent() -> Agent:
    return Agent(tools=[retrieve, query_structured_kb], system_prompt=SYSTEM_PROMPT)
```

System prompt covers: routing rules, CMA scope, citation format (source + time period), HR use-case framing.

---

### Phase 6b — REST API (`src/api.py`)
FastAPI app exposing the agent as an HTTP endpoint. Deployed on AWS Lambda via `mangum`.

**Endpoints:**
- `POST /chat` — accepts `{"message": "..."}`, returns `{"response": "..."}`
- `GET /health` — health check

**CORS configuration:**
- `ALLOWED_ORIGINS` env var — set to `*` during development; update to the external site URL when provided (comma-separated for multiple origins)
- Headers: `Content-Type`, `Authorization`
- Methods: `POST`, `GET`, `OPTIONS`

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
import os

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("ALLOWED_ORIGINS", "*").split(","),
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

@app.post("/chat")
async def chat(body: ChatRequest):
    agent = get_agent()
    return {"response": str(agent(body.message))}

handler = Mangum(app)  # Lambda entry point
```

**CORS update process (when external URL is available):**
1. Set `ALLOWED_ORIGINS=https://client-site.com` in Lambda environment variables
2. Update API Gateway CORS allowed origins to match
3. No code changes required

---

### Phase 7 — Validation Queries
Run these against the deployed API Gateway endpoint to confirm end-to-end function:

1. `"What was the average 2-bedroom rent in Calgary in 2024?"` → structured KB
2. `"Compare CPI shelter index for Toronto and Vancouver from 2022 to 2024."` → structured KB
3. `"Which city has the lowest vacancy rate?"` → structured KB
4. `"How is the NHPI calculated?"` → vector KB
5. `"For a mid-level analyst earning $85,000, which CMA offers the best affordability?"` → both KBs

---

## Build Order
```
Phase 1 (scaffold) → Phase 2 (StatCan ingest) → Phase 3 (CMHC ingest)
       ↓
Phase 4 (Redshift schema) → Phase 5 (AWS infra) → load data
       ↓
Phase 6 (agent) → Phase 6b (REST API + CORS) → Phase 7 (validate)
```
Phases 2 and 3 can be developed in parallel. Phase 5 requires AWS credentials configured locally (`aws configure`).

When the external site URL is known, update `ALLOWED_ORIGINS` in the Lambda environment variables and API Gateway CORS config — no code changes required.

---

## Key Files
```
data-analytics-ca/
├── pyproject.toml
├── .env.example
├── src/
│   ├── config.py              # CMA mapping, PIDs, S3 paths, system prompt
│   ├── ingest/
│   │   ├── statcan.py         # WDS API fetcher (all 5 tables)
│   │   └── cmhc.py            # Excel parser (vacancy + rent sheets)
│   ├── load/
│   │   └── redshift_loader.py # S3 → Redshift via Data API
│   ├── agent.py               # Strands agent core (infrastructure-agnostic)
│   └── api.py                 # FastAPI + Mangum Lambda handler, CORS middleware
├── infra/
│   ├── redshift_schema.sql    # Star schema DDL
│   └── setup.py               # Boto3 provisioning (S3, Redshift, KBs, Lambda, API GW)
└── data/
    └── cmhc/                  # Manual Excel drop folder (not committed to git)
```
