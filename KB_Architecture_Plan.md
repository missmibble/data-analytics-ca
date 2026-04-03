# Knowledge Base Architecture Plan
## ForeSite Analytics — AI Agent Data Access Strategy

---

## Data Source Assessment

### Source 1 — Statistics Canada CPI (Table 18-10-0004-12)

| Attribute | Detail |
|-----------|--------|
| Format | CSV (5 variants including full-table ZIP) |
| API | Yes — Statistics Canada WDS REST API, no auth required |
| Volume | ~5–10 MB uncompressed across all CMAs |
| Access | Free, Open Government Licence |
| Blocker | None |

**CMA coverage:** All 16 target CMAs (Toronto, Vancouver, Calgary, Montreal, Ottawa-Gatineau, Edmonton, Victoria, Winnipeg, Quebec City, Halifax, Regina, Saskatoon, Saint John, Charlottetown, St. John's, Thunder Bay).

**Key WDS API endpoints:**
```
GET https://www.statcan.gc.ca/en/json/getFullTableDownloadCSV/{pid}/en
GET https://www.statcan.gc.ca/en/json/getDataFromCubePidCoordAndLatestNPeriods/{pid}/{coord}/{n}
```
Use PID `1810000412` for CMA-level CPI (% change). PID `1810000401` for national/provincial all-items index.

**KB fit:** Structured KB (NL-to-SQL) — primary choice. Time-series tabular data is best queried with precision via SQL, not semantic similarity.

---

### Source 2 — Statistics Canada Median Income (Table 11-10-0239-01)

| Attribute | Detail |
|-----------|--------|
| Format | CSV (4 variants including full-table ZIP) |
| API | Yes — same WDS REST API, PID `1110023901` |
| Volume | <1 MB (annual frequency, 21 CMAs) |
| Access | Free, Open Government Licence |
| Blocker | None |

**Dimensions:** Geography × Age group × Sex × Income source type × Measure (median, average, aggregate). Annual frequency requires date-dimension bridging to join with monthly data.

**KB fit:** Structured KB — best loaded as a dimension/fact table. Small enough to embed entirely. Annual frequency slots naturally into the existing star schema date dimension.

---

### Source 3 — Statistics Canada Food & Gasoline Prices

| Attribute | Detail |
|-----------|--------|
| Tables | Food: PID `1810024501` / Gas: PID `1810000101` |
| Format | CSV via WDS API |
| Access | Free, Open Government Licence |
| Blocker | None |

Same WDS API as CPI. Monthly frequency. Food prices by city and product category; gasoline by province and select cities.

**KB fit:** Structured KB alongside CPI.

---

### Source 4 — CMHC Rental Market Survey

| Attribute | Detail |
|-----------|--------|
| Format | Excel (.xlsx, multi-sheet per edition) |
| API | None |
| Volume | ~7 annual editions, ~5–20 sheets each (~50 MB total) |
| Access | Public, no account required |
| Blocker | Excel parsing required before ingestion |

**Available data:** Annual editions (2019–2025). Tables include: vacancy rates by bedroom type (Table 1.1.1), average rents by bedroom type (Table 3.1.1), universe counts, turnover rates, condo apartment supplement data for 17 centres.

**Ingestion approach:** Batch ETL using `openpyxl` or PySpark Excel reader to extract target sheets → normalize to CSV → load to S3 → sync to KB. Re-run annually when CMHC publishes new edition.

**KB fit:** Structured KB after ETL. Annual batch ingestion workflow.

---

### Source 5 — Statistics Canada New Housing Price Index (NHPI)

*Replaces CREA MLS HPI, which requires REALTOR® membership and has no public API.*

| Attribute | Detail |
|-----------|--------|
| Table | Open Government Portal dataset `324befd1-893b-42e6-bece-6d30af3dd9f1` |
| Format | CSV via WDS API |
| API | Yes — same WDS REST API, free, no auth required |
| Frequency | Monthly (from January 1981) |
| Access | Free, Open Government Licence |
| Blocker | None |

**Coverage:** New residential construction prices by CMA. Uses a hedonic pricing model similar to CREA HPI methodology but scoped to new builds. Covers all target CMAs.

**Note:** NHPI covers new construction only, not resale. This is a methodological difference from the original CREA HPI. For the purposes of HR workforce planning (cost-of-living signal, housing affordability trends), NHPI provides a consistent and fully programmatic substitute.

**KB fit:** Structured KB alongside CPI and income data. Same WDS ingestion pipeline as other StatCan sources.

---

## KB Architecture Decision

### Single KB vs. Multiple KBs

**Recommendation: Two KBs with distinct roles.**

| KB | Type | Backend | Purpose |
|----|------|---------|---------|
| `foresite-structured-kb` | Structured (NL-to-SQL) | Amazon Redshift Serverless | Precise numeric queries: "What was Calgary's average 2BR rent in 2023?" "Compare CPI shelter index across cities from 2022–2024." |
| `foresite-vector-kb` | Vector (RAG) | S3 + Amazon Bedrock (with embeddings) | Semantic/contextual queries over narrative content: CMHC reports, methodology notes, annual report summaries, data definitions |

**Why not a single KB:**
- The data is predominantly structured and time-series. Vector similarity search over numeric CSV rows produces poor precision for quantitative queries (e.g., "lowest rent city in 2023" requires sorting/filtering, not semantic matching).
- Structured KB uses Bedrock's NL-to-SQL path (Redshift) for exact numeric answers.
- Vector KB handles any unstructured content (CMHC narrative sections, methodology PDFs, data dictionary).

**Why not separate KBs per data source:**
- All structured sources share the same query patterns and can be joined in Redshift (CPI + income + rent by CMA and year). Separating them would prevent cross-source queries.
- The vector KB is small enough to consolidate.

---

## Data Ingestion Pipeline

```
Statistics Canada WDS API  ──→ S3 (raw CSVs)  ──→ ETL (PySpark) ──→ Redshift ──→ Structured KB
  └─ CPI (1810000412)
  └─ Median Income (1110023901)
  └─ Food Prices (1810024501)
  └─ Gasoline Prices (1810000101)
  └─ NHPI (324befd1)

CMHC Excel downloads       ──→ S3 (raw Excel) ──→ ETL (openpyxl) ──→ Redshift ──→ Structured KB
CMHC PDF reports/notes     ──→ S3 (PDFs)                         ─────────────→ Vector KB
```

**AWS Region:** `ca-central-1` for all services (S3, Redshift Serverless, Bedrock, Bedrock Agent Runtime).

**Update schedule:**
- StatCan sources: Monthly automated pull via WDS API
- CMHC Excel: Annual batch (new edition released ~November each year)
- Vector KB: Re-sync when new CMHC reports are added to S3

---

## Strands Agent Architecture

### Tools

```python
from strands import Agent
from strands_tools import retrieve         # built-in: vector KB semantic search
import boto3

bedrock_runtime = boto3.client("bedrock-agent-runtime", region_name="ca-central-1")

def query_structured_kb(query: str) -> str:
    """Query structured (NL-to-SQL) KB for precise numeric/time-series data."""
    response = bedrock_runtime.retrieve_and_generate(
        input={"text": query},
        retrieveAndGenerateConfiguration={
            "type": "KNOWLEDGE_BASE",
            "knowledgeBaseConfiguration": {
                "knowledgeBaseId": STRUCTURED_KB_ID,
                "modelArn": "arn:aws:bedrock:ca-central-1::foundation-model/anthropic.claude-sonnet-4-5-20250929-v1:0",
                "retrievalConfiguration": {
                    "vectorSearchConfiguration": {"numberOfResults": 5}
                }
            }
        }
    )
    return response["output"]["text"]

agent = Agent(
    tools=[retrieve, query_structured_kb],
    system_prompt="""You are ForeSite Analytics, an HR recruitment assistant for Canadian workforce planning.
    
    You have access to two data tools:
    - query_structured_kb: Use for specific numeric questions about CPI, rent, income, or housing prices by city and time period.
    - retrieve: Use for contextual questions about methodology, definitions, or narrative analysis.
    
    All data covers major Canadian CMAs: Toronto, Vancouver, Calgary, Montreal, Ottawa, Edmonton, and others.
    When answering, cite the data source (Statistics Canada, CMHC) and time period."""
)
```

### Routing Logic

The agent autonomously routes queries:
- "What is the average 2-bedroom rent in Calgary in 2024?" → `query_structured_kb` (exact numeric lookup)
- "How is the vacancy rate calculated?" → `retrieve` (definition/methodology from vector KB)
- "Which city has the best affordability for a mid-level data analyst salary?" → both tools (income + rent + CPI comparison)

---

## Open Items / Blockers

| Item | Status | Action Required |
|------|--------|-----------------|
| CMHC Excel parsing | Needs ETL work | Identify specific sheet names and column layouts per edition |
| Redshift Serverless setup | TBD | Provision namespace + workgroup in `ca-central-1` |
| Bedrock model | Confirmed | `anthropic.claude-sonnet-4-5-20250929-v1:0` — used in the `retrieve_and_generate` boto3 call; Strands `Agent()` uses its built-in default, no model spec needed |
| StatCan WDS API rate limits | Low risk | 10 req/sec per IP; full-table ZIP downloads bypass this entirely |
