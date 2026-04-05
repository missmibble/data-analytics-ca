"""
ForeSite Analytics — Strands agent core.

Kept infrastructure-agnostic: can be imported by api.py (Lambda) or run directly.

Direct usage:
    python -m src.agent
"""

import logging
import os

import boto3
from dotenv import load_dotenv
from strands import Agent, tool
from strands.models import BedrockModel
from src.config import AWS_REGION, STRUCTURED_KB_ID, SYSTEM_PROMPT

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

_bedrock_runtime = boto3.client("bedrock-agent-runtime", region_name=AWS_REGION)

STRUCTURED_MODEL_ARN = (
    "arn:aws:bedrock:ca-central-1:383429078788:inference-profile/"
    "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
)


_INDICATOR_CONTEXT = (
    "Use these exact indicator_name values from dim_indicator: "
    "'CPI - All-items', 'CPI - Energy', 'CPI - Food', 'CPI - Shelter', 'CPI - Transportation', "
    "'Gasoline price (per litre)', "
    "'Food price - Bread (per 675g)', 'Food price - Eggs (per dozen)', 'Food price - Milk (per 2L)', "
    "'Avg rent - Bachelor', 'Avg rent - 1 Bedroom', 'Avg rent - 2 Bedroom', "
    "'Avg rent - 3 Bedroom +', 'Avg rent - Total', "
    "'Vacancy rate - Bachelor', 'Vacancy rate - 1 Bedroom', 'Vacancy rate - 2 Bedroom', "
    "'Vacancy rate - 3 Bedroom +', 'Vacancy rate - Total', "
    "'NHPI - Total', 'NHPI - House only', 'NHPI - Land only'. "
    "Data storage rules: "
    "CPI and gasoline are monthly — always filter dim_date with is_annual=FALSE and month between 1 and 12. "
    "Rent and vacancy data are annual October surveys — filter with month=10 and is_annual=FALSE. "
    "Income data is in fact_annual_income, not fact_monthly. "
)


@tool
def query_structured_kb(query: str) -> str:
    """
    Query the structured (NL-to-SQL) knowledge base for precise numeric and
    time-series data — rents, CPI values, income figures, vacancy rates, NHPI.
    """
    kb_id = STRUCTURED_KB_ID or os.environ["STRUCTURED_KB_ID"]
    enriched_query = _INDICATOR_CONTEXT + "Query: " + query
    response = _bedrock_runtime.retrieve_and_generate(
        input={"text": enriched_query},
        retrieveAndGenerateConfiguration={
            "type": "KNOWLEDGE_BASE",
            "knowledgeBaseConfiguration": {
                "knowledgeBaseId": kb_id,
                "modelArn": STRUCTURED_MODEL_ARN,
            },
        },
    )
    return response["output"]["text"]


def get_agent() -> Agent:
    """Return a configured ForeSite agent instance."""
    model = BedrockModel(
        model_id="us.anthropic.claude-sonnet-4-5-20250929-v1:0",
        region_name=AWS_REGION,
    )
    return Agent(
        model=model,
        tools=[query_structured_kb],
        system_prompt=SYSTEM_PROMPT,
    )


if __name__ == "__main__":
    print("ForeSite Analytics — type your question or 'exit' to quit.\n")
    agent = get_agent()
    while True:
        try:
            user_input = input("ForeSite > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break
        if user_input.lower() in ("exit", "quit"):
            break
        if not user_input:
            continue
        response = agent(user_input)
        print(f"\n{response}\n")
