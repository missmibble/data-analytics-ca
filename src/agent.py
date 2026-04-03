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
from strands import Agent
from strands_tools import retrieve

from src.config import AWS_REGION, STRUCTURED_KB_ID, VECTOR_KB_ID, SYSTEM_PROMPT

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

_bedrock_runtime = boto3.client("bedrock-agent-runtime", region_name=AWS_REGION)

STRUCTURED_MODEL_ARN = (
    "arn:aws:bedrock:ca-central-1::foundation-model/"
    "anthropic.claude-sonnet-4-5-20250929-v1:0"
)


def query_structured_kb(query: str) -> str:
    """
    Query the structured (NL-to-SQL) knowledge base for precise numeric and
    time-series data — rents, CPI values, income figures, vacancy rates, NHPI.
    """
    kb_id = STRUCTURED_KB_ID or os.environ["STRUCTURED_KB_ID"]
    response = _bedrock_runtime.retrieve_and_generate(
        input={"text": query},
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
    os.environ.setdefault("STRANDS_KNOWLEDGE_BASE_ID", VECTOR_KB_ID or os.environ.get("VECTOR_KB_ID", ""))
    return Agent(
        tools=[retrieve, query_structured_kb],
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
