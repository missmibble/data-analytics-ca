"""
ForeSite Analytics — Lambda handler.

Invocation modes:
  - API Gateway POST /chat   → dispatches async worker, returns {job_id}
  - API Gateway GET /result  → polls S3 for job result
  - API Gateway GET /health  → health check
  - Worker mode              → runs agent, writes result to S3
"""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

_S3_BUCKET = os.environ.get("S3_BUCKET_RAW", "foresite-raw-ca-383429078788")
_LAMBDA_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "foresite-agent-api")
_AWS_REGION = os.environ.get("AWS_REGION", "ca-central-1")

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="ForeSite Analytics API", version="0.1.0")

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*")
_origins = [o.strip() for o in _raw_origins.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
    max_age=300,
)


class ChatRequest(BaseModel):
    message: str


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/chat")
def chat(body: ChatRequest) -> dict:
    """Dispatch an async worker and return a job_id immediately."""
    if not body.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")

    job_id = str(uuid.uuid4())
    log.info("Dispatching job %s: %s", job_id, body.message[:120])

    boto3.client("lambda", region_name=_AWS_REGION).invoke(
        FunctionName=_LAMBDA_NAME,
        InvocationType="Event",
        Payload=json.dumps({
            "__mode": "worker",
            "job_id": job_id,
            "message": body.message,
        }).encode(),
    )
    return {"job_id": job_id}


@app.get("/result/{job_id}")
def get_result(job_id: str) -> dict:
    """Poll S3 for job result. Returns {status: pending} if not ready."""
    s3 = boto3.client("s3", region_name=_AWS_REGION)
    try:
        obj = s3.get_object(Bucket=_S3_BUCKET, Key=f"results/{job_id}.json")
        return json.loads(obj["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {"status": "pending"}
        log.exception("S3 error fetching result %s", job_id)
        raise HTTPException(status_code=500, detail=str(e))


_mangum_handler = Mangum(app, lifespan="off")


# ---------------------------------------------------------------------------
# Worker — runs inside an async Lambda invocation, writes result to S3
# ---------------------------------------------------------------------------

def _run_worker(job_id: str, message: str) -> None:
    s3 = boto3.client("s3", region_name=_AWS_REGION)
    key = f"results/{job_id}.json"
    log.info("Worker starting job %s", job_id)
    try:
        from src.agent import get_agent
        result = get_agent()(message)
        body = json.dumps({
            "status": "complete",
            "response": str(result),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as exc:
        log.exception("Worker job %s failed", job_id)
        body = json.dumps({
            "status": "error",
            "detail": str(exc),
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })
    s3.put_object(Bucket=_S3_BUCKET, Key=key, Body=body.encode(), ContentType="application/json")
    log.info("Worker job %s written to s3://%s/%s", job_id, _S3_BUCKET, key)


# ---------------------------------------------------------------------------
# Unified Lambda entry point
# ---------------------------------------------------------------------------

def handler(event, arg2, arg3=None):
    # Worker mode: async self-invocation
    if isinstance(event, dict) and event.get("__mode") == "worker":
        _run_worker(event["job_id"], event["message"])
        return

    # Streaming Function URL mode (unused but kept for future use)
    if arg3 is not None:
        response_stream, context = arg2, arg3
        response_stream.set_headers({"Content-Type": "application/json"})
        response_stream.write(b'{"error":"use async polling endpoint"}')
        response_stream.close()
        return

    # API Gateway mode
    return _mangum_handler(event, arg2)


# ---------------------------------------------------------------------------
# Local dev server
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)
