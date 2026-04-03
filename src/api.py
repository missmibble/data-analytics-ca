"""
ForeSite Analytics — FastAPI REST API with CORS, deployed on AWS Lambda via Mangum.

Endpoints:
    POST /chat    {"message": "..."} → {"response": "..."}
    GET  /health  → {"status": "ok"}

CORS:
    Controlled by the ALLOWED_ORIGINS environment variable.
    Set to * during development; update to the external site URL when known.
    Multiple origins: comma-separated (e.g. https://site-a.com,https://site-b.com).
    No code changes required — update the Lambda env var and API Gateway CORS config.
"""

import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from pydantic import BaseModel

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="ForeSite Analytics API",
    description="Natural language interface to Canadian HR economic indicators",
    version="0.1.0",
)

# CORS — origins controlled entirely by environment variable
_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*")
_origins = [o.strip() for o in _raw_origins.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=False,       # set True only if cookies/auth headers needed
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
    max_age=300,
)

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    message: str

class ChatResponse(BaseModel):
    response: str

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
def chat(body: ChatRequest) -> ChatResponse:
    if not body.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")

    log.info("Chat request: %s", body.message[:120])

    # Import here so Lambda cold-start doesn't fail if KB env vars are missing
    from src.agent import get_agent
    agent = get_agent()

    try:
        result = agent(body.message)
        return ChatResponse(response=str(result))
    except Exception as exc:
        log.exception("Agent error: %s", exc)
        raise HTTPException(status_code=500, detail="Agent error — see logs for details")


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------

handler = Mangum(app, lifespan="off")


# ---------------------------------------------------------------------------
# Local dev server
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)
