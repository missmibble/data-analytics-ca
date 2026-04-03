"""
ForeSite Analytics — Lambda handler.

Supports two invocation modes:
  - API Gateway (regular):            handler(event, context)
  - Function URL (streaming SSE):     handler(event, response_stream, context)

Endpoints:
    POST /chat    Streams agent tokens as Server-Sent Events (Function URL)
                  or returns {"response": "..."} JSON (API Gateway fallback)
    GET  /health  {"status": "ok"}
    OPTIONS *     CORS preflight
"""

import asyncio
import json
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
# FastAPI app (used for regular / API Gateway invocations)
# ---------------------------------------------------------------------------

app = FastAPI(
    title="ForeSite Analytics API",
    description="Natural language interface to Canadian HR economic indicators",
    version="0.1.0",
)

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


class ChatResponse(BaseModel):
    response: str


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
def chat(body: ChatRequest) -> ChatResponse:
    if not body.message.strip():
        raise HTTPException(status_code=400, detail="message must not be empty")

    log.info("Chat request: %s", body.message[:120])

    from src.agent import get_agent
    agent = get_agent()

    try:
        result = agent(body.message)
        return ChatResponse(response=str(result))
    except Exception as exc:
        log.exception("Agent error: %s", exc)
        raise HTTPException(status_code=500, detail="Agent error — see logs for details")


_mangum_handler = Mangum(app, lifespan="off")


# ---------------------------------------------------------------------------
# Streaming handler (Function URL with InvokeMode=RESPONSE_STREAM)
# ---------------------------------------------------------------------------

def _cors_headers() -> dict:
    origin = _origins[0] if _origins else "*"
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    }


async def _stream_chat(event: dict, response_stream) -> None:
    """Write agent tokens as SSE chunks into the Lambda response stream."""
    response_stream.set_headers({
        **_cors_headers(),
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    })
    try:
        body = json.loads(event.get("body") or "{}")
        message = body.get("message", "").strip()
        if not message:
            response_stream.write(b'data: {"error":"No message provided"}\n\ndata: [DONE]\n\n')
            return

        log.info("Streaming chat: %s", message[:120])
        from src.agent import get_agent
        agent = get_agent()

        async for chunk in agent.stream_async(message):
            if chunk.get("data"):
                payload = json.dumps({"token": chunk["data"]})
                response_stream.write(f"data: {payload}\n\n".encode())

        response_stream.write(b"data: [DONE]\n\n")

    except Exception as exc:
        log.exception("Streaming error")
        error_payload = json.dumps({"error": str(exc)})
        response_stream.write(f"data: {error_payload}\n\ndata: [DONE]\n\n".encode())
    finally:
        response_stream.close()


async def _handle_streaming(event: dict, response_stream, context) -> None:
    path = event.get("rawPath", "/")
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET").upper()

    if method == "OPTIONS":
        response_stream.set_headers({**_cors_headers(), "Content-Type": "text/plain"})
        response_stream.write(b"")
        response_stream.close()
    elif path == "/health":
        response_stream.set_headers({**_cors_headers(), "Content-Type": "application/json"})
        response_stream.write(b'{"status":"ok"}')
        response_stream.close()
    elif path == "/chat" and method == "POST":
        await _stream_chat(event, response_stream)
    else:
        response_stream.set_headers({**_cors_headers(), "Content-Type": "application/json"})
        response_stream.write(b'{"error":"not found"}')
        response_stream.close()


# ---------------------------------------------------------------------------
# Unified Lambda entry point
# ---------------------------------------------------------------------------

def handler(event, arg2, arg3=None):
    """
    Routes to streaming or regular handler based on invocation mode:
      API Gateway:       handler(event, context)               → arg3 is None
      Function URL SSE:  handler(event, response_stream, ctx)  → arg3 is not None
    """
    if arg3 is not None:
        response_stream, context = arg2, arg3
        asyncio.run(_handle_streaming(event, response_stream, context))
    else:
        return _mangum_handler(event, arg2)


# ---------------------------------------------------------------------------
# Local dev server
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)
