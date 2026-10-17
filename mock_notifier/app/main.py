"""
Mock HTTP Notification Service.

Purpose
───────
Acts as a stand-in for a real email/SMS gateway.  Lets us demonstrate
at-least-once delivery, idempotency, retries, and recovery without
any real external dependencies.

Design decisions:
  • State is kept in-memory in module-level dicts/counters so the service
    is completely stateless from a persistence standpoint — reset by
    restarting the container.  For the demo this is intentional.
  • Configurable failure modes via env vars AND a live admin endpoint so
    the demo script can change behaviour without restarting the container.
  • Idempotency is implemented by storing the set of already-delivered
    idempotency keys.  On a duplicate key: we increment the duplicate
    counter (so the demo can prove the backend retried) but do NOT count
    it as a fresh delivery.
  • The "fully down" mode just returns 503 immediately, so the backend
    dispatcher can show recovery when it is toggled back off.
  • asyncio.sleep in the timeout path suspends the request coroutine but
    does not block other requests — Uvicorn/asyncio handles concurrency.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

# Maps idempotency key → request log entry
_request_log: dict[str, dict[str, Any]] = {}

# Counters
_stats: dict[str, int] = {
    "received":   0,
    "delivered":  0,
    "failed":     0,
    "timed_out":  0,
    "duplicates": 0,
}

# Behaviour config (mutable at runtime via /admin/config)
_config: dict[str, Any] = {
    "failure_rate":  float(os.getenv("FAILURE_RATE",  "0.0")),   # 0.0–1.0
    "timeout_rate":  float(os.getenv("TIMEOUT_RATE",  "0.0")),   # 0.0–1.0
    "fully_down":    os.getenv("FULLY_DOWN", "false").lower() == "true",
    "timeout_sleep": float(os.getenv("TIMEOUT_SLEEP", "10.0")),  # seconds to sleep on timeout
}

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class NotifyRequest(BaseModel):
    idempotencyKey: str
    memberId: str
    workshopId: str
    type: str
    message: str


class ConfigUpdate(BaseModel):
    failure_rate: float | None = None
    timeout_rate: float | None = None
    fully_down:   bool  | None = None
    timeout_sleep: float | None = None


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Mock Notification Service", version="1.0.0")


@app.post("/notify", status_code=200)
async def notify(request: NotifyRequest) -> dict[str, str]:
    """
    Accept a notification delivery attempt.

    Behaviour:
      • fully_down=True  → always 503
      • timeout_rate     → sleep long enough for the client to time out, then 200
      • failure_rate     → 500 with error message
      • otherwise        → 200 delivered (idempotent on key)
    """
    key = request.idempotencyKey
    _stats["received"] += 1
    logger.info("Received notify key=%s member=%s type=%s", key, request.memberId, request.type)

    # --- Fully down ---
    if _config["fully_down"]:
        _stats["failed"] += 1
        raise HTTPException(status_code=503, detail="Service fully down")

    # --- Idempotency check ---
    if key in _request_log and _request_log[key]["outcome"] == "delivered":
        _stats["duplicates"] += 1
        logger.info("Duplicate delivery for key=%s (ignored)", key)
        _log_request(key, request, "duplicate")
        return {"status": "ok", "note": "duplicate ignored"}

    # --- Timeout simulation ---
    if random.random() < _config["timeout_rate"]:
        _stats["timed_out"] += 1
        sleep_s = _config["timeout_sleep"]
        logger.info("Simulating timeout for key=%s (sleeping %ss)", key, sleep_s)
        # Record request as delivered, then sleep longer than backend timeout
        _stats["delivered"] += 1
        _log_request(key, request, "delivered")
        await asyncio.sleep(sleep_s)
        return {"status": "ok", "note": "delivered after timeout"}

    # --- Failure simulation ---
    if random.random() < _config["failure_rate"]:
        _stats["failed"] += 1
        _log_request(key, request, "failed")
        raise HTTPException(status_code=500, detail="Simulated notifier failure")

    # --- Successful delivery ---
    _stats["delivered"] += 1
    _log_request(key, request, "delivered")
    return {"status": "ok"}


def _log_request(key: str, request: NotifyRequest, outcome: str) -> None:
    # Record request; if a previous attempt failed, upgrade outcome on delivery
    if key not in _request_log:
        _request_log[key] = {
            "idempotencyKey": key,
            "memberId":       request.memberId,
            "workshopId":     request.workshopId,
            "type":           request.type,
            "outcome":        outcome,
            "timestamp":      datetime.now(timezone.utc).isoformat(),
            "duplicate_count": 0,
        }
    elif outcome in ("delivered", "timeout_delivered"):
        _request_log[key]["outcome"] = "delivered"
        _request_log[key]["timestamp"] = datetime.now(timezone.utc).isoformat()
    elif outcome == "duplicate":
        _request_log[key]["duplicate_count"] = (
            _request_log[key].get("duplicate_count", 0) + 1
        )


# ---------------------------------------------------------------------------
# Stats & inspection endpoints
# ---------------------------------------------------------------------------

@app.get("/stats")
async def get_stats() -> dict[str, Any]:
    """Totals: received, delivered, failed, timed_out, duplicates."""
    return dict(_stats)


@app.get("/delivered")
async def get_delivered() -> list[dict[str, Any]]:
    """Return all entries that were successfully delivered (first delivery)."""
    return [v for v in _request_log.values() if v["outcome"] == "delivered"]


@app.get("/log")
async def get_full_log() -> list[dict[str, Any]]:
    """Full request log (all outcomes)."""
    return list(_request_log.values())


# ---------------------------------------------------------------------------
# Admin / config endpoints
# ---------------------------------------------------------------------------

@app.get("/admin/config")
async def get_config() -> dict[str, Any]:
    return dict(_config)


@app.post("/admin/config")
async def update_config(update: ConfigUpdate) -> dict[str, Any]:
    """
    Adjust failure/timeout rates at runtime without restarting the container.
    Used by the demo script to demonstrate recovery.
    """
    if update.failure_rate is not None:
        _config["failure_rate"] = max(0.0, min(1.0, update.failure_rate))
    if update.timeout_rate is not None:
        _config["timeout_rate"] = max(0.0, min(1.0, update.timeout_rate))
    if update.fully_down is not None:
        _config["fully_down"] = update.fully_down
    if update.timeout_sleep is not None:
        _config["timeout_sleep"] = max(0.0, update.timeout_sleep)
    logger.info("Config updated: %s", _config)
    return dict(_config)


@app.post("/admin/reset")
async def reset_state() -> dict[str, str]:
    """Reset all stats and request log (for test isolation)."""
    _request_log.clear()
    for k in _stats:
        _stats[k] = 0
    _config["failure_rate"] = 0.0
    _config["timeout_rate"] = 0.0
    _config["fully_down"] = False
    return {"status": "reset"}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
