"""
Admin endpoints for outbox dead-letter management.

Endpoints:
  GET  /admin/outbox/dead    — list dead outbox rows
  POST /admin/outbox/{id}/retry — reset a dead row back to pending

Design decisions:
  • Dead-letter visibility is essential for ops: without it you'd have to
    query the DB directly to know what notifications failed permanently.
  • Retry resets to pending with attempts=0 and next_attempt_at=now(), so
    the dispatcher picks it up on the next poll cycle.
  • We intentionally do NOT auto-retry dead rows: the operator must
    explicitly acknowledge and retry them, because they may represent a
    misconfiguration that needs investigation first.
"""

from __future__ import annotations

import uuid

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request

from app.models.schemas import OutboxRow, RetryResponse

router = APIRouter(prefix="/admin", tags=["admin"])


def get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


@router.get("/outbox/dead", response_model=list[OutboxRow])
async def list_dead_outbox(pool: asyncpg.Pool = Depends(get_pool)) -> list[OutboxRow]:
    """Return all outbox rows with status='dead'."""
    rows = await pool.fetch(
        """
        SELECT id, type, workshop_id, member_id, payload, status, attempts,
               max_attempts, next_attempt_at, locked_until, last_error,
               created_at, sent_at
        FROM outbox
        WHERE status = 'dead'
        ORDER BY created_at DESC
        """
    )
    return [OutboxRow.model_validate(dict(r)) for r in rows]


@router.post("/outbox/{outbox_id}/retry", response_model=RetryResponse)
async def retry_dead_outbox(
    outbox_id: uuid.UUID,
    pool: asyncpg.Pool = Depends(get_pool),
) -> RetryResponse:
    """Reset a dead outbox row to pending so the dispatcher retries it."""
    result = await pool.fetchval(
        """
        UPDATE outbox
        SET status='pending', attempts=0, next_attempt_at=now(),
            locked_until=NULL, last_error=NULL
        WHERE id=$1 AND status='dead'
        RETURNING id
        """,
        outbox_id,
    )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Dead outbox row not found (it may already be pending/sent)",
        )
    return RetryResponse(message="Row reset to pending", outbox_id=outbox_id)


@router.get("/outbox/all", response_model=list[OutboxRow])
async def list_all_outbox(pool: asyncpg.Pool = Depends(get_pool)) -> list[OutboxRow]:
    """Return all outbox rows (for debugging / demo purposes)."""
    rows = await pool.fetch(
        """
        SELECT id, type, workshop_id, member_id, payload, status, attempts,
               max_attempts, next_attempt_at, locked_until, last_error,
               created_at, sent_at
        FROM outbox
        ORDER BY created_at DESC
        LIMIT 200
        """
    )
    return [OutboxRow.model_validate(dict(r)) for r in rows]
