"""
Workshop service: create, fetch, and cancel workshops.

Design decisions:
  • Cancellation uses SELECT ... FOR UPDATE on the workshop row so that a
    concurrent registration (which takes FOR SHARE on the same row) must
    wait.  This means the cancel/register race is fully serialised at DB
    level — no application-level locking needed.
  • Cancelling an already-cancelled workshop is idempotent: we check the
    current status first and skip the update + outbox inserts if already
    cancelled.  This avoids duplicate notification rows.
  • The bulk outbox INSERT uses a single INSERT ... SELECT from the
    registrations table so the number of rows is exactly right, even if a
    new registration snuck in before the FOR UPDATE lock was taken.  The FOR
    UPDATE on the workshop row blocks new FOR SHARE (registration) attempts
    during the whole transaction so no registration can be missed.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

import asyncpg

from app.models.schemas import WorkshopCreate, WorkshopResponse

logger = logging.getLogger(__name__)


async def create_workshop(
    pool: asyncpg.Pool, data: WorkshopCreate
) -> WorkshopResponse:
    """Insert a new workshop and return it."""
    row = await pool.fetchrow(
        """
        INSERT INTO workshops (title, description, starts_at, capacity)
        VALUES ($1, $2, $3, $4)
        RETURNING id, title, description, starts_at, capacity, status, created_at
        """,
        data.title,
        data.description,
        data.starts_at,
        data.capacity,
    )
    return WorkshopResponse.model_validate(dict(row))


async def get_workshop(
    pool: asyncpg.Pool, workshop_id: uuid.UUID
) -> WorkshopResponse | None:
    """Fetch a single workshop by ID, returning None if not found."""
    row = await pool.fetchrow(
        """
        SELECT id, title, description, starts_at, capacity, status, created_at
        FROM workshops WHERE id = $1
        """,
        workshop_id,
    )
    if row is None:
        return None
    return WorkshopResponse.model_validate(dict(row))


async def cancel_workshop(
    pool: asyncpg.Pool, workshop_id: uuid.UUID
) -> WorkshopResponse:
    """
    Cancel a workshop and enqueue one outbox notification per registered member.

    Atomicity guarantee:
      All of the following happen in ONE transaction:
        1. Lock the workshop row with FOR UPDATE (blocks concurrent registrations
           that hold FOR SHARE).
        2. Check idempotency — if already cancelled, return without changes.
        3. Set status = 'cancelled'.
        4. Insert one outbox row per registered member via INSERT ... SELECT.

    The FOR UPDATE lock in step 1 means no new registrations can be inserted
    between the lock and the INSERT ... SELECT, so no member can be missed.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Step 1: lock the workshop row
            row = await conn.fetchrow(
                "SELECT id, title, description, starts_at, capacity, status, created_at "
                "FROM workshops WHERE id = $1 FOR UPDATE",
                workshop_id,
            )
            if row is None:
                raise WorkshopNotFoundError(workshop_id)

            # Step 2: idempotency — already cancelled, nothing to do
            if row["status"] == "cancelled":
                logger.info("Workshop %s already cancelled, skipping", workshop_id)
                return WorkshopResponse.model_validate(dict(row))

            # Step 3: mark cancelled
            updated = await conn.fetchrow(
                "UPDATE workshops SET status = 'cancelled' WHERE id = $1 "
                "RETURNING id, title, description, starts_at, capacity, status, created_at",
                workshop_id,
            )

            # Step 4: bulk insert one outbox row per registered member.
            # The payload is built inline so each row carries all the info the
            # notifier needs without extra DB lookups at delivery time.
            await conn.execute(
                """
                INSERT INTO outbox (type, workshop_id, member_id, payload)
                SELECT
                    'cancellation',
                    $1::uuid,
                    member_id,
                    jsonb_build_object(
                        'workshopId',    $1::uuid,
                        'workshopTitle', $2::text,
                        'memberId',      member_id,
                        'type',          'cancellation',
                        'message',       'Workshop "' || $2::text || '" has been cancelled.'
                    )
                FROM registrations
                WHERE workshop_id = $1::uuid
                """,
                workshop_id,
                row["title"],
            )

            affected = await conn.fetchval(
                "SELECT COUNT(*) FROM registrations WHERE workshop_id = $1",
                workshop_id,
            )
            logger.info(
                "Cancelled workshop %s, queued %d cancellation notifications",
                workshop_id, affected,
            )
            return WorkshopResponse.model_validate(dict(updated))


# ---------------------------------------------------------------------------
# Domain errors — raised by services, caught in API layer
# ---------------------------------------------------------------------------

class WorkshopNotFoundError(Exception):
    def __init__(self, workshop_id: uuid.UUID) -> None:
        super().__init__(f"Workshop {workshop_id} not found")
        self.workshop_id = workshop_id


class WorkshopAlreadyCancelledError(Exception):
    def __init__(self, workshop_id: uuid.UUID) -> None:
        super().__init__(f"Workshop {workshop_id} is already cancelled")
        self.workshop_id = workshop_id
