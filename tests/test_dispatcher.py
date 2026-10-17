"""
Tests for the outbox dispatcher:
  - Retries with exponential backoff
  - Marks sent only on 2xx
  - One member's failure does not block others
  - Lease expiry allows reclaim of stuck 'sending' rows
  - Restart recovery: pending rows are delivered after simulated restart
  - Duplicate delivery handled via idempotency key
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest
from httpx import AsyncClient, Response, TimeoutException

from app.services.dispatcher import Dispatcher, _claim_batch, _mark_failed, _mark_sent


# ---------------------------------------------------------------------------
# Helper: insert a raw outbox row for testing
# ---------------------------------------------------------------------------

async def _insert_outbox_row(
    pool: asyncpg.Pool,
    *,
    status: str = "pending",
    attempts: int = 0,
    max_attempts: int = 5,
    next_attempt_at: datetime | None = None,
    locked_until: datetime | None = None,
    member_id: str = "test@example.com",
    workshop_id: uuid.UUID | None = None,
) -> uuid.UUID:
    if workshop_id is None:
        workshop_id = await pool.fetchval(
            "INSERT INTO workshops (title, starts_at) VALUES ('Test', now() + interval '1 day') RETURNING id"
        )
    if next_attempt_at is None:
        next_attempt_at = datetime.now(timezone.utc)

    row_id = await pool.fetchval(
        """
        INSERT INTO outbox (type, workshop_id, member_id, payload, status,
                            attempts, max_attempts, next_attempt_at, locked_until)
        VALUES ('confirmation', $1, $2,
                '{"workshopId":"x","memberId":"x","type":"confirmation","message":"hi"}'::jsonb,
                $3, $4, $5, $6, $7)
        RETURNING id
        """,
        workshop_id, member_id, status, attempts, max_attempts, next_attempt_at, locked_until,
    )
    return row_id


# ---------------------------------------------------------------------------
# Claiming & lease tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_claim_batch_claims_pending_rows(db_pool):
    """Pending rows due now are claimed and set to 'sending'."""
    row_id = await _insert_outbox_row(db_pool)
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            rows = await _claim_batch(conn, lease_seconds=30)
    assert any(r["id"] == row_id for r in rows)

    status = await db_pool.fetchval("SELECT status FROM outbox WHERE id=$1", row_id)
    assert status == "sending"


@pytest.mark.asyncio
async def test_claim_batch_skips_future_rows(db_pool):
    """Rows with next_attempt_at in the future are not claimed."""
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await _insert_outbox_row(db_pool, next_attempt_at=future)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            rows = await _claim_batch(conn, lease_seconds=30)
    assert len(rows) == 0


@pytest.mark.asyncio
async def test_lease_expiry_reclaims_stuck_sending_row(db_pool):
    """
    A row stuck in 'sending' with an expired lease must be re-claimed.
    This simulates crash recovery: a worker died mid-delivery.
    """
    expired_lease = datetime.now(timezone.utc) - timedelta(seconds=1)
    row_id = await _insert_outbox_row(
        db_pool, status="sending", locked_until=expired_lease
    )

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            rows = await _claim_batch(conn, lease_seconds=30)

    claimed_ids = [r["id"] for r in rows]
    assert row_id in claimed_ids


@pytest.mark.asyncio
async def test_active_lease_not_reclaimed(db_pool):
    """A row with an active (future) lease must NOT be re-claimed (SKIP LOCKED emulated)."""
    active_lease = datetime.now(timezone.utc) + timedelta(seconds=60)
    await _insert_outbox_row(
        db_pool, status="sending", locked_until=active_lease
    )

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            rows = await _claim_batch(conn, lease_seconds=30)

    assert len(rows) == 0


# ---------------------------------------------------------------------------
# Mark sent / failed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_mark_sent(db_pool):
    row_id = await _insert_outbox_row(db_pool)
    async with db_pool.acquire() as conn:
        await _mark_sent(conn, row_id)
    row = await db_pool.fetchrow("SELECT status, sent_at FROM outbox WHERE id=$1", row_id)
    assert row["status"] == "sent"
    assert row["sent_at"] is not None


@pytest.mark.asyncio
async def test_mark_failed_increments_attempts_and_schedules_retry(db_pool):
    row_id = await _insert_outbox_row(db_pool, attempts=0, max_attempts=5)
    async with db_pool.acquire() as conn:
        await _mark_failed(conn, row_id, attempts=0, max_attempts=5, error="timeout")

    row = await db_pool.fetchrow(
        "SELECT status, attempts, last_error FROM outbox WHERE id=$1", row_id
    )
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert "timeout" in row["last_error"]


@pytest.mark.asyncio
async def test_mark_failed_dead_on_max_attempts(db_pool):
    """After max_attempts, the row should be marked dead."""
    row_id = await _insert_outbox_row(db_pool, attempts=4, max_attempts=5)
    async with db_pool.acquire() as conn:
        await _mark_failed(conn, row_id, attempts=4, max_attempts=5, error="final error")

    status = await db_pool.fetchval("SELECT status FROM outbox WHERE id=$1", row_id)
    assert status == "dead"


# ---------------------------------------------------------------------------
# Dispatcher integration: delivery success
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatcher_marks_sent_on_200(db_pool):
    """Dispatcher must mark row as sent when notifier returns 200."""
    row_id = await _insert_outbox_row(db_pool)

    transport = MagicMock()
    transport.handle_async_request = AsyncMock(
        return_value=Response(200, json={"status": "ok"})
    )

    dispatcher = Dispatcher(db_pool)

    async with AsyncClient(transport=transport, base_url="http://mock_notifier:8001") as client:
        await dispatcher._poll_once(client)

    status = await db_pool.fetchval("SELECT status FROM outbox WHERE id=$1", row_id)
    assert status == "sent"


@pytest.mark.asyncio
async def test_dispatcher_retries_on_500(db_pool):
    """Dispatcher must NOT mark sent on 500; must increment attempts."""
    row_id = await _insert_outbox_row(db_pool, attempts=0, max_attempts=5)

    transport = MagicMock()
    transport.handle_async_request = AsyncMock(
        return_value=Response(500, json={"detail": "error"})
    )

    dispatcher = Dispatcher(db_pool)
    async with AsyncClient(transport=transport, base_url="http://mock_notifier:8001") as client:
        await dispatcher._poll_once(client)

    row = await db_pool.fetchrow("SELECT status, attempts FROM outbox WHERE id=$1", row_id)
    assert row["status"] == "pending"   # back to pending (not sent, not dead)
    assert row["attempts"] == 1


@pytest.mark.asyncio
async def test_one_member_failure_does_not_block_others(db_pool):
    """
    When delivering multiple rows, one member's failure must not prevent
    successful delivery for other members.
    """
    # Create workshop
    ws_id = await db_pool.fetchval(
        "INSERT INTO workshops (title, starts_at) VALUES ('Multi', now() + interval '1 day') RETURNING id"
    )

    # Insert 3 rows
    good_id_1 = await _insert_outbox_row(db_pool, workshop_id=ws_id, member_id="ok1@x.com")
    bad_id    = await _insert_outbox_row(db_pool, workshop_id=ws_id, member_id="bad@x.com")
    good_id_2 = await _insert_outbox_row(db_pool, workshop_id=ws_id, member_id="ok2@x.com")

    call_count = 0

    async def side_effect(request, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        # Extract member from request body — fail for "bad@x.com"
        import json
        body = json.loads(request.content)
        if body.get("memberId") == "bad@x.com":
            return Response(500, json={"detail": "error"})
        return Response(200, json={"status": "ok"})

    transport = MagicMock()
    transport.handle_async_request = AsyncMock(side_effect=side_effect)

    dispatcher = Dispatcher(db_pool)
    async with AsyncClient(transport=transport, base_url="http://mock_notifier:8001") as client:
        await dispatcher._poll_once(client)

    statuses = await db_pool.fetch(
        "SELECT id, status FROM outbox WHERE id = ANY($1::uuid[])",
        [good_id_1, bad_id, good_id_2],
    )
    status_map = {r["id"]: r["status"] for r in statuses}

    assert status_map[good_id_1] == "sent"
    assert status_map[good_id_2] == "sent"
    assert status_map[bad_id]    == "pending"   # retrying


# ---------------------------------------------------------------------------
# Restart recovery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_restart_recovery_pending_rows_delivered(db_pool):
    """
    Simulated restart: pending rows that existed before the dispatcher
    started must be delivered.  This is the key resilience property.
    """
    row_id = await _insert_outbox_row(db_pool)

    transport = MagicMock()
    transport.handle_async_request = AsyncMock(
        return_value=Response(200, json={"status": "ok"})
    )

    # A "new" dispatcher instance (as would be created after restart)
    dispatcher = Dispatcher(db_pool)
    async with AsyncClient(transport=transport, base_url="http://mock_notifier:8001") as client:
        await dispatcher._poll_once(client)

    status = await db_pool.fetchval("SELECT status FROM outbox WHERE id=$1", row_id)
    assert status == "sent"


# ---------------------------------------------------------------------------
# Idempotency key uniqueness
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_outbox_row_id_used_as_idempotency_key(db_pool):
    """The dispatcher must send the outbox row id as the Idempotency-Key header."""
    row_id = await _insert_outbox_row(db_pool)

    received_headers: dict = {}

    async def capture_request(request, *args, **kwargs):
        received_headers.update(dict(request.headers))
        return Response(200, json={"status": "ok"})

    transport = MagicMock()
    transport.handle_async_request = AsyncMock(side_effect=capture_request)

    dispatcher = Dispatcher(db_pool)
    async with AsyncClient(transport=transport, base_url="http://mock_notifier:8001") as client:
        await dispatcher._poll_once(client)

    assert received_headers.get("idempotency-key") == str(row_id)
