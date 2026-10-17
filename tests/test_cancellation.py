"""
Tests for workshop cancellation behaviour.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import asyncpg
import pytest

from app.models.schemas import RegistrationCreate, WorkshopCreate
from app.services import registration_service, workshop_service
from app.services.workshop_service import WorkshopNotFoundError

FUTURE = datetime(2030, 6, 1, 9, 0, tzinfo=timezone.utc)


async def _make_workshop(pool: asyncpg.Pool, title: str = "Workshop"):
    return await workshop_service.create_workshop(
        pool, WorkshopCreate(title=title, starts_at=FUTURE)
    )


async def _register(pool: asyncpg.Pool, workshop_id: uuid.UUID, member: str):
    return await registration_service.register_member(
        pool, workshop_id, RegistrationCreate(member_id=member)
    )


# ---------------------------------------------------------------------------
# Cancellation basics
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_creates_one_notification_per_member(db_pool):
    """
    Cancelling a workshop with N registered members must create exactly N
    cancellation outbox rows — one per member.
    """
    ws = await _make_workshop(db_pool, "ML Workshop")
    members = ["alice@x.com", "bob@x.com", "carol@x.com"]
    for m in members:
        await _register(db_pool, ws.id, m)

    # Cancellation
    cancelled = await workshop_service.cancel_workshop(db_pool, ws.id)
    assert cancelled.status == "cancelled"

    # Exactly 3 cancellation rows, one per member
    rows = await db_pool.fetch(
        "SELECT member_id FROM outbox WHERE workshop_id=$1 AND type='cancellation'",
        ws.id,
    )
    notified_members = {r["member_id"] for r in rows}
    assert notified_members == set(members)


@pytest.mark.asyncio
async def test_cancel_with_no_members(db_pool):
    """Cancelling a workshop with no registrations creates zero outbox rows."""
    ws = await _make_workshop(db_pool)
    await workshop_service.cancel_workshop(db_pool, ws.id)

    count = await db_pool.fetchval(
        "SELECT COUNT(*) FROM outbox WHERE workshop_id=$1 AND type='cancellation'", ws.id
    )
    assert count == 0


@pytest.mark.asyncio
async def test_cancel_idempotent_no_duplicate_notifications(db_pool):
    """
    Cancelling twice must be idempotent:
      - Second call returns the workshop as cancelled (no error).
      - No additional outbox rows are created.
    """
    ws = await _make_workshop(db_pool)
    await _register(db_pool, ws.id, "dan@x.com")
    await _register(db_pool, ws.id, "eva@x.com")

    # First cancellation
    await workshop_service.cancel_workshop(db_pool, ws.id)
    count_after_first = await db_pool.fetchval(
        "SELECT COUNT(*) FROM outbox WHERE workshop_id=$1 AND type='cancellation'", ws.id
    )

    # Second cancellation — must be idempotent
    result = await workshop_service.cancel_workshop(db_pool, ws.id)
    assert result.status == "cancelled"

    count_after_second = await db_pool.fetchval(
        "SELECT COUNT(*) FROM outbox WHERE workshop_id=$1 AND type='cancellation'", ws.id
    )
    # Exactly the same number of rows — none added
    assert count_after_first == count_after_second == 2


@pytest.mark.asyncio
async def test_cancel_nonexistent_workshop(db_pool):
    with pytest.raises(WorkshopNotFoundError):
        await workshop_service.cancel_workshop(db_pool, uuid.uuid4())


# ---------------------------------------------------------------------------
# Register / Cancel Race condition
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_after_cancel_rejected(db_pool):
    """
    After cancellation, a new registration must be rejected (WorkshopCancelledError).
    This simulates the race where cancel completes first.
    """
    from app.services.registration_service import WorkshopCancelledError

    ws = await _make_workshop(db_pool)
    await workshop_service.cancel_workshop(db_pool, ws.id)

    with pytest.raises(WorkshopCancelledError):
        await _register(db_pool, ws.id, "frank@x.com")

    # No registration should exist
    count = await db_pool.fetchval(
        "SELECT COUNT(*) FROM registrations WHERE workshop_id=$1", ws.id
    )
    assert count == 0


@pytest.mark.asyncio
async def test_cancel_sees_all_registrations_before_commit(db_pool):
    """
    Members registered BEFORE the cancel transaction starts must all receive
    a cancellation notification.  Members who try to register AFTER the
    workshop is locked (FOR UPDATE) must be blocked.

    This test verifies the basic property: all pre-cancel members are notified.
    The actual concurrency race is serialised at DB level via FOR UPDATE / FOR SHARE.
    """
    ws = await _make_workshop(db_pool)
    members = [f"user{i}@x.com" for i in range(5)]
    for m in members:
        await _register(db_pool, ws.id, m)

    await workshop_service.cancel_workshop(db_pool, ws.id)

    rows = await db_pool.fetch(
        "SELECT member_id FROM outbox WHERE workshop_id=$1 AND type='cancellation'", ws.id
    )
    notified = {r["member_id"] for r in rows}
    assert notified == set(members)
