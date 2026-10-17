"""
Tests for workshop creation, retrieval, and duplicate registration.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import asyncpg
import pytest

from app.models.schemas import RegistrationCreate, WorkshopCreate
from app.services import registration_service, workshop_service
from app.services.registration_service import DuplicateRegistrationError, WorkshopCancelledError
from app.services.workshop_service import WorkshopNotFoundError


FUTURE = datetime(2030, 1, 1, 10, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _make_workshop(pool: asyncpg.Pool, title: str = "Test Workshop"):
    return await workshop_service.create_workshop(
        pool,
        WorkshopCreate(title=title, starts_at=FUTURE),
    )


async def _register(pool: asyncpg.Pool, workshop_id: uuid.UUID, member: str):
    return await registration_service.register_member(
        pool, workshop_id, RegistrationCreate(member_id=member)
    )


# ---------------------------------------------------------------------------
# Workshop CRUD
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_and_get_workshop(db_pool):
    ws = await _make_workshop(db_pool, "Python Workshop")
    assert ws.title == "Python Workshop"
    assert ws.status == "active"

    fetched = await workshop_service.get_workshop(db_pool, ws.id)
    assert fetched is not None
    assert fetched.id == ws.id


@pytest.mark.asyncio
async def test_get_nonexistent_workshop(db_pool):
    result = await workshop_service.get_workshop(db_pool, uuid.uuid4())
    assert result is None


# ---------------------------------------------------------------------------
# Registration basics
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_registration_success(db_pool):
    ws = await _make_workshop(db_pool)
    reg = await _register(db_pool, ws.id, "alice@example.com")
    assert reg.member_id == "alice@example.com"
    assert reg.workshop_id == ws.id

    # Verify exactly one outbox row was created
    count = await db_pool.fetchval(
        "SELECT COUNT(*) FROM outbox WHERE workshop_id=$1 AND type='confirmation'",
        ws.id,
    )
    assert count == 1


@pytest.mark.asyncio
async def test_duplicate_registration_returns_no_extra_outbox_row(db_pool):
    """
    Core requirement: duplicate registration → 409, and NO extra outbox row.
    The outbox count must remain 1 (from the first registration).
    """
    ws = await _make_workshop(db_pool)
    await _register(db_pool, ws.id, "bob@example.com")

    # Second registration must raise
    with pytest.raises(DuplicateRegistrationError):
        await _register(db_pool, ws.id, "bob@example.com")

    # Still exactly one outbox row — not two
    count = await db_pool.fetchval(
        "SELECT COUNT(*) FROM outbox WHERE workshop_id=$1 AND member_id=$2",
        ws.id, "bob@example.com",
    )
    assert count == 1


@pytest.mark.asyncio
async def test_register_on_unknown_workshop(db_pool):
    """Registration on a nonexistent workshop → WorkshopNotFoundError, no outbox row."""
    fake_id = uuid.uuid4()
    with pytest.raises(WorkshopNotFoundError):
        await _register(db_pool, fake_id, "charlie@example.com")

    count = await db_pool.fetchval("SELECT COUNT(*) FROM outbox")
    assert count == 0


@pytest.mark.asyncio
async def test_register_on_cancelled_workshop(db_pool):
    """Registration on cancelled workshop → WorkshopCancelledError, no outbox row."""
    ws = await _make_workshop(db_pool)
    await workshop_service.cancel_workshop(db_pool, ws.id)

    with pytest.raises(WorkshopCancelledError):
        await _register(db_pool, ws.id, "dave@example.com")

    # Only cancellation outbox rows may exist (zero, since no members registered)
    conf_count = await db_pool.fetchval(
        "SELECT COUNT(*) FROM outbox WHERE type='confirmation'"
    )
    assert conf_count == 0


# ---------------------------------------------------------------------------
# Atomicity: registration + outbox are in one transaction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_registration_outbox_atomicity(db_pool):
    """
    Simulate a scenario where we can verify that if the transaction rolls back,
    NEITHER the registration NOR the outbox row persists.

    We test this indirectly: by attempting to register on a cancelled workshop
    (which raises before committing), we verify that no registration record and
    no outbox row were written.
    """
    ws = await _make_workshop(db_pool)
    await workshop_service.cancel_workshop(db_pool, ws.id)

    # This will raise WorkshopCancelledError inside the transaction
    with pytest.raises(WorkshopCancelledError):
        await registration_service.register_member(
            db_pool, ws.id, RegistrationCreate(member_id="eve@example.com")
        )

    reg_count = await db_pool.fetchval(
        "SELECT COUNT(*) FROM registrations WHERE workshop_id=$1", ws.id
    )
    outbox_count = await db_pool.fetchval(
        "SELECT COUNT(*) FROM outbox WHERE type='confirmation' AND workshop_id=$1", ws.id
    )
    assert reg_count == 0
    assert outbox_count == 0
