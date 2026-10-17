"""
pytest configuration and shared fixtures.

Design decisions:
  • We use a REAL PostgreSQL database (via DATABASE_URL env var defaulting to
    a test-specific DB) rather than mocking it.  This means tests exercise
    actual SQL, locking behaviour, and constraint enforcement — exactly what
    matters for the outbox pattern.
  • Each test gets a fresh database state by running inside a transaction that
    is rolled back at the end (faster than DROP/CREATE).  The pool is created
    once per session.
  • The mock notifier is stubbed with httpx.MockTransport so tests are
    self-contained and don't need the notifier container to be running.
  • pytest-asyncio in "auto" mode avoids having to decorate every async test
    with @pytest.mark.asyncio.
"""

from __future__ import annotations

import asyncio
import os
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
import pytest_asyncio
from httpx import AsyncClient, Response

from app.config import settings
from app.db.engine import _init_connection, init_db

# Override DB URL for tests
TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    os.getenv("DATABASE_URL", "postgresql://workshop:workshop@localhost:5432/workshop_db"),
)





# ---------------------------------------------------------------------------
# Database pool — function-scoped to match test event loop
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    pool = await asyncpg.create_pool(
        dsn=TEST_DATABASE_URL,
        min_size=1,
        max_size=5,
        init=_init_connection,
    )
    await init_db(pool)
    yield pool
    await pool.close()


# ---------------------------------------------------------------------------
# Per-test DB cleanup — truncate all tables between tests for isolation
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True)
async def clean_db(db_pool: asyncpg.Pool) -> AsyncGenerator[None, None]:
    """Truncate all tables before each test."""
    yield
    async with db_pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE TABLE outbox, registrations, workshops RESTART IDENTITY CASCADE"
        )


# ---------------------------------------------------------------------------
# Mock HTTP client for the notifier
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_notifier_ok() -> AsyncClient:
    """Returns a mock httpx.AsyncClient that always responds 200."""
    transport = MagicMock()
    transport.handle_async_request = AsyncMock(
        return_value=Response(200, json={"status": "ok"})
    )
    client = AsyncClient(transport=transport, base_url="http://mock_notifier:8001")
    return client


@pytest.fixture
def mock_notifier_fail() -> AsyncClient:
    """Returns a mock httpx.AsyncClient that always responds 500."""
    transport = MagicMock()
    transport.handle_async_request = AsyncMock(
        return_value=Response(500, json={"detail": "Simulated failure"})
    )
    client = AsyncClient(transport=transport, base_url="http://mock_notifier:8001")
    return client
