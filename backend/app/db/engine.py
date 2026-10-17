"""
asyncpg connection pool factory.

Design decisions:
  • We use asyncpg directly (not SQLAlchemy) for maximum control over
    locking primitives (SELECT ... FOR UPDATE SKIP LOCKED, FOR SHARE)
    which are central to the outbox dispatcher and registration race logic.
    SQLAlchemy's ORM abstraction makes those harder to express clearly.
  • The pool is stored on the FastAPI app state so it lives for the entire
    process lifetime and is shared across requests — no per-request overhead.
  • init_db() runs the DDL script idempotently (IF NOT EXISTS) on startup
    so the schema is always in sync without a migration tool dependency.
    In production you'd use Alembic; here simplicity trumps.
"""

from __future__ import annotations

import json
from pathlib import Path

import asyncpg

from app.config import settings


_INIT_SQL_PATH = Path(__file__).parent / "init.sql"


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Register jsonb codec so asyncpg seamlessly handles JSON payloads as dicts."""
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def create_pool() -> asyncpg.Pool:
    """Create and return an asyncpg connection pool."""
    pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
        init=_init_connection,
    )
    return pool  # type: ignore[return-value]


async def init_db(pool: asyncpg.Pool) -> None:
    """Run the DDL init script to ensure tables exist."""
    ddl = _INIT_SQL_PATH.read_text()
    async with pool.acquire() as conn:
        await conn.execute(ddl)
