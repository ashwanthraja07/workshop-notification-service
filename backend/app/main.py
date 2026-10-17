"""
FastAPI application factory.

Design decisions:
  • We use the lifespan context manager (the modern FastAPI / Starlette pattern)
    instead of on_event decorators, which are deprecated.  This ensures startup
    and shutdown resources are always paired correctly.
  • The asyncpg pool and the Dispatcher are stored on app.state so they are
    accessible anywhere via request.app.state — no global variables.
  • The dispatcher is started after the pool is created and the DB schema is
    initialised.  On shutdown, we stop the dispatcher before closing the pool
    so in-flight deliveries can still write their final status.
  • Structured logging is configured at startup with a level that can be
    overridden via the LOG_LEVEL env var.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from app.api import admin, workshops
from app.config import settings
from app.db.engine import create_pool, init_db
from app.services.dispatcher import Dispatcher


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Create pool → init DB → start dispatcher on startup; reverse on shutdown."""
    logger.info("Starting up Workshop Registration Service…")

    pool = await create_pool()
    await init_db(pool)
    logger.info("Database pool created and schema initialised")

    dispatcher = Dispatcher(pool)
    await dispatcher.start()

    # Store on app.state so dependencies can access them
    app.state.pool = pool
    app.state.dispatcher = dispatcher

    logger.info("Application ready")

    yield  # ← server is running here

    logger.info("Shutting down…")
    await dispatcher.stop()
    await pool.close()
    logger.info("Shutdown complete")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    app = FastAPI(
        title="Workshop Registration & Notifications",
        description=(
            "Transactional outbox pattern demo: workshops, registrations, "
            "and at-least-once delivery via an async outbox dispatcher."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    app.include_router(workshops.router)
    app.include_router(admin.router)

    @app.get("/health", tags=["health"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    async def root():
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/docs")

    return app


app = create_app()
