"""
Tests for the FastAPI HTTP layer (routes, status codes, error mapping).

Design note:
  We use httpx.AsyncClient with ASGITransport so we can use the async
  FastAPI app directly without Uvicorn. The lifespan is bypassed by
  manually injecting pool/dispatcher into app.state before the test.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
import pytest_asyncio
import httpx
from httpx import AsyncClient, ASGITransport

from app.main import create_app

FUTURE_ISO = "2030-06-15T10:00:00+00:00"


@pytest_asyncio.fixture
async def app_client(db_pool):
    """AsyncClient connected to the app with a real DB pool, mocked dispatcher."""
    application = create_app()

    # Inject pool directly so lifespan's pool creation is skipped
    application.state.pool = db_pool

    mock_dispatcher = MagicMock()
    mock_dispatcher.start = AsyncMock()
    mock_dispatcher.stop  = AsyncMock()
    application.state.dispatcher = mock_dispatcher

    # Use ASGITransport — runs the app in-process, no actual HTTP socket needed
    transport = ASGITransport(app=application)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


# ---------------------------------------------------------------------------
# Workshop endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_workshop_201(app_client):
    resp = await app_client.post("/workshops", json={"title": "HTTP Test WS", "starts_at": FUTURE_ISO})
    assert resp.status_code == 201
    data = resp.json()
    assert data["title"] == "HTTP Test WS"
    assert data["status"] == "active"


@pytest.mark.asyncio
async def test_get_workshop_200(app_client):
    resp = await app_client.post("/workshops", json={"title": "Get WS", "starts_at": FUTURE_ISO})
    ws_id = resp.json()["id"]
    resp2 = await app_client.get(f"/workshops/{ws_id}")
    assert resp2.status_code == 200
    assert resp2.json()["id"] == ws_id


@pytest.mark.asyncio
async def test_get_workshop_404(app_client):
    resp = await app_client.get(f"/workshops/{uuid.uuid4()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_cancel_workshop_200(app_client):
    resp = await app_client.post("/workshops", json={"title": "Cancel WS", "starts_at": FUTURE_ISO})
    ws_id = resp.json()["id"]
    resp2 = await app_client.post(f"/workshops/{ws_id}/cancel")
    assert resp2.status_code == 200
    assert resp2.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_nonexistent_workshop_404(app_client):
    resp = await app_client.post(f"/workshops/{uuid.uuid4()}/cancel")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Registration endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_201(app_client):
    resp = await app_client.post("/workshops", json={"title": "Reg WS", "starts_at": FUTURE_ISO})
    ws_id = resp.json()["id"]
    resp2 = await app_client.post(
        f"/workshops/{ws_id}/register", json={"member_id": "http_alice@x.com"}
    )
    assert resp2.status_code == 201


@pytest.mark.asyncio
async def test_register_duplicate_409(app_client):
    resp = await app_client.post("/workshops", json={"title": "Dup WS", "starts_at": FUTURE_ISO})
    ws_id = resp.json()["id"]
    await app_client.post(f"/workshops/{ws_id}/register", json={"member_id": "http_bob@x.com"})
    resp2 = await app_client.post(
        f"/workshops/{ws_id}/register", json={"member_id": "http_bob@x.com"}
    )
    assert resp2.status_code == 409


@pytest.mark.asyncio
async def test_register_cancelled_409(app_client):
    resp = await app_client.post("/workshops", json={"title": "Canc Reg WS", "starts_at": FUTURE_ISO})
    ws_id = resp.json()["id"]
    await app_client.post(f"/workshops/{ws_id}/cancel")
    resp2 = await app_client.post(
        f"/workshops/{ws_id}/register", json={"member_id": "http_carol@x.com"}
    )
    assert resp2.status_code == 409


@pytest.mark.asyncio
async def test_register_unknown_workshop_404(app_client):
    resp = await app_client.post(
        f"/workshops/{uuid.uuid4()}/register", json={"member_id": "x@x.com"}
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Admin endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_admin_dead_empty(app_client):
    resp = await app_client.get("/admin/outbox/dead")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
