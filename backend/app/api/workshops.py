"""
Workshop and Registration API routes.

Route map:
  POST   /workshops                  — create a workshop
  GET    /workshops/{id}             — fetch a workshop
  POST   /workshops/{id}/cancel      — cancel a workshop
  POST   /workshops/{id}/register    — register a member

Design decisions:
  • We use FastAPI's Depends() to inject the asyncpg pool from app.state,
    keeping route handlers thin and testable.
  • Domain errors raised by services are caught here and converted to
    appropriate HTTP responses.  This keeps the service layer free of HTTP
    concerns.
  • 409 Conflict is returned for duplicate registrations and for registering
    on a cancelled workshop (semantically "the resource state conflicts with
    the request").
"""

from __future__ import annotations

import uuid

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.models.schemas import (
    RegistrationCreate,
    RegistrationResponse,
    WorkshopCreate,
    WorkshopResponse,
)
from app.services import registration_service, workshop_service
from app.services.registration_service import (
    DuplicateRegistrationError,
    WorkshopCancelledError,
)
from app.services.workshop_service import WorkshopNotFoundError

router = APIRouter(prefix="/workshops", tags=["workshops"])


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------

def get_pool(request: Request) -> asyncpg.Pool:
    """Extract the asyncpg pool stored on app state by the lifespan hook."""
    return request.app.state.pool


# ---------------------------------------------------------------------------
# Workshop endpoints
# ---------------------------------------------------------------------------

@router.post("", response_model=WorkshopResponse, status_code=status.HTTP_201_CREATED)
async def create_workshop(
    body: WorkshopCreate,
    pool: asyncpg.Pool = Depends(get_pool),
) -> WorkshopResponse:
    return await workshop_service.create_workshop(pool, body)


@router.get("/{workshop_id}", response_model=WorkshopResponse)
async def get_workshop(
    workshop_id: uuid.UUID,
    pool: asyncpg.Pool = Depends(get_pool),
) -> WorkshopResponse:
    result = await workshop_service.get_workshop(pool, workshop_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Workshop not found")
    return result


@router.post("/{workshop_id}/cancel", response_model=WorkshopResponse)
async def cancel_workshop(
    workshop_id: uuid.UUID,
    pool: asyncpg.Pool = Depends(get_pool),
) -> WorkshopResponse:
    try:
        return await workshop_service.cancel_workshop(pool, workshop_id)
    except WorkshopNotFoundError:
        raise HTTPException(status_code=404, detail="Workshop not found")


# ---------------------------------------------------------------------------
# Registration endpoint
# ---------------------------------------------------------------------------

@router.post(
    "/{workshop_id}/register",
    response_model=RegistrationResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register_member(
    workshop_id: uuid.UUID,
    body: RegistrationCreate,
    pool: asyncpg.Pool = Depends(get_pool),
) -> RegistrationResponse:
    try:
        return await registration_service.register_member(pool, workshop_id, body)
    except WorkshopNotFoundError:
        raise HTTPException(status_code=404, detail="Workshop not found")
    except WorkshopCancelledError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Workshop is cancelled; registration not accepted",
        )
    except DuplicateRegistrationError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Member is already registered for this workshop",
        )
