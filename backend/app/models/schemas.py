"""
Pydantic v2 request and response schemas.

Design decisions:
  • Separate Request / Response models so we never accidentally expose
    internal fields (e.g. status, created_at) in request bodies.
  • UUID fields use Python's uuid.UUID type; FastAPI serialises them to strings.
  • datetime fields use datetime with timezone awareness; asyncpg returns
    aware datetimes for TIMESTAMPTZ columns automatically.
  • model_config = ConfigDict(from_attributes=True) lets us call
    Model.model_validate(asyncpg.Record) which behaves like a dict.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


# =============================================================================
# Workshop
# =============================================================================

class WorkshopCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    starts_at: datetime
    capacity: Optional[int] = Field(None, gt=0)


class WorkshopResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    title: str
    description: Optional[str]
    starts_at: datetime
    capacity: Optional[int]
    status: str
    created_at: datetime


# =============================================================================
# Registration
# =============================================================================

class RegistrationCreate(BaseModel):
    member_id: str = Field(..., min_length=1, max_length=255,
                            description="Opaque member identifier (email, UUID, etc.)")


class RegistrationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    workshop_id: uuid.UUID
    member_id: str
    created_at: datetime


# =============================================================================
# Outbox (admin / dead-letter)
# =============================================================================

class OutboxRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type: str
    workshop_id: Optional[uuid.UUID]
    member_id: Optional[str]
    payload: dict[str, Any]
    status: str
    attempts: int
    max_attempts: int
    next_attempt_at: datetime
    locked_until: Optional[datetime]
    last_error: Optional[str]
    created_at: datetime
    sent_at: Optional[datetime]

    @field_validator("payload", mode="before")
    @classmethod
    def parse_payload(cls, v: Any) -> Any:
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return v
        return v


class RetryResponse(BaseModel):
    message: str
    outbox_id: uuid.UUID
