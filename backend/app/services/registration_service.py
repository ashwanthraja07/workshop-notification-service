"""
Registration service.

Design decisions:
  • The registration and the outbox row are inserted in ONE transaction.
    If either insert fails, both are rolled back — the transactional outbox
    guarantee.  There is no window where a registration exists without a
    pending notification, or a notification exists without a registration.
  • We take FOR SHARE on the workshop row before inserting.  FOR SHARE
    blocks a concurrent cancellation (which needs FOR UPDATE) but allows
    other concurrent registrations (which also take FOR SHARE) to proceed.
    This means:
      – Register vs register: no conflict, both succeed or fail on the UNIQUE
        constraint independently.
      – Register vs cancel: the first one to acquire its lock wins.  If
        register wins, cancellation waits; if cancel wins, register sees
        status='cancelled' and aborts.
  • We do NOT call the notification service here.  The API returns as soon as
    the DB transaction commits; the dispatcher delivers asynchronously.  This
    keeps the registration endpoint fast and decoupled from notifier health.
  • Duplicate detection: we check explicitly before inserting to return a
    clean 409 rather than propagating a raw asyncpg.UniqueViolationError
    to the caller.  The DB constraint is still the authoritative guard.
"""

from __future__ import annotations

import json
import logging
import uuid

import asyncpg

from app.models.schemas import RegistrationCreate, RegistrationResponse

logger = logging.getLogger(__name__)


async def register_member(
    pool: asyncpg.Pool,
    workshop_id: uuid.UUID,
    data: RegistrationCreate,
) -> RegistrationResponse:
    """
    Register a member for a workshop, atomically inserting an outbox row.

    Raises:
        WorkshopNotFoundError       — workshop_id does not exist.
        WorkshopCancelledError      — workshop is cancelled.
        DuplicateRegistrationError  — member already registered.
    """
    # Import here to avoid circular imports between services
    from app.services.workshop_service import WorkshopNotFoundError

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Lock the workshop row FOR SHARE.
            # • Allows concurrent registrations (they also take FOR SHARE).
            # • Blocks a concurrent cancellation (needs FOR UPDATE).
            workshop = await conn.fetchrow(
                "SELECT id, status, title FROM workshops WHERE id = $1 FOR SHARE",
                workshop_id,
            )

            if workshop is None:
                raise WorkshopNotFoundError(workshop_id)

            if workshop["status"] == "cancelled":
                raise WorkshopCancelledError(workshop_id)

            # Check for existing registration before attempting insert so we
            # can raise a domain-specific error rather than catching the raw
            # UniqueViolation from asyncpg.
            existing = await conn.fetchval(
                "SELECT id FROM registrations WHERE workshop_id = $1 AND member_id = $2",
                workshop_id,
                data.member_id,
            )
            if existing is not None:
                raise DuplicateRegistrationError(workshop_id, data.member_id)

            # Insert the registration
            reg = await conn.fetchrow(
                """
                INSERT INTO registrations (workshop_id, member_id)
                VALUES ($1, $2)
                RETURNING id, workshop_id, member_id, created_at
                """,
                workshop_id,
                data.member_id,
            )

            # Insert the outbox row in the SAME transaction.
            # If this insert fails (e.g. DB constraint), the whole transaction
            # rolls back — no orphaned registration without a notification.
            payload = json.dumps({
                "workshopId": str(workshop_id),
                "workshopTitle": workshop["title"],
                "memberId": data.member_id,
                "type": "confirmation",
                "message": f'You are registered for "{workshop["title"]}".',
            })
            await conn.execute(
                """
                INSERT INTO outbox (type, workshop_id, member_id, payload)
                VALUES ('confirmation', $1, $2, $3::jsonb)
                """,
                workshop_id,
                data.member_id,
                payload,
            )

            logger.info(
                "Registered member %s for workshop %s", data.member_id, workshop_id
            )
            return RegistrationResponse.model_validate(dict(reg))


# ---------------------------------------------------------------------------
# Domain errors
# ---------------------------------------------------------------------------

class WorkshopCancelledError(Exception):
    def __init__(self, workshop_id: uuid.UUID) -> None:
        super().__init__(f"Workshop {workshop_id} is cancelled")
        self.workshop_id = workshop_id


class DuplicateRegistrationError(Exception):
    def __init__(self, workshop_id: uuid.UUID, member_id: str) -> None:
        super().__init__(
            f"Member {member_id!r} is already registered for workshop {workshop_id}"
        )
        self.workshop_id = workshop_id
        self.member_id = member_id
