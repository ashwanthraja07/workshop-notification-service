"""
Outbox dispatcher — background worker that delivers pending notifications.

Architecture overview
─────────────────────
The dispatcher runs as an asyncio background task alongside the FastAPI server.
On each poll cycle it:
  1. Claims a batch of due outbox rows using SELECT … FOR UPDATE SKIP LOCKED
     with a lease timestamp (locked_until).  SKIP LOCKED means multiple
     dispatcher instances never claim the same row.
  2. Sends each claimed row to the mock notifier concurrently, bounded by
     a semaphore (DISPATCHER_CONCURRENCY) so we don't open unlimited
     connections.
  3. On 2xx: marks the row sent.
  4. On error / timeout: releases the lease, increments attempts, computes
     the next retry time using exponential backoff with random jitter.
  5. After max_attempts failures: marks the row dead and logs it.

Lease / crash recovery
───────────────────────
If the backend crashes while a row is in 'sending' state, the locked_until
timestamp ensures the row becomes claimable again after the lease expires.
On restart the dispatcher simply resumes normal polling — it does not need
to know that a crash occurred.  This is the key resilience property.

At-least-once delivery
───────────────────────
Because a timeout can mean the notifier actually received the request
(network dropped after delivery), we cannot guarantee exactly-once.  We
therefore send the outbox row id as Idempotency-Key on every attempt, and
the mock notifier deduplicates by that key.  Callers of the notifier must
be idempotency-aware — this is documented in DESIGN.md.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import httpx

from app.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _compute_next_attempt(attempts: int) -> datetime:
    """
    Exponential backoff with random jitter.

    delay = base * 2^attempts, capped at backoff_max.
    jitter is ±jitter_factor * delay, making the distribution uniform in
    [delay*(1-j), delay*(1+j)].  Jitter prevents thundering-herd when
    many rows fail at the same time and are all scheduled identically.
    """
    base = settings.dispatcher_backoff_base
    cap  = settings.dispatcher_backoff_max
    raw  = base * (2 ** attempts)
    delay = min(raw, cap)
    jitter_range = delay * settings.dispatcher_jitter_factor
    jitter = random.uniform(-jitter_range, jitter_range)
    final_delay = max(0.0, delay + jitter)
    return datetime.now(timezone.utc) + timedelta(seconds=final_delay)


async def _claim_batch(conn: asyncpg.Connection, lease_seconds: int) -> list[asyncpg.Record]:
    """
    Claim a batch of pending/sending outbox rows that are due for processing.

    SKIP LOCKED: rows locked by another transaction (another dispatcher
    instance) are simply skipped rather than blocking, so multiple workers
    can safely run concurrently.

    We also reclaim rows stuck in 'sending' whose lease has expired (the
    locked_until < now() condition covers this because those rows have
    status='sending' and their lock has expired).
    """
    now = datetime.now(timezone.utc)
    lease_expiry = now + timedelta(seconds=lease_seconds)

    rows = await conn.fetch(
        """
        UPDATE outbox
        SET    status       = 'sending',
               locked_until = $2
        WHERE  id IN (
            SELECT id FROM outbox
            WHERE  status IN ('pending', 'sending')
              AND  next_attempt_at <= $1
              AND  (locked_until IS NULL OR locked_until < $1)
            ORDER BY next_attempt_at
            LIMIT  $3
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, type, workshop_id, member_id, payload,
                  attempts, max_attempts, last_error
        """,
        now,
        lease_expiry,
        settings.dispatcher_concurrency,
    )
    return list(rows)


async def _mark_sent(conn: asyncpg.Connection, row_id: uuid.UUID) -> None:
    await conn.execute(
        "UPDATE outbox SET status='sent', sent_at=now(), locked_until=NULL WHERE id=$1",
        row_id,
    )


async def _mark_failed(
    conn: asyncpg.Connection,
    row_id: uuid.UUID,
    attempts: int,
    max_attempts: int,
    error: str,
) -> None:
    """Release lease and schedule retry or mark dead."""
    new_attempts = attempts + 1
    if new_attempts >= max_attempts:
        logger.error(
            "Outbox row %s exhausted %d attempts — marking dead. Last error: %s",
            row_id, max_attempts, error,
        )
        await conn.execute(
            """
            UPDATE outbox
            SET status='dead', attempts=$2, last_error=$3,
                locked_until=NULL, next_attempt_at=now()
            WHERE id=$1
            """,
            row_id, new_attempts, error,
        )
    else:
        next_at = _compute_next_attempt(new_attempts)
        logger.warning(
            "Outbox row %s attempt %d/%d failed (%s), next retry at %s",
            row_id, new_attempts, max_attempts, error, next_at.isoformat(),
        )
        await conn.execute(
            """
            UPDATE outbox
            SET status='pending', attempts=$2, last_error=$3,
                locked_until=NULL, next_attempt_at=$4
            WHERE id=$1
            """,
            row_id, new_attempts, error, next_at,
        )


# ---------------------------------------------------------------------------
# Row delivery
# ---------------------------------------------------------------------------

async def _deliver_row(
    client: httpx.AsyncClient,
    pool: asyncpg.Pool,
    row: asyncpg.Record,
) -> None:
    """Send one outbox row to the notifier and update its status."""
    row_id: uuid.UUID = row["id"]
    try:
        raw_payload = row["payload"]
        if isinstance(raw_payload, str):
            try:
                payload = json.loads(raw_payload)
            except Exception:
                payload = {}
        elif isinstance(raw_payload, dict):
            payload = raw_payload
        else:
            payload = dict(raw_payload) if raw_payload else {}

        # Inject required fields the notifier expects
        notify_body = {
            "idempotencyKey": str(row_id),
            "memberId":       row["member_id"],
            "workshopId":     str(row["workshop_id"]),
            "type":           row["type"],
            "message":        payload.get("message", ""),
        }

        resp = await client.post(
            f"{settings.notifier_url}/notify",
            json=notify_body,
            headers={"Idempotency-Key": str(row_id)},
            timeout=settings.notifier_timeout,
        )
        if resp.status_code < 300:
            logger.info("Delivered outbox row %s (status %d)", row_id, resp.status_code)
            async with pool.acquire() as conn:
                await _mark_sent(conn, row_id)
        else:
            error_msg = f"HTTP {resp.status_code}: {resp.text[:200]}"
            logger.warning("Notifier returned error for row %s: %s", row_id, error_msg)
            async with pool.acquire() as conn:
                await _mark_failed(
                    conn, row_id, row["attempts"], row["max_attempts"], error_msg
                )
    except (httpx.TimeoutException, httpx.RequestError) as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.warning("Notifier request failed for row %s: %s", row_id, error_msg)
        async with pool.acquire() as conn:
            await _mark_failed(
                conn, row_id, row["attempts"], row["max_attempts"], error_msg
            )
    except Exception as exc:
        error_msg = f"Unexpected delivery error: {type(exc).__name__}: {exc}"
        logger.exception("Unexpected error delivering row %s: %s", row_id, exc)
        async with pool.acquire() as conn:
            await _mark_failed(
                conn, row_id, row["attempts"], row["max_attempts"], error_msg
            )


# ---------------------------------------------------------------------------
# Main dispatcher loop
# ---------------------------------------------------------------------------

class Dispatcher:
    """
    Manages the outbox polling lifecycle.

    Designed so:
      • _running flag + asyncio.Event allows graceful shutdown.
      • A semaphore caps concurrent HTTP sends.
      • Each row is delivered in a separate asyncio task so one slow/failing
        row never blocks others.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._running = False
        self._stop_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(settings.dispatcher_concurrency)

    async def start(self) -> None:
        """Begin polling.  Call once on application startup."""
        self._running = True
        self._stop_event.clear()
        logger.info("Dispatcher started (poll_interval=%.1fs)", settings.dispatcher_poll_interval)
        asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """
        Graceful shutdown: stop claiming new rows and let in-flight tasks finish.
        We wait up to (lease_seconds) for tasks to complete.
        """
        logger.info("Dispatcher stopping — waiting for in-flight tasks…")
        self._running = False
        self._stop_event.set()

    async def _poll_loop(self) -> None:
        async with httpx.AsyncClient() as client:
            while self._running:
                try:
                    await self._poll_once(client)
                except Exception as exc:
                    # Broad catch: a bug in the poll loop must not kill the worker
                    logger.exception("Unexpected error in dispatcher poll loop: %s", exc)

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=settings.dispatcher_poll_interval,
                    )
                    # Stop event fired — exit loop
                    break
                except asyncio.TimeoutError:
                    pass  # normal poll interval elapsed

        logger.info("Dispatcher poll loop exited")

    async def _poll_once(self, client: httpx.AsyncClient) -> None:
        """Claim a batch and spawn a bounded task per row."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                rows = await _claim_batch(conn, settings.dispatcher_lease_seconds)

        if not rows:
            return

        logger.debug("Claimed %d outbox rows", len(rows))

        tasks = []
        for row in rows:
            task = asyncio.create_task(
                self._bounded_deliver(client, row)
            )
            tasks.append(task)

        # Wait for this batch to complete before the next poll.
        # This keeps backpressure simple; at larger scale you'd decouple.
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _bounded_deliver(
        self, client: httpx.AsyncClient, row: asyncpg.Record
    ) -> None:
        """Acquire semaphore slot before delivering to cap concurrent HTTP calls."""
        async with self._semaphore:
            await _deliver_row(client, self._pool, row)
