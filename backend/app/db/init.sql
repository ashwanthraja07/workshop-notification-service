-- =============================================================================
-- Workshop Registration & Notifications — Database Schema
-- =============================================================================
-- Design decisions:
--   • TIMESTAMPTZ everywhere so the DB stores UTC and clients can localise.
--   • UNIQUE (workshop_id, member_id) in registrations enforces at-DB-level
--     exactly-once semantics; the app relies on this to return 409.
--   • Outbox index on (status, next_attempt_at) is the hot read path for the
--     dispatcher's polling query.  A partial index on pending/sending keeps it
--     small as delivered rows grow.
--   • workshop_id + member_id in the outbox are nullable because future
--     notification types (e.g. organiser summaries) may not target a member.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Workshops
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workshops (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title       TEXT        NOT NULL,
    description TEXT,
    starts_at   TIMESTAMPTZ NOT NULL,
    capacity    INT,                       -- NULL means unlimited
    status      TEXT        NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active', 'cancelled')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Registrations
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS registrations (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workshop_id UUID        NOT NULL REFERENCES workshops(id),
    member_id   TEXT        NOT NULL,      -- opaque member identifier (email, UUID, etc.)
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Core business rule: one registration per member per workshop.
    -- The DB constraint is the authoritative guard; app code also checks to
    -- return a user-friendly 409 before the DB raises UniqueViolation.
    CONSTRAINT uq_workshop_member UNIQUE (workshop_id, member_id)
);

CREATE INDEX IF NOT EXISTS idx_registrations_workshop
    ON registrations (workshop_id);

-- ---------------------------------------------------------------------------
-- Outbox
-- ---------------------------------------------------------------------------
-- Implements the Transactional Outbox pattern:
--   Insert outbox rows in the SAME transaction as the business operation so
--   notifications are guaranteed to be created if and only if the operation
--   commits.  A separate dispatcher worker polls this table and delivers.
--
-- Status lifecycle:
--   pending → sending (dispatcher claims a lease) → sent | dead
--   dead rows can be retried manually via the admin endpoint.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    type             TEXT        NOT NULL,   -- 'confirmation' | 'cancellation'
    workshop_id      UUID        REFERENCES workshops(id),
    member_id        TEXT,
    payload          JSONB       NOT NULL,   -- arbitrary notification payload
    status           TEXT        NOT NULL DEFAULT 'pending'
                                 CHECK (status IN ('pending', 'sending', 'sent', 'dead')),
    attempts         INT         NOT NULL DEFAULT 0,
    max_attempts     INT         NOT NULL DEFAULT 5,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until     TIMESTAMPTZ,            -- dispatcher lease expiry
    last_error       TEXT,                   -- last failure reason for debugging
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at          TIMESTAMPTZ             -- set when status → sent
);

-- Hot path: the dispatcher polls for rows that are due and not locked.
-- Partial index excludes terminal states (sent, dead) which are never re-read.
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox (status, next_attempt_at)
    WHERE status IN ('pending', 'sending');
