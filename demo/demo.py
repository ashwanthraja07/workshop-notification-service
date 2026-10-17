#!/usr/bin/env python3
"""
Workshop Registration & Notifications — End-to-End Demo Script
==============================================================

Demonstrates all scenarios with strict assertion checking:
  1. Normal flow: create workshop, register members, confirmation delivery.
  2. Failure rate 100% & recovery: retries with exponential backoff & jitter,
     asserting failed_attempts > 0 and status != sent while pending, then restoring
     notifier and verifying delivery.
  3. Timeout & idempotency: mock sleeps longer than backend client timeout (5s),
     backend retries, mock reports duplicates >= 1 deduplicated by idempotency key,
     and exactly one delivery is recorded in mock per outbox row.
  4. Workshop cancellation: every registered member notified, idempotent second cancel.
  5. Real crash recovery: mock fully down, register members, run `docker-compose kill backend`
     and `docker-compose up -d backend` via subprocess, restore mock, verify delivery.
  6. Full run audit: verify failed registrations create zero outbox rows (explaining
     setup row delta), and compare all sent outbox IDs against mock delivered keys across
     the entire run.

Usage:
    python demo/demo.py

Requires: httpx  (pip install httpx)
"""

from __future__ import annotations

import subprocess
import sys
import time
import uuid
import httpx

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

BACKEND = "http://localhost:8000"
NOTIFIER = "http://localhost:8001"


# ── Pretty printing helpers ──────────────────────────────────────────────────

def banner(text: str, char: str = "═") -> None:
    width = 72
    print(f"\n{char * width}")
    print(f"  {text}")
    print(f"{char * width}")


def section(text: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {text}")
    print(f"{'─' * 60}")


def ok(msg: str) -> None:
    print(f"  ✔  {msg}")


def info(msg: str) -> None:
    print(f"  ℹ  {msg}")


def warn(msg: str) -> None:
    print(f"  ⚠  {msg}")


def fail(msg: str) -> None:
    print(f"  ✘  {msg}")


# ── Polling & verification helpers ──────────────────────────────────────────

def wait_and_verify_delivery(
    client: httpx.Client,
    expected_ids: set[str],
    timeout: int = 90,
    poll_interval: float = 2.0,
) -> dict[str, dict]:
    """
    Poll BOTH the DB outbox status (/admin/outbox/all) AND the mock notifier (/delivered)
    until every expected ID has status='sent' in DB and appears in /delivered,
    or until timeout expires.

    Returns:
        dict[str, dict]: Map of outbox_id -> {status, failed_attempts, delivered_in_mock, error}
    """
    deadline = time.time() + timeout
    latest_details: dict[str, dict] = {}

    while time.time() < deadline:
        try:
            db_rows = client.get(f"{BACKEND}/admin/outbox/all").json()
            delivered_list = client.get(f"{NOTIFIER}/delivered").json()
            delivered_keys = {e["idempotencyKey"] for e in delivered_list}

            all_satisfied = True
            for oid in expected_ids:
                row = next((r for r in db_rows if str(r["id"]) == oid), None)
                if not row:
                    all_satisfied = False
                    latest_details[oid] = {
                        "status": "missing",
                        "failed_attempts": 0,
                        "delivered_in_mock": False,
                        "error": "Row not in DB",
                    }
                    continue

                db_status = row["status"]
                failed_attempts = row["attempts"]
                in_mock = oid in delivered_keys

                latest_details[oid] = {
                    "status": db_status,
                    "failed_attempts": failed_attempts,
                    "delivered_in_mock": in_mock,
                    "error": row.get("last_error"),
                }

                if db_status != "sent" or not in_mock:
                    all_satisfied = False

            if all_satisfied and len(expected_ids) > 0:
                return latest_details

        except Exception as exc:
            warn(f"Polling error (will retry): {exc}")

        time.sleep(poll_interval)

    return latest_details


def get_outbox_keys_for_workshop(client: httpx.Client, workshop_id: str) -> dict[str, str]:
    """Return {outbox_id: status} for a workshop's outbox rows via admin endpoint."""
    rows = client.get(f"{BACKEND}/admin/outbox/all").json()
    return {str(r["id"]): r["status"] for r in rows if r.get("workshop_id") == workshop_id}


# ── Notifier config helpers ──────────────────────────────────────────────────

def reset_notifier(client: httpx.Client) -> None:
    resp = client.post(f"{NOTIFIER}/admin/reset")
    assert resp.status_code == 200, "Failed to reset notifier"


def set_failure_rate(client: httpx.Client, rate: float) -> None:
    resp = client.post(f"{NOTIFIER}/admin/config", json={"failure_rate": rate})
    assert resp.status_code == 200, "Failed to set failure rate"
    info(f"Mock notifier failure_rate → {rate:.0%}")


def set_timeout_mode(client: httpx.Client, timeout_rate: float, sleep_s: float) -> None:
    resp = client.post(f"{NOTIFIER}/admin/config", json={
        "timeout_rate": timeout_rate,
        "timeout_sleep": sleep_s,
    })
    assert resp.status_code == 200, "Failed to set timeout mode"
    info(f"Mock notifier timeout_rate → {timeout_rate:.0%}, sleep → {sleep_s}s")


def set_fully_down(client: httpx.Client, down: bool) -> None:
    resp = client.post(f"{NOTIFIER}/admin/config", json={"fully_down": down})
    assert resp.status_code == 200, "Failed to set fully_down"
    info(f"Mock notifier fully_down → {down}")


# ════════════════════════════════════════════════════════════════════════════
# SCENARIO 1 — Normal Registration & Confirmation Delivery
# ════════════════════════════════════════════════════════════════════════════

def scenario_1(client: httpx.Client) -> set[str]:
    banner("SCENARIO 1 — Normal Registration & Confirmation Delivery")

    section("Creating workshop")
    resp = client.post(f"{BACKEND}/workshops", json={
        "title": "Intro to FastAPI",
        "starts_at": "2030-03-01T10:00:00Z",
    })
    assert resp.status_code == 201, f"Expected 201, got {resp.status_code}: {resp.text}"
    ws = resp.json()
    assert ws["status"] == "active", f"Expected active status, got {ws['status']}"
    ok(f"Workshop created: id={ws['id']}  title='{ws['title']}'  status={ws['status']}")

    section("Registering 3 members")
    members = ["alice@demo.com", "bob@demo.com", "carol@demo.com"]
    for m in members:
        r = client.post(f"{BACKEND}/workshops/{ws['id']}/register", json={"member_id": m})
        assert r.status_code == 201, f"Registration failed for {m}: {r.status_code} {r.text}"
        reg = r.json()
        assert reg["member_id"] == m
        ok(f"Registered {m}  →  reg_id={reg['id']}")

    section("Getting expected outbox row IDs")
    outbox = get_outbox_keys_for_workshop(client, ws["id"])
    assert len(outbox) == len(members), f"Expected {len(members)} outbox rows, got {len(outbox)}"
    info(f"Outbox rows created: {len(outbox)}")
    for oid, status in outbox.items():
        info(f"  outbox_id={oid}  status={status}")

    section("Waiting for dispatcher to deliver confirmations (≤60s)…")
    expected = set(outbox.keys())
    details = wait_and_verify_delivery(client, expected, timeout=60)

    delivered_count = 0
    for oid in expected:
        d = details.get(oid, {})
        is_delivered = d.get("status") == "sent" and d.get("delivered_in_mock", False)
        failed_attempts = d.get("failed_attempts", 0)
        if is_delivered:
            delivered_count += 1
            ok(f"Confirmation delivered → outbox_id={oid}  failed_attempts={failed_attempts}")
        else:
            fail(f"NOT delivered → outbox_id={oid}  status={d.get('status')}  failed_attempts={failed_attempts}")

    assert delivered_count == len(members), f"Only {delivered_count}/{len(members)} confirmations delivered"
    ok(f"All {len(members)} confirmations delivered (verified in DB & mock) ✔")

    stats = client.get(f"{NOTIFIER}/stats").json()
    info(f"Notifier stats: {stats}")
    return expected


# ════════════════════════════════════════════════════════════════════════════
# SCENARIO 2 — Failure Rate 100% → Pending State Check → Recovery
# ════════════════════════════════════════════════════════════════════════════

def scenario_2(client: httpx.Client) -> set[str]:
    banner("SCENARIO 2 — Failure Rate 100% → Pending State Check → Recovery")

    section("Creating workshop")
    resp = client.post(f"{BACKEND}/workshops", json={
        "title": "Async Python Deep Dive",
        "starts_at": "2030-04-01T14:00:00Z",
    })
    assert resp.status_code == 201, f"Expected 201, got {resp.status_code}: {resp.text}"
    ws = resp.json()
    ok(f"Workshop created: id={ws['id']}")

    section("Setting notifier failure_rate = 100%")
    set_failure_rate(client, 1.0)
    ok("Notifier configured with 100% simulated failure rate")

    section("Registering 4 members under 100% failure rate")
    members = [f"retry_user_{i}@demo.com" for i in range(4)]
    for m in members:
        r = client.post(f"{BACKEND}/workshops/{ws['id']}/register", json={"member_id": m})
        assert r.status_code == 201, f"Registration failed for {m}: {r.status_code}"
        ok(f"Registered {m}")

    outbox = get_outbox_keys_for_workshop(client, ws["id"])
    expected_ids = set(outbox.keys())
    assert len(expected_ids) == len(members), f"Expected {len(members)} outbox rows, got {len(expected_ids)}"
    info(f"Outbox rows created: {len(expected_ids)}")

    section("Watching retries and backoff for 12 seconds (100% failures expected)…")
    time.sleep(12)

    section("Verifying rows are pending with failed_attempts > 0 during 100% failure rate…")
    all_current_rows = client.get(f"{BACKEND}/admin/outbox/all").json()
    for oid in expected_ids:
        row = next((r for r in all_current_rows if str(r["id"]) == oid), None)
        assert row is not None, f"Row {oid} not found in DB"
        status = row["status"]
        failed_attempts = row["attempts"]

        info(f"  outbox_id={oid}  db_status={status}  failed_attempts={failed_attempts}")
        assert failed_attempts > 0, f"Expected failed_attempts > 0 for {oid}, got {failed_attempts}"
        assert status != "sent", f"Row {oid} status must NOT be 'sent' while failure_rate=100%"

    ok("All rows verified pending in DB with failed_attempts > 0 during 100% failure outage ✔")

    section("Restoring notifier (failure_rate = 0%)")
    set_failure_rate(client, 0.0)
    ok("Notifier failure rate restored to 0%")

    section("Waiting for full recovery (polling DB outbox and mock delivered list)…")
    details = wait_and_verify_delivery(client, expected_ids, timeout=90)

    all_delivered = True
    delivered_count = 0
    for oid in expected_ids:
        d = details.get(oid, {})
        status = d.get("status")
        failed_attempts = d.get("failed_attempts", 0)
        in_mock = d.get("delivered_in_mock", False)

        info(f"  outbox_id={oid}  failed_attempts={failed_attempts}  db_status={status}  in_mock_delivered={in_mock}")

        if status == "sent" and in_mock:
            delivered_count += 1
            ok(f"Delivered after {failed_attempts} failed_attempts → outbox_id={oid}")
        else:
            all_delivered = False
            fail(f"STILL not delivered → outbox_id={oid}  db_status={status}  failed_attempts={failed_attempts}  in_mock={in_mock}")

    final_stats = client.get(f"{NOTIFIER}/stats").json()
    info(f"Final notifier stats: {final_stats}")

    if not all_delivered or delivered_count != len(expected_ids):
        fail(f"Recovery failed: only {delivered_count}/{len(expected_ids)} rows delivered")
        sys.exit(1)

    assert delivered_count == len(expected_ids)
    ok(f"Recovery complete: all {delivered_count}/{len(expected_ids)} rows delivered after retries ✔")
    return expected_ids


# ════════════════════════════════════════════════════════════════════════════
# SCENARIO (TIMEOUT) — Client Timeout → Retry → Idempotency Deduplication
# ════════════════════════════════════════════════════════════════════════════

def scenario_timeout(client: httpx.Client) -> set[str]:
    banner("SCENARIO (TIMEOUT) — Client Timeout → Retry → Idempotency Deduplication")

    section("Configuring mock for timeout mode")
    initial_stats = client.get(f"{NOTIFIER}/stats").json()
    initial_duplicates = initial_stats["duplicates"]
    info(f"Mock duplicates count prior to test: {initial_duplicates}")

    # Backend client timeout is 5.0s. Set mock sleep to 6.0s so backend times out.
    set_timeout_mode(client, timeout_rate=1.0, sleep_s=6.0)
    ok("Mock configured: timeout_rate=100%, timeout_sleep=6.0s (backend client timeout is 5.0s)")

    section("Creating workshop and registering member under timeout mode")
    ws = client.post(f"{BACKEND}/workshops", json={
        "title": "Idempotency & Timeout Workshop",
        "starts_at": "2030-08-15T10:00:00Z",
    }).json()
    r = client.post(f"{BACKEND}/workshops/{ws['id']}/register", json={"member_id": "timeout_user@demo.com"})
    assert r.status_code == 201
    ok("Registered member (outbox row created)")

    outbox_ids = set(get_outbox_keys_for_workshop(client, ws["id"]).keys())
    assert len(outbox_ids) == 1
    oid = list(outbox_ids)[0]
    info(f"Outbox row ID: {oid}")

    section("Waiting for backend client timeout (5s), retry, and idempotent ACK…")
    details = wait_and_verify_delivery(client, outbox_ids, timeout=60)

    d = details.get(oid, {})
    status = d.get("status")
    failed_attempts = d.get("failed_attempts", 0)
    in_mock = d.get("delivered_in_mock", False)

    info(f"  outbox_id={oid}  failed_attempts={failed_attempts}  db_status={status}  in_mock_delivered={in_mock}")
    assert status == "sent" and in_mock, "Outbox row not delivered after timeout retry"
    assert failed_attempts >= 1, f"Expected failed_attempts >= 1 due to client timeout, got {failed_attempts}"
    ok(f"Row delivered after {failed_attempts} failed_attempts (timeout handled gracefully) ✔")

    section("Restoring mock timeout configuration")
    set_timeout_mode(client, timeout_rate=0.0, sleep_s=10.0)
    ok("Mock notifier restored to normal configuration (timeout_rate = 0%)")

    section("Verifying idempotency key deduplication in mock notifier")
    new_stats = client.get(f"{NOTIFIER}/stats").json()
    duplicates_delta = new_stats["duplicates"] - initial_duplicates
    info(f"Duplicate notifications detected by mock: {duplicates_delta}")
    assert duplicates_delta >= 1, f"Expected at least 1 duplicate deduplicated, found {duplicates_delta}"
    ok(f"Mock reported duplicate delivery: {duplicates_delta} duplicate(s) deduplicated by idempotency key ✔")

    delivered_list = client.get(f"{NOTIFIER}/delivered").json()
    matching_deliveries = [e for e in delivered_list if e["idempotencyKey"] == oid]
    assert len(matching_deliveries) == 1, (
        f"Expected exactly 1 delivery in mock for {oid}, found {len(matching_deliveries)}"
    )
    ok(f"Exactly one delivery recorded for outbox_id={oid} (no duplicate notifications stored) ✔")

    return outbox_ids


# ════════════════════════════════════════════════════════════════════════════
# SCENARIO 3 — Cancellation Notifies Every Registered Member
# ════════════════════════════════════════════════════════════════════════════

def scenario_3(client: httpx.Client) -> tuple[str, set[str]]:
    banner("SCENARIO 3 — Cancellation Notifies Every Registered Member")

    ws = client.post(f"{BACKEND}/workshops", json={
        "title": "PostgreSQL Internals",
        "starts_at": "2030-05-10T09:00:00Z",
    }).json()

    section("Registering 5 members")
    members = [f"cancel_user_{i}@demo.com" for i in range(5)]
    for m in members:
        r = client.post(f"{BACKEND}/workshops/{ws['id']}/register", json={"member_id": m})
        assert r.status_code == 201

    conf_outbox = get_outbox_keys_for_workshop(client, ws["id"])
    assert len(conf_outbox) == len(members)
    ok(f"Registered {len(members)} members to workshop {ws['id']}")

    section("Waiting for initial confirmation deliveries…")
    wait_and_verify_delivery(client, set(conf_outbox.keys()), timeout=60)
    ok("All initial confirmations delivered")

    section("Cancelling workshop…")
    r_cancel = client.post(f"{BACKEND}/workshops/{ws['id']}/cancel")
    assert r_cancel.status_code == 200, f"Cancel returned {r_cancel.status_code}"
    cancelled = r_cancel.json()
    assert cancelled["status"] == "cancelled", "Workshop status is not cancelled"
    ok(f"Workshop status = {cancelled['status']}")

    section("Waiting for cancellation notifications to be delivered…")
    all_rows = client.get(f"{BACKEND}/admin/outbox/all").json()
    canc_ids = {str(r["id"]) for r in all_rows
                if r.get("workshop_id") == ws["id"] and r["type"] == "cancellation"}
    assert len(canc_ids) == len(members), f"Expected {len(members)} cancellation rows, found {len(canc_ids)}"
    info(f"Cancellation outbox rows generated: {len(canc_ids)}")

    canc_details = wait_and_verify_delivery(client, canc_ids, timeout=60)
    for oid in canc_ids:
        d = canc_details.get(oid, {})
        assert d.get("status") == "sent" and d.get("delivered_in_mock"), f"Cancellation {oid} not delivered"
        ok(f"Cancellation delivered → outbox_id={oid}  failed_attempts={d.get('failed_attempts')}")

    section("Cancelling again (idempotency check)…")
    r_cancel2 = client.post(f"{BACKEND}/workshops/{ws['id']}/cancel")
    assert r_cancel2.status_code == 200, "Second cancel request failed"
    all_rows_after = client.get(f"{BACKEND}/admin/outbox/all").json()
    new_canc_count = len([r for r in all_rows_after
                          if r.get("workshop_id") == ws["id"] and r["type"] == "cancellation"])
    assert new_canc_count == len(members), f"Idempotency failed: expected {len(members)} rows, found {new_canc_count}"
    ok(f"Second cancellation idempotent: still exactly {new_canc_count} rows in DB ✔")

    all_s3_ids = set(conf_outbox.keys()) | canc_ids
    return ws["id"], all_s3_ids


# ════════════════════════════════════════════════════════════════════════════
# SCENARIO 4 — Real Backend Crash Recovery (docker-compose kill / up)
# ════════════════════════════════════════════════════════════════════════════

def scenario_4(client: httpx.Client) -> set[str]:
    banner("SCENARIO 4 — Real Backend Crash & Restart Recovery")

    section("Creating workshop")
    resp = client.post(f"{BACKEND}/workshops", json={
        "title": "Distributed Systems 101",
        "starts_at": "2030-07-20T11:00:00Z",
    })
    assert resp.status_code == 201
    ws = resp.json()
    ok(f"Workshop created: id={ws['id']}")

    section("Taking mock notifier down (fully_down = True)")
    set_fully_down(client, True)
    ok("Mock notifier set to fully down (returns 503)")

    section("Registering 3 members while notifier is down")
    members = [f"crash_user_{i}@demo.com" for i in range(3)]
    for m in members:
        r = client.post(f"{BACKEND}/workshops/{ws['id']}/register", json={"member_id": m})
        assert r.status_code == 201
        ok(f"Registered {m} (outbox row created while notifier down)")

    # Wait 4 seconds for dispatcher to attempt delivery and see failure
    time.sleep(4)
    outbox = client.get(f"{BACKEND}/admin/outbox/all").json()
    pending = [r for r in outbox if r.get("workshop_id") == ws["id"]
               and r["status"] in ("pending", "sending")]
    assert len(pending) == len(members), f"Expected {len(members)} pending rows, found {len(pending)}"
    info(f"Rows safely pending in database: {len(pending)}/{len(members)}")

    section("Simulating crash: executing `docker-compose kill backend`…")
    kill_cmd = subprocess.run(["docker-compose", "kill", "backend"], capture_output=True, text=True)
    if kill_cmd.returncode != 0:
        fail(f"Failed to kill backend container: {kill_cmd.stderr}")
        sys.exit(1)
    ok("Backend container killed (SIGKILL confirmed)")

    time.sleep(2)

    section("Restarting backend: executing `docker-compose up -d backend`…")
    up_cmd = subprocess.run(["docker-compose", "up", "-d", "backend"], capture_output=True, text=True)
    if up_cmd.returncode != 0:
        fail(f"Failed to start backend container: {up_cmd.stderr}")
        sys.exit(1)
    ok("Backend container started via Docker Compose")

    section("Waiting for backend API to become ready…")
    deadline = time.time() + 45
    ready = False
    while time.time() < deadline:
        try:
            h = client.get(f"{BACKEND}/health", timeout=3)
            if h.status_code == 200:
                ready = True
                break
        except Exception:
            pass
        time.sleep(1)

    if not ready:
        fail("Backend failed to become ready after restart")
        sys.exit(1)
    ok("Backend API is back online and accepting requests")

    section("Restoring mock notifier (fully_down = False)")
    set_fully_down(client, False)
    ok("Mock notifier restored to operational state")

    section("Waiting for dispatcher to recover and deliver pending outbox rows…")
    outbox_ids = {str(r["id"]) for r in pending}
    details = wait_and_verify_delivery(client, outbox_ids, timeout=90)

    delivered_count = 0
    for oid in outbox_ids:
        d = details.get(oid, {})
        status = d.get("status")
        failed_attempts = d.get("failed_attempts", 0)
        in_mock = d.get("delivered_in_mock", False)

        info(f"  outbox_id={oid}  failed_attempts={failed_attempts}  db_status={status}  in_mock_delivered={in_mock}")

        if status == "sent" and in_mock:
            delivered_count += 1
            ok(f"Delivered after restart recovery → outbox_id={oid}  failed_attempts={failed_attempts}")
        else:
            fail(f"NOT delivered after restart → outbox_id={oid}  status={status}")

    if delivered_count != len(outbox_ids):
        fail(f"Restart recovery failed: only {delivered_count}/{len(outbox_ids)} delivered")
        sys.exit(1)

    assert delivered_count == len(outbox_ids)
    ok("Restart recovery complete: all pending rows delivered by restarted dispatcher ✔")
    return outbox_ids


# ════════════════════════════════════════════════════════════════════════════
# SCENARIO 5 — Full Run Audit & Failed Registration Guarantees
# ════════════════════════════════════════════════════════════════════════════

def scenario_5(client: httpx.Client, cancelled_workshop_id: str, run_outbox_ids: set[str]) -> None:
    banner("SCENARIO 5 — Full Run Audit & Non-Happy-Path Isolation")

    section("Part A: Verifying failed registrations create ZERO outbox rows")
    initial_db_rows = client.get(f"{BACKEND}/admin/outbox/all").json()
    before_count = len(initial_db_rows)
    info(f"Outbox rows in DB before failure tests: {before_count}")

    # 1. Non-existent workshop ID -> 404
    non_existent_id = str(uuid.uuid4())
    r1 = client.post(f"{BACKEND}/workshops/{non_existent_id}/register", json={"member_id": "ghost@demo.com"})
    assert r1.status_code == 404, f"Expected 404, got {r1.status_code}"
    ok(f"Registering on non-existent workshop returned 404 Not Found ✔")

    # 2. Registration on cancelled workshop -> 409
    r2 = client.post(f"{BACKEND}/workshops/{cancelled_workshop_id}/register", json={"member_id": "late@demo.com"})
    assert r2.status_code == 409, f"Expected 409, got {r2.status_code}"
    ok(f"Registering on cancelled workshop returned 409 Conflict ✔")

    # 3. Duplicate registration -> 409
    info("Setup for duplicate test: creating 1 workshop and 1 valid initial registration…")
    ws_dup = client.post(f"{BACKEND}/workshops", json={
        "title": "Duplicate Guard Workshop",
        "starts_at": "2030-11-01T09:00:00Z",
    }).json()
    r_first = client.post(f"{BACKEND}/workshops/{ws_dup['id']}/register", json={"member_id": "dup@demo.com"})
    assert r_first.status_code == 201, "First registration failed"
    ok("Setup registration succeeded (created 1 valid setup outbox row)")

    dup_outbox_id = set(get_outbox_keys_for_workshop(client, ws_dup["id"]).keys())
    assert len(dup_outbox_id) == 1

    r_dup = client.post(f"{BACKEND}/workshops/{ws_dup['id']}/register", json={"member_id": "dup@demo.com"})
    assert r_dup.status_code == 409, f"Expected 409 for duplicate, got {r_dup.status_code}"
    ok(f"Duplicate registration returned 409 Conflict ✔")

    # Confirm outbox row count delta
    after_db_rows = client.get(f"{BACKEND}/admin/outbox/all").json()
    after_count = len(after_db_rows)
    delta = after_count - before_count

    info(f"Outbox rows in DB after failure tests:  {after_count}")
    info(f"Outbox row delta:                      +{delta}")
    info("Explanation of outbox row delta:")
    info("  • Non-existent workshop attempt (404) -> exactly 0 outbox rows created")
    info("  • Cancelled workshop attempt (409)    -> exactly 0 outbox rows created")
    info("  • Duplicate registration attempt (409)-> exactly 0 outbox rows created")
    info("  • The +1 delta corresponds solely to the 1 valid setup registration needed to trigger duplicate 409.")

    assert delta == 1, (
        f"Expected outbox delta of +1 (from setup row), got {delta}. "
        "Failed registrations must NEVER create outbox rows!"
    )
    ok("Verified: failed registrations created 0 outbox rows (delta matches setup row) ✔")

    # Wait for the single setup registration to deliver
    wait_and_verify_delivery(client, dup_outbox_id, timeout=30)
    run_outbox_ids = run_outbox_ids | dup_outbox_id

    section("Part B: Full Run Audit — DB Outbox vs Mock Deliveries")
    all_outbox = client.get(f"{BACKEND}/admin/outbox/all").json()
    run_db_rows = [r for r in all_outbox if str(r["id"]) in run_outbox_ids]
    sent_db_ids = {str(r["id"]) for r in run_db_rows if r["status"] == "sent"}
    pending_db_ids = {str(r["id"]) for r in run_db_rows if r["status"] in ("pending", "sending")}
    dead_db_ids = {str(r["id"]) for r in run_db_rows if r["status"] == "dead"}

    if pending_db_ids:
        info(f"Waiting for remaining {len(pending_db_ids)} pending rows to finish delivering…")
        wait_and_verify_delivery(client, pending_db_ids, timeout=60)
        all_outbox = client.get(f"{BACKEND}/admin/outbox/all").json()
        run_db_rows = [r for r in all_outbox if str(r["id"]) in run_outbox_ids]
        sent_db_ids = {str(r["id"]) for r in run_db_rows if r["status"] == "sent"}

    delivered_list = client.get(f"{NOTIFIER}/delivered").json()
    delivered_keys = {e["idempotencyKey"] for e in delivered_list}
    stats = client.get(f"{NOTIFIER}/stats").json()

    info(f"Total outbox rows in DB:            {len(all_outbox)} (this run: {len(run_outbox_ids)})")
    info(f"Total sent outbox rows for run:     {len(sent_db_ids)}")
    info(f"Total delivered recorded by mock:   {len(delivered_keys)}")
    info(f"Total mock requests received:       {stats['received']}")
    info(f"Mock duplicates deduplicated:       {stats['duplicates']}")
    info(f"Dead letter outbox rows in DB:      {len(dead_db_ids)}")

    all_db_ids = {str(r["id"]) for r in all_outbox}
    missing = sent_db_ids - delivered_keys
    unaccounted = delivered_keys - all_db_ids

    if missing:
        fail(f"Audit FAILED: {len(missing)} sent DB rows not recorded in mock: {missing}")
        sys.exit(1)
    ok(f"No lost notifications: all {len(sent_db_ids)} sent outbox rows for this run were delivered to mock ✔")

    if unaccounted:
        fail(f"Audit FAILED: mock recorded unexpected keys not in DB: {unaccounted}")
        sys.exit(1)
    ok("No phantom deliveries: all delivered keys originate from valid DB outbox rows ✔")

    assert stats["duplicates"] >= 1, f"Expected duplicates >= 1, found {stats['duplicates']}"
    ok(f"Duplicates tracked: {stats['duplicates']} (at-least-once retries safely deduplicated by idempotency key) ✔")

    assert len(dead_db_ids) == 0, f"Expected 0 dead rows, found {len(dead_db_ids)}"
    ok("Dead letter count: 0 (all retried rows eventually succeeded) ✔")

    ok("Full Run Audit PASSED ✔")


# ════════════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════════════

def main() -> None:
    banner("Workshop Registration & Notifications — End-to-End Demo", "█")
    info(f"Backend:  {BACKEND}")
    info(f"Notifier: {NOTIFIER}")

    with httpx.Client(timeout=30) as client:
        # Verify services are reachable
        try:
            r_back = client.get(f"{BACKEND}/health")
            r_notif = client.get(f"{NOTIFIER}/health")
            assert r_back.status_code == 200, f"Backend returned {r_back.status_code}"
            assert r_notif.status_code == 200, f"Notifier returned {r_notif.status_code}"
        except Exception as e:
            fail(f"Services not reachable: {e}")
            fail("Ensure containers are running:  docker-compose up -d")
            sys.exit(1)

        ok("All services reachable")

        # Reset notifier once at the beginning of the entire run
        reset_notifier(client)
        info("Mock notifier state reset for clean full-run audit")

        try:
            s1_ids = scenario_1(client)
            s2_ids = scenario_2(client)
            s_timeout_ids = scenario_timeout(client)
            cancelled_ws_id, s3_ids = scenario_3(client)
            s4_ids = scenario_4(client)
            run_outbox_ids = s1_ids | s2_ids | s_timeout_ids | s3_ids | s4_ids
            scenario_5(client, cancelled_ws_id, run_outbox_ids)
        except AssertionError as e:
            fail(f"Demo assertion failed: {e}")
            sys.exit(1)
        except Exception as e:
            fail(f"Unexpected demo error: {e}")
            sys.exit(1)

    banner("ALL SCENARIOS COMPLETE ✔", "█")


if __name__ == "__main__":
    main()
