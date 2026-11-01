"""PostgreSQL: the full event history, idempotent inserts, and the SQL behind the API."""
from __future__ import annotations

from typing import Iterable, Optional

import psycopg

from .models import TelemetryEvent

SCHEMA = """
CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id     TEXT PRIMARY KEY,                -- set by the robot; a re-sent event has the same id
    robot_id     TEXT NOT NULL,
    seq          BIGINT NOT NULL CHECK (seq >= 0),
    ts           TIMESTAMPTZ NOT NULL,            -- robot clock
    x            DOUBLE PRECISION NOT NULL,
    y            DOUBLE PRECISION NOT NULL,
    battery      REAL NOT NULL CHECK (battery BETWEEN 0 AND 100),
    state        TEXT NOT NULL CHECK (state IN ('IDLE', 'MOVING', 'CHARGING', 'FAULT')),
    fault_code   TEXT,
    accepted_at  TIMESTAMPTZ NOT NULL,            -- ingest API accepted it
    stored_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS telemetry_events_robot_ts ON telemetry_events (robot_id, ts DESC);
"""

_COLS = "(event_id, robot_id, seq, ts, x, y, battery, state, fault_code, accepted_at)"
_ROW = "(%s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, to_timestamp(%s))"


def connect(dsn: str, schema: Optional[str] = None, connect_timeout: int = 2) -> psycopg.Connection:
    """Autocommit connection; transactions are opened explicitly with `conn.transaction()`."""
    conn = psycopg.connect(dsn, autocommit=True, connect_timeout=connect_timeout)
    if schema:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        conn.execute(f'SET search_path TO "{schema}"')
    return conn


def create_schema(conn: psycopg.Connection, reset: bool = False) -> None:
    if reset:
        conn.execute("DROP TABLE IF EXISTS telemetry_events")
    conn.execute(SCHEMA)


def _row(event: TelemetryEvent, accepted_at: float) -> tuple:
    return (event.event_id, event.robot_id, event.seq, event.ts, event.x, event.y, event.battery,
            event.state.value, event.fault_code, accepted_at)


def insert_events(conn: psycopg.Connection, items: list[tuple[TelemetryEvent, float]]) -> set[str]:
    """Inserts a batch in one transaction. An event already stored (same event_id) is skipped by
    ON CONFLICT DO NOTHING, which makes redelivered and re-sent messages harmless.
    Returns the ids that were newly inserted."""
    if not items:
        return set()
    sql = (f"INSERT INTO telemetry_events {_COLS} VALUES " + ", ".join([_ROW] * len(items))
           + " ON CONFLICT (event_id) DO NOTHING RETURNING event_id")
    params = [v for ev, acc in items for v in _row(ev, acc)]
    with conn.transaction():
        cur = conn.execute(sql, params)
        return {r[0] for r in cur.fetchall()}


def robot_history(conn: psycopg.Connection, robot_id: str, limit: int = 50) -> list[dict]:
    cur = conn.execute(
        """SELECT seq, extract(epoch FROM ts), x, y, battery, state, fault_code
             FROM telemetry_events WHERE robot_id = %s ORDER BY ts DESC LIMIT %s""",
        (robot_id, limit))
    return [dict(seq=r[0], ts=float(r[1]), x=r[2], y=r[3], battery=r[4], state=r[5], fault_code=r[6])
            for r in cur.fetchall()]


def fleet_summary(conn: psycopg.Connection, low_battery: float = 20.0) -> dict:
    """Latest report of every robot (DISTINCT ON), robots by state, low-battery robots, faults by code."""
    latest = conn.execute(
        """SELECT DISTINCT ON (robot_id) robot_id, seq, state, battery
             FROM telemetry_events ORDER BY robot_id, ts DESC, seq DESC""").fetchall()
    faults = conn.execute(
        """SELECT fault_code, count(*) FROM telemetry_events
            WHERE state = 'FAULT' GROUP BY fault_code ORDER BY fault_code""").fetchall()
    total = conn.execute("SELECT count(*) FROM telemetry_events").fetchone()[0]
    by_state: dict[str, int] = {}
    for _, _, state, _ in latest:
        by_state[state] = by_state.get(state, 0) + 1
    return {
        "robots": len(latest),
        "events": total,
        "by_state": dict(sorted(by_state.items())),
        "low_battery": sorted(r[0] for r in latest if r[3] < low_battery),
        "fault_reports_by_code": {code: n for code, n in faults},
        "latest": {r[0]: {"seq": r[1], "state": r[2], "battery": round(float(r[3]), 2)} for r in latest},
    }


def all_event_ids(conn: psycopg.Connection) -> list[str]:
    return [r[0] for r in conn.execute("SELECT event_id FROM telemetry_events").fetchall()]


def latencies(conn: psycopg.Connection, exclude: Iterable[str] = ()) -> list[float]:
    """Seconds from the robot creating each report to it being stored in PostgreSQL."""
    rows = conn.execute(
        "SELECT event_id, extract(epoch FROM stored_at - ts) FROM telemetry_events").fetchall()
    skip = set(exclude)
    return [float(s) for eid, s in rows if eid not in skip]
