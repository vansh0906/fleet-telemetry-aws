"""Ingest and query API (FastAPI).

POST /telemetry           robots upload buffered reports; validated, then queued in SQS (202) or 503
GET  /robots/{id}         latest state from DynamoDB (one key lookup)
GET  /robots/{id}/history recent reports from PostgreSQL
GET  /fleet/summary       fleet-wide SQL: robots by state, low battery, faults by code
GET  /health, /metrics
"""
from __future__ import annotations

import threading
import time
from collections import Counter

import psycopg
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Query

from . import config, db
from .models import TelemetryBatch, encode_message

app = FastAPI(title="Robot fleet telemetry")

_metrics: Counter = Counter()
_metrics_lock = threading.Lock()
_local = threading.local()


def _count(key: str, n: int = 1) -> None:
    with _metrics_lock:
        _metrics[key] += n


def _sqs():
    # fail_fast: a robot is better served by a quick 503 (it keeps the data and retries) than a long hang.
    if not hasattr(_local, "sqs"):
        _local.sqs = config.client("sqs", fail_fast=True)
    return _local.sqs


def _table():
    if not hasattr(_local, "table"):
        _local.table = config.resource("dynamodb").Table(config.table_name())
    return _local.table


def _pg() -> psycopg.Connection:
    conn = getattr(_local, "pg", None)
    if conn is None or conn.closed:
        conn = _local.pg = db.connect(config.pg_dsn(), schema=config.pg_schema())
    return conn


def _send_all(bodies: list[str]) -> None:
    """send_message_batch in chunks of 10 (the SQS limit); entries that fail are retried once."""
    sqs, url = _sqs(), config.queue_url()
    for start in range(0, len(bodies), 10):
        entries = [{"Id": str(i), "MessageBody": b} for i, b in enumerate(bodies[start:start + 10])]
        for attempt in range(2):
            failed = sqs.send_message_batch(QueueUrl=url, Entries=entries).get("Failed", [])
            if not failed:
                break
            ids = {f["Id"] for f in failed}
            entries = [e for e in entries if e["Id"] in ids]
        else:
            raise RuntimeError(f"{len(entries)} messages refused by SQS")


@app.post("/telemetry", status_code=202)
def ingest(batch: TelemetryBatch) -> dict:
    accepted_at = time.time()
    bodies = [encode_message(ev, accepted_at) for ev in batch.events]
    try:
        _send_all(bodies)
    except (BotoCoreError, ClientError, RuntimeError):
        # Some events of this batch may already be queued. The robot re-sends the whole batch, and the
        # worker's ON CONFLICT on event_id turns those repeats into no-ops.
        _count("ingest_503")
        raise HTTPException(status_code=503, detail="queue unavailable, keep the data and retry")
    _count("events_accepted", len(bodies))
    _count("batches_accepted")
    return {"accepted": len(bodies)}


@app.get("/robots/{robot_id}")
def robot_state(robot_id: str) -> dict:
    try:
        item = _table().get_item(Key={"robot_id": robot_id}).get("Item")
    except (BotoCoreError, ClientError):
        raise HTTPException(status_code=503, detail="state store unavailable")
    if not item:
        raise HTTPException(status_code=404, detail="unknown robot")
    return {k: (float(v) if k not in ("robot_id", "state", "fault_code", "seq") else
                (int(v) if k == "seq" else v)) for k, v in item.items()}


@app.get("/robots/{robot_id}/history")
def robot_history(robot_id: str, limit: int = Query(50, ge=1, le=1000)) -> list[dict]:
    try:
        return db.robot_history(_pg(), robot_id, limit)
    except psycopg.Error:
        _local.pg = None
        raise HTTPException(status_code=503, detail="database unavailable")


@app.get("/fleet/summary")
def fleet_summary(low_battery: float = Query(20.0, ge=0, le=100)) -> dict:
    try:
        return db.fleet_summary(_pg(), low_battery)
    except psycopg.Error:
        _local.pg = None
        raise HTTPException(status_code=503, detail="database unavailable")


@app.get("/health")
def health() -> dict:
    checks = {}
    try:
        _sqs().get_queue_attributes(QueueUrl=config.queue_url(), AttributeNames=["QueueArn"])
        checks["sqs"] = "ok"
    except (BotoCoreError, ClientError) as exc:
        checks["sqs"] = f"error: {type(exc).__name__}"
    try:
        _pg().execute("SELECT 1")
        checks["postgres"] = "ok"
    except psycopg.Error as exc:
        _local.pg = None
        checks["postgres"] = f"error: {type(exc).__name__}"
    if any(v != "ok" for v in checks.values()):
        raise HTTPException(status_code=503, detail=checks)
    return checks


@app.get("/metrics")
def metrics() -> dict:
    with _metrics_lock:
        return dict(_metrics)
