"""Queue worker: SQS -> PostgreSQL (history) + DynamoDB (latest state per robot).

Delivery from SQS is at-least-once, so every step is idempotent:
  1. INSERT ... ON CONFLICT (event_id) DO NOTHING      -> a redelivered event is stored once
  2. DynamoDB put only if the stored seq is older       -> a late or repeated report never overwrites newer state
  3. delete the messages from the queue                 -> only after 1 and 2 succeeded
A worker that dies between 1 and 3 leaves the messages in the queue; SQS delivers them again after the
visibility timeout and steps 1 and 2 turn the repeat into a no-op.
"""
from __future__ import annotations

import argparse
import random
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Optional

import psycopg
from botocore.exceptions import BotoCoreError, ClientError

from . import config, db
from .models import PoisonMessage, TelemetryEvent, parse_message


class WorkerCrash(Exception):
    """Injected fault: the worker dies at this point, as if its process was killed."""


@dataclass
class WorkerStats:
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    received: int = 0            # messages received from SQS (including repeats)
    inserted: int = 0            # rows newly written to PostgreSQL
    duplicates: int = 0          # valid messages whose event was already stored
    state_updates: int = 0       # DynamoDB writes that won the seq condition
    stale_skipped: int = 0       # DynamoDB writes refused because a newer report was already there
    poison_receives: int = 0     # receives of messages that can never be parsed
    db_errors: int = 0
    aws_errors: int = 0
    crashes: int = 0
    breaker_open_s: float = 0.0
    batches: int = 0

    def add(self, **kw) -> None:
        with self.lock:
            for k, v in kw.items():
                setattr(self, k, getattr(self, k) + v)

    def snapshot(self) -> dict:
        with self.lock:
            return {k: v for k, v in self.__dict__.items() if k != "lock"}


def _dec(v: float, places: int = 3) -> Decimal:
    return Decimal(str(round(v, places)))


def state_item(event: TelemetryEvent) -> dict:
    item = {"robot_id": event.robot_id, "seq": event.seq, "ts": _dec(event.ts), "x": _dec(event.x),
            "y": _dec(event.y), "battery": _dec(event.battery, 2), "state": event.state.value}
    if event.fault_code:
        item["fault_code"] = event.fault_code
    return item


def put_latest_state(table, event: TelemetryEvent) -> bool:
    """Conditional write: succeeds only if the table has no newer report for this robot.
    Returns False when the write was refused because a newer (or the same) report is already stored."""
    try:
        table.put_item(Item=state_item(event),
                       ConditionExpression="attribute_not_exists(robot_id) OR #seq < :seq",
                       ExpressionAttributeNames={"#seq": "seq"},
                       ExpressionAttributeValues={":seq": event.seq})
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


class Worker:
    def __init__(self, name: str, sqs, queue_url: str, table, dsn: str, stats: WorkerStats,
                 stop: threading.Event, schema: Optional[str] = None, circuit_breaker: bool = True,
                 crash_prob: float = 0.0, seed: int = 0, shadow_table=None, wait_s: int = 1):
        self.name = name
        self.sqs = sqs
        self.queue_url = queue_url
        self.table = table
        self.dsn = dsn
        self.schema = schema
        self.stats = stats
        self.stop = stop
        self.circuit_breaker = circuit_breaker
        self.crash_prob = crash_prob
        self.rng = random.Random(seed)
        self.shadow_table = shadow_table      # baseline for comparison: plain last-write-wins puts
        self.wait_s = wait_s
        self.conn: Optional[psycopg.Connection] = None
        self.breaker_open_since: Optional[float] = None
        self.backoff = 0.2

    # ---- database connection and circuit breaker -------------------------------------------------
    def _db(self) -> psycopg.Connection:
        if self.conn is None or self.conn.closed:
            self.conn = db.connect(self.dsn, schema=self.schema)
        return self.conn

    def _drop_db(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None

    def _breaker_probe(self) -> None:
        """While the breaker is open the worker stops taking messages; it only checks the database."""
        time.sleep(self.backoff)
        try:
            self._db().execute("SELECT 1")
            self.stats.add(breaker_open_s=time.time() - self.breaker_open_since)
            self.breaker_open_since = None
            self.backoff = 0.2
        except psycopg.Error:
            self._drop_db()
            self.backoff = min(2.0, self.backoff * 2)

    def _maybe_crash(self) -> None:
        if self.crash_prob and self.rng.random() < self.crash_prob:
            raise WorkerCrash(self.name)

    # ---- main loop -----------------------------------------------------------------------------------
    def run(self) -> None:
        while not self.stop.is_set():
            if self.breaker_open_since is not None:
                self._breaker_probe()
                continue
            try:
                self.step()
            except (BotoCoreError, ClientError):
                # SQS or DynamoDB unreachable or throttling: nothing was deleted, so nothing is lost.
                self.stats.add(aws_errors=1)
                time.sleep(0.5)
        if self.breaker_open_since is not None:
            self.stats.add(breaker_open_s=time.time() - self.breaker_open_since)
        self._drop_db()

    def step(self) -> int:
        """Receives and processes one batch of up to 10 messages. Returns the number received."""
        msgs = self.sqs.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=10,
                                        WaitTimeSeconds=self.wait_s,
                                        MessageSystemAttributeNames=["ApproximateReceiveCount"]
                                        ).get("Messages", [])
        if not msgs:
            return 0
        self.stats.add(received=len(msgs), batches=1)

        good: list[tuple[dict, TelemetryEvent, float]] = []
        for m in msgs:
            try:
                event, accepted_at = parse_message(m["Body"])
                good.append((m, event, accepted_at))
            except PoisonMessage:
                # Not deleted: SQS delivers it again, and after maxReceiveCount deliveries moves it to the DLQ.
                self.stats.add(poison_receives=1)
        if not good:
            return len(msgs)

        try:
            inserted = db.insert_events(self._db(), [(ev, acc) for _, ev, acc in good])
        except (psycopg.OperationalError, psycopg.InterfaceError):
            self._on_db_failure([m for m, _, _ in good])
            return len(msgs)
        self.stats.add(inserted=len(inserted), duplicates=len(good) - len(inserted))

        self._maybe_crash()                               # dies after commit, before the state update

        newest: dict[str, TelemetryEvent] = {}
        for _, ev, _ in good:
            if ev.robot_id not in newest or ev.seq > newest[ev.robot_id].seq:
                newest[ev.robot_id] = ev
        for ev in newest.values():
            if put_latest_state(self.table, ev):
                self.stats.add(state_updates=1)
            else:
                self.stats.add(stale_skipped=1)
            if self.shadow_table is not None:
                self.shadow_table.put_item(Item=state_item(ev))

        self._maybe_crash()                               # dies after the state update, before delete

        self.sqs.delete_message_batch(QueueUrl=self.queue_url, Entries=[
            {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, (m, _, _) in enumerate(good)])
        return len(msgs)

    def _on_db_failure(self, msgs: list[dict]) -> None:
        self.stats.add(db_errors=1)
        self._drop_db()
        if self.circuit_breaker:
            # Leave the messages alone (SQS shows them again after the visibility timeout) and stop
            # receiving until the database answers. Each message is charged at most one extra receive.
            self.breaker_open_since = time.time()
        else:
            # Naive retry: release the messages at once. Every retry costs a receive, so during an outage
            # healthy messages run through maxReceiveCount and end up in the dead-letter queue.
            self.sqs.change_message_visibility_batch(QueueUrl=self.queue_url, Entries=[
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 0}
                for i, m in enumerate(msgs)])


class WorkerPool:
    """N worker threads plus a supervisor that replaces a worker when it crashes."""

    def __init__(self, n: int, make_worker: Callable[[str, threading.Event, WorkerStats], Worker]):
        self.n = n
        self.make_worker = make_worker
        self.stop = threading.Event()
        self.stats = WorkerStats()
        self.threads: list[threading.Thread] = []
        self._counter = 0
        self._supervisor: Optional[threading.Thread] = None

    def _spawn(self) -> threading.Thread:
        self._counter += 1
        worker = self.make_worker(f"w{self._counter}", self.stop, self.stats)

        def body():
            try:
                worker.run()
            except WorkerCrash:
                worker._drop_db()
                self.stats.add(crashes=1)

        t = threading.Thread(target=body, name=worker.name, daemon=True)
        t.start()
        return t

    def start(self) -> "WorkerPool":
        self.threads = [self._spawn() for _ in range(self.n)]
        self._supervisor = threading.Thread(target=self._supervise, daemon=True)
        self._supervisor.start()
        return self

    def _supervise(self) -> None:
        while not self.stop.is_set():
            for i, t in enumerate(self.threads):
                if not t.is_alive() and not self.stop.is_set():
                    self.threads[i] = self._spawn()
            time.sleep(0.05)

    def shutdown(self) -> dict:
        self.stop.set()
        if self._supervisor:
            self._supervisor.join()
        for t in self.threads:
            t.join(timeout=10)
        return self.stats.snapshot()


def main() -> None:
    ap = argparse.ArgumentParser(description="Run queue workers until Ctrl-C.")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--no-circuit-breaker", action="store_true")
    args = ap.parse_args()
    sqs = config.client("sqs")
    table = config.resource("dynamodb").Table(config.table_name())
    pool = WorkerPool(args.workers, lambda name, stop, stats: Worker(
        name, sqs, config.queue_url(), table, config.pg_dsn(), stats, stop, schema=config.pg_schema(),
        circuit_breaker=not args.no_circuit_breaker)).start()
    print(f"{args.workers} workers on {config.queue_url()}; Ctrl-C to stop")
    try:
        while True:
            time.sleep(5)
            print(pool.stats.snapshot())
    except KeyboardInterrupt:
        print(pool.shutdown())


if __name__ == "__main__":
    main()
