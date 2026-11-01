"""Worker and API against an emulated AWS (moto, in-process) and a real PostgreSQL.
Skipped when PostgreSQL is not reachable at FLEET_PG_DSN."""
import os
import threading
import time
import unittest
import uuid

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import boto3  # noqa: E402
import psycopg  # noqa: E402
import requests  # noqa: E402
from moto import mock_aws  # noqa: E402

from fleet import config, db, infra  # noqa: E402
from fleet.local import free_port  # noqa: E402
from fleet.models import TelemetryEvent, encode_message  # noqa: E402
from fleet.worker import Worker, WorkerCrash, WorkerStats  # noqa: E402

SCHEMA = "fleet_test"


def pg_available() -> bool:
    try:
        with psycopg.connect(config.pg_dsn(), connect_timeout=2):
            return True
    except psycopg.Error:
        return False


def make_event(robot="AR-0001", seq=1, state="MOVING", fault_code=None, battery=50.0) -> TelemetryEvent:
    return TelemetryEvent(event_id=uuid.uuid4().hex, robot_id=robot, seq=seq, ts=time.time() - 10 + seq * 0.01,
                          x=1.0, y=2.0, battery=battery, state=state, fault_code=fault_code)


@unittest.skipUnless(pg_available(), "PostgreSQL not reachable at FLEET_PG_DSN")
class TestWorker(unittest.TestCase):
    def setUp(self):
        self.mock = mock_aws()
        self.mock.start()
        self.out = infra.deploy_stack(boto3.client("cloudformation"), stack_name="t", prefix="t", visibility_timeout=1)
        self.sqs = boto3.client("sqs")
        self.table = boto3.resource("dynamodb").Table(self.out["TableName"])
        self.conn = db.connect(config.pg_dsn(), schema=SCHEMA)
        db.create_schema(self.conn, reset=True)
        self.stats = WorkerStats()
        self.stop = threading.Event()
        self.workers: list[Worker] = []

    def tearDown(self):
        for w in self.workers:
            w._drop_db()
        self.conn.close()
        self.mock.stop()

    def worker(self, **kw) -> Worker:
        kw.setdefault("dsn", config.pg_dsn())
        w = Worker("t", self.sqs, self.out["QueueUrl"], self.table, stats=self.stats, stop=self.stop,
                   schema=SCHEMA, wait_s=0, **kw)
        self.workers.append(w)
        return w

    def send(self, *events: TelemetryEvent) -> None:
        for ev in events:
            self.sqs.send_message(QueueUrl=self.out["QueueUrl"], MessageBody=encode_message(ev, time.time()))

    def rows(self) -> int:
        return self.conn.execute("SELECT count(*) FROM telemetry_events").fetchone()[0]

    def drain(self, w: Worker, seconds: float = 4.0) -> None:
        deadline = time.time() + seconds
        while time.time() < deadline:
            if w.step() == 0 and sum(infra.queue_depth(self.sqs, self.out["QueueUrl"])) == 0:
                return
            time.sleep(0.05)

    def test_stores_history_and_latest_state(self):
        self.send(make_event(seq=1), make_event(seq=2), make_event(robot="AR-0002", seq=1))
        self.drain(self.worker())
        self.assertEqual(self.rows(), 3)
        self.assertEqual(int(self.table.get_item(Key={"robot_id": "AR-0001"})["Item"]["seq"]), 2)
        self.assertEqual(sum(infra.queue_depth(self.sqs, self.out["QueueUrl"])), 0)

    def test_duplicate_is_stored_once(self):
        ev = make_event()
        self.send(ev, ev)
        self.drain(self.worker())
        self.assertEqual(self.rows(), 1)
        self.assertEqual(self.stats.duplicates, 1)

    def test_late_report_does_not_overwrite_newer_state(self):
        self.send(make_event(seq=5, state="CHARGING"))
        self.drain(self.worker())
        self.send(make_event(seq=3, state="MOVING"))
        self.drain(self.worker())
        item = self.table.get_item(Key={"robot_id": "AR-0001"})["Item"]
        self.assertEqual((int(item["seq"]), item["state"]), (5, "CHARGING"))
        self.assertEqual(self.stats.stale_skipped, 1)
        self.assertEqual(self.rows(), 2)

    def test_crash_after_commit_is_redelivered_and_idempotent(self):
        ev = make_event(seq=9)
        self.send(ev)
        with self.assertRaises(WorkerCrash):
            self.worker(crash_prob=1.0).step()
        self.assertEqual(self.rows(), 1)
        self.assertNotIn("Item", self.table.get_item(Key={"robot_id": "AR-0001"}))
        time.sleep(1.1)                                   # visibility timeout: SQS delivers it again
        self.drain(self.worker())
        self.assertEqual(self.rows(), 1)
        self.assertEqual(self.stats.duplicates, 1)
        self.assertEqual(int(self.table.get_item(Key={"robot_id": "AR-0001"})["Item"]["seq"]), 9)

    def test_poison_message_goes_to_dlq(self):
        self.sqs.send_message(QueueUrl=self.out["QueueUrl"], MessageBody="{not json")
        w = self.worker()
        deadline = time.time() + 8
        while time.time() < deadline and sum(infra.queue_depth(self.sqs, self.out["QueueUrl"])) > 0:
            w.step()
            time.sleep(0.2)
        self.assertEqual(self.stats.poison_receives, 3)
        dead = infra.drain_queue(self.sqs, self.out["DeadLetterQueueUrl"], wait_s=0)
        self.assertEqual([m["Body"] for m in dead], ["{not json"])

    def test_database_down_opens_breaker_and_keeps_message(self):
        dead_dsn = f"host=127.0.0.1 port={free_port()} user=nobody dbname=none"
        self.send(make_event())
        w = self.worker(dsn=dead_dsn)
        w.step()
        self.assertIsNotNone(w.breaker_open_since)
        self.assertEqual(self.stats.db_errors, 1)
        self.assertEqual(sum(infra.queue_depth(self.sqs, self.out["QueueUrl"])), 1)
        # The database comes back: the breaker closes and the message is processed.
        w.dsn = config.pg_dsn()
        w._breaker_probe()
        self.assertIsNone(w.breaker_open_since)
        time.sleep(1.1)
        self.drain(w)
        self.assertEqual(self.rows(), 1)


@unittest.skipUnless(pg_available(), "PostgreSQL not reachable at FLEET_PG_DSN")
class TestApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import uvicorn

        cls.mock = mock_aws()
        cls.mock.start()
        out = infra.deploy_stack(boto3.client("cloudformation"), stack_name="api", prefix="api")
        cls.out = out
        os.environ.update(FLEET_QUEUE_URL=out["QueueUrl"], FLEET_TABLE=out["TableName"], FLEET_PG_SCHEMA=SCHEMA)
        cls.conn = db.connect(config.pg_dsn(), schema=SCHEMA)
        db.create_schema(cls.conn, reset=True)
        from fleet.api import app

        cls.port = free_port()
        cls.server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=cls.port, log_level="warning"))
        cls.thread = threading.Thread(target=cls.server.run, daemon=True)
        cls.thread.start()
        deadline = time.time() + 10
        while not cls.server.started and time.time() < deadline:
            time.sleep(0.05)
        cls.base = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.should_exit = True
        cls.thread.join(timeout=5)
        cls.conn.close()
        os.environ.pop("FLEET_PG_SCHEMA", None)
        cls.mock.stop()

    def test_accepts_valid_batch_and_queues_each_event(self):
        events = [make_event(seq=i).model_dump(mode="json") for i in range(1, 24)]
        r = requests.post(f"{self.base}/telemetry", json={"events": events}, timeout=5)
        self.assertEqual(r.status_code, 202, r.text)
        self.assertEqual(r.json(), {"accepted": 23})
        msgs = infra.drain_queue(boto3.client("sqs"), self.out["QueueUrl"], wait_s=0)
        self.assertEqual(len(msgs), 23)

    def test_rejects_malformed(self):
        good = make_event().model_dump(mode="json")
        bad_batches = [{"events": []}, {"events": [dict(good, battery=150)]}, {"evts": [good]},
                       {"events": [dict(good, state="FAULT")]}, {"events": [dict(good, seq="x")]}]
        for body in bad_batches:
            with self.subTest(body=body):
                self.assertEqual(requests.post(f"{self.base}/telemetry", json=body, timeout=5).status_code, 422)

    def test_queries(self):
        db.create_schema(self.conn, reset=True)
        db.insert_events(self.conn, [(make_event(seq=1, battery=15.0), time.time()),
                                     (make_event(seq=2, state="FAULT", fault_code="LIDAR_TIMEOUT", battery=14.0),
                                      time.time()),
                                     (make_event(robot="AR-0002", seq=1, battery=80.0), time.time())])
        s = requests.get(f"{self.base}/fleet/summary", timeout=5).json()
        self.assertEqual(s["robots"], 2)
        self.assertEqual(s["by_state"], {"FAULT": 1, "MOVING": 1})
        self.assertEqual(s["low_battery"], ["AR-0001"])
        self.assertEqual(s["fault_reports_by_code"], {"LIDAR_TIMEOUT": 1})
        hist = requests.get(f"{self.base}/robots/AR-0001/history?limit=1", timeout=5).json()
        self.assertEqual([h["seq"] for h in hist], [2])
        self.assertEqual(requests.get(f"{self.base}/robots/AR-0404", timeout=5).status_code, 404)
        self.assertEqual(requests.get(f"{self.base}/health", timeout=5).status_code, 200)


if __name__ == "__main__":
    unittest.main()
