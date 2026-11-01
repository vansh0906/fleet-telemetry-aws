#!/usr/bin/env python3
"""Runs every experiment end to end and prints the requirement table.

Needs a PostgreSQL server (FLEET_PG_DSN, see README). AWS is emulated by a local moto server, so no AWS
account is used. Takes about 3 minutes.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import uuid
from pathlib import Path

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import requests  # noqa: E402
from psycopg.conninfo import conninfo_to_dict, make_conninfo  # noqa: E402

from fleet import config, db, infra, local, robots  # noqa: E402
from fleet.chaos import TcpProxy  # noqa: E402
from fleet.models import PoisonMessage, TelemetryEvent, encode_message, parse_message  # noqa: E402
from fleet.worker import Worker, WorkerPool  # noqa: E402

ROOT = Path(__file__).resolve().parent
VISIBILITY_S = 2          # short, so redelivery after a crash is quick to observe
SHADOW_TABLE = "fleet-robot-state-naive"


def pct(values: list[float], q: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, int(round(q / 100 * (len(s) - 1))))] if s else float("nan")


def log(msg: str = "") -> None:
    print(msg, flush=True)


class Rig:
    """Everything the experiments need: moto server, stack, API, proxies and database connection."""

    def __init__(self, logdir: Path):
        self.logdir = logdir
        self.moto_port, self.sqs_proxy_port, self.api_port, self.pg_proxy_port = (local.free_port() for _ in range(4))
        self.moto = local.start_moto(self.moto_port, logdir)
        self.endpoint = f"http://127.0.0.1:{self.moto_port}"
        t = time.time()
        self.cfn = config.client("cloudformation", endpoint=self.endpoint)
        self.out = infra.deploy_stack(self.cfn, visibility_timeout=VISIBILITY_S)
        self.deploy_s = time.time() - t
        self.queue_url, self.dlq_url = self.out["QueueUrl"], self.out["DeadLetterQueueUrl"]
        self.sqs = config.client("sqs", endpoint=self.endpoint)
        self.ddb = config.resource("dynamodb", endpoint=self.endpoint)
        self.table = self.ddb.Table(self.out["TableName"])
        if SHADOW_TABLE not in [t.name for t in self.ddb.tables.all()]:
            self.ddb.create_table(TableName=SHADOW_TABLE, BillingMode="PAY_PER_REQUEST",
                                  AttributeDefinitions=[{"AttributeName": "robot_id", "AttributeType": "S"}],
                                  KeySchema=[{"AttributeName": "robot_id", "KeyType": "HASH"}])
        self.shadow = self.ddb.Table(SHADOW_TABLE)

        self.dsn = config.pg_dsn()
        info = conninfo_to_dict(self.dsn)
        self.pg_proxy = TcpProxy(self.pg_proxy_port, info.get("host", "127.0.0.1"), int(info.get("port", 5432))).start()
        self.dsn_via_proxy = make_conninfo(self.dsn, host="127.0.0.1", port=str(self.pg_proxy_port))
        self.sqs_proxy = TcpProxy(self.sqs_proxy_port, "127.0.0.1", self.moto_port).start()
        self.conn = db.connect(self.dsn)
        db.create_schema(self.conn, reset=True)
        # The API reaches AWS through a proxy, so the link between API and SQS can be cut.
        self.api = local.start_api(self.api_port, {
            "AWS_ENDPOINT_URL": f"http://127.0.0.1:{self.sqs_proxy_port}", "FLEET_QUEUE_URL": self.queue_url,
            "FLEET_TABLE": self.out["TableName"], "FLEET_PG_DSN": self.dsn, "FLEET_PG_SCHEMA": ""}, logdir)
        self.api_url = f"http://127.0.0.1:{self.api_port}"

    def close(self) -> None:
        for fn in (self.api.stop, self.pg_proxy.stop, self.sqs_proxy.stop, self.conn.close, self.moto.stop):
            try:
                fn()
            except Exception:
                pass

    def reset(self) -> None:
        db.create_schema(self.conn, reset=True)
        for url in (self.queue_url, self.dlq_url):      # PurgeQueue is limited to once a minute
            infra.drain_queue(self.sqs, url, wait_s=0)
        for t in (self.table, self.shadow):
            for item in t.scan(ProjectionExpression="robot_id").get("Items", []):
                t.delete_item(Key={"robot_id": item["robot_id"]})

    def pool(self, n: int, via_proxy: bool = False, shadow: bool = False, **kw) -> WorkerPool:
        """n workers; via_proxy routes their database traffic through the proxy that can cut the link;
        shadow also writes every state update unconditionally to a second table (the comparison baseline)."""
        dsn = self.dsn_via_proxy if via_proxy else self.dsn

        def make(name, stop, stats):
            ddb = config.resource("dynamodb", endpoint=self.endpoint)    # resources are not thread-safe
            return Worker(name, config.client("sqs", endpoint=self.endpoint), self.queue_url,
                          ddb.Table(self.out["TableName"]), dsn, stats, stop, seed=int(name[1:]) * 7919,
                          shadow_table=ddb.Table(SHADOW_TABLE) if shadow else None, **kw)
        return WorkerPool(n, make).start()

    def wait_drained(self, timeout_s: float = 120) -> float:
        """Waits until the main queue holds no messages (waiting or in flight). Returns seconds waited."""
        t = time.time()
        quiet = 0
        while time.time() - t < timeout_s:
            visible, in_flight = infra.queue_depth(self.sqs, self.queue_url)
            quiet = quiet + 1 if visible + in_flight == 0 else 0
            if quiet >= 3:
                return time.time() - t
            time.sleep(0.3)
        raise TimeoutError("queue did not drain")

    def dead_letters(self) -> tuple[list[str], list[str]]:
        """Drains the DLQ. Returns (bodies that are valid telemetry, bodies that are poison)."""
        valid, poison = [], []
        for m in infra.drain_queue(self.sqs, self.dlq_url, wait_s=0):
            try:
                parse_message(m["Body"])
                valid.append(m["Body"])
            except PoisonMessage:
                poison.append(m["Body"])
        return valid, poison

    def stored_ids(self) -> list[str]:
        return db.all_event_ids(self.conn)


def poison_bodies() -> list[str]:
    good = {"event_id": "feedfacecafebeef", "robot_id": "AR-0001", "seq": 1, "ts": time.time(), "x": 1.0,
            "y": 1.0, "battery": 50.0, "state": "IDLE", "fault_code": None}
    kinds = [
        "{truncated",                                                   # cut-off JSON
        "",                                                             # empty body
        json.dumps({"event": {**good, "battery": 180}, "accepted_at": 1.0}),
        json.dumps({"event": {**good, "state": "DANCING"}, "accepted_at": 1.0}),
        json.dumps({"event": {"robot_id": "AR-0001"}, "accepted_at": 1.0}),
        json.dumps({"event": {**good, "seq": -4}, "accepted_at": 1.0}),
        json.dumps({"event": {**good, "state": "FAULT"}, "accepted_at": 1.0}),
        json.dumps({"payload": "firmware v0.9 format"}),
        json.dumps([1, 2, 3]),
        json.dumps({"event": {**good, "x": "NaN"}, "accepted_at": 1.0}),
    ]
    return [k if k else " " for k in kinds] * 2      # SQS rejects an empty body, so send a single space


def experiment_main(rig: Rig, R: dict) -> None:
    n_robots, hz, duration = int(os.environ.get("ROBOTS", 20)), float(os.environ.get("HZ", 3)), 20.0
    log(f"[1  Fleet run: {n_robots} robots x {hz:.0f} Hz x {duration:.0f} s, 4 workers]")
    log("   faults injected: 3 % lost acks (re-sent batches), 5 % reports held back 1-3 s, "
        "worker crash on 2 % of batches, 20 poison messages")
    rig.reset()
    pool = rig.pool(4, crash_prob=0.02, shadow=True)
    poison = poison_bodies()

    depth: list[int] = []

    def inject(t0):
        # Poison messages arrive 5 s in; meanwhile the queue depth (consumer lag) is sampled every 0.5 s.
        sent = False
        while time.time() - t0 < duration:
            if not sent and time.time() - t0 >= 5:
                for i in range(0, len(poison), 10):
                    rig.sqs.send_message_batch(QueueUrl=rig.queue_url, Entries=[
                        {"Id": str(j), "MessageBody": b} for j, b in enumerate(poison[i:i + 10])])
                sent = True
            depth.append(sum(infra.queue_depth(rig.sqs, rig.queue_url)))
            time.sleep(0.5)

    t = time.time()
    ledger = robots.run_fleet(rig.api_url, n_robots, hz, duration, seed=11, hold_prob=0.05, dup_prob=0.03,
                              during=inject)
    drain_s = rig.wait_drained()
    stats = pool.shutdown()
    stored = rig.stored_ids()
    stored_set = set(stored)
    missing = ledger.accepted - stored_set
    extra = stored_set - ledger.accepted
    unacked = set(ledger.events) - ledger.accepted
    log(f"   reports {len(ledger.events)}, acknowledged {len(ledger.accepted)}, not acknowledged {len(unacked)}")
    log(f"   rows in PostgreSQL {len(stored)} (distinct {len(stored_set)}): missing {len(missing)}, extra {len(extra)}")
    log(f"   messages received {stats['received']}: inserted {stats['inserted']}, duplicates absorbed "
        f"{stats['duplicates']}, poison receives {stats['poison_receives']}")
    log(f"   duplicate sources: {ledger.dup_resends} re-sent batches, {stats['crashes']} worker crashes "
        f"(replaced by the supervisor)")

    truth = ledger.latest_by_robot()
    cond = {i["robot_id"]: int(i["seq"]) for i in rig.table.scan()["Items"]}
    naive = {i["robot_id"]: int(i["seq"]) for i in rig.shadow.scan()["Items"]}
    cond_ok = sum(cond.get(r) == ev["seq"] for r, ev in truth.items())
    naive_ok = sum(naive.get(r) == ev["seq"] for r, ev in truth.items())
    log(f"   latest state in DynamoDB: conditional write {cond_ok}/{len(truth)} robots correct, "
        f"plain last-write-wins {naive_ok}/{len(truth)}; out-of-order writes refused {stats['stale_skipped']}")

    valid_dead, poison_dead = rig.dead_letters()
    log(f"   dead-letter queue: {len(poison_dead)} poison, {len(valid_dead)} valid reports")

    lat = db.latencies(rig.conn, exclude=ledger.held_back)
    lat_all = db.latencies(rig.conn)
    log(f"   robot -> PostgreSQL latency (on-time reports, n={len(lat)}): p50 {pct(lat, 50) * 1000:.0f} ms, "
        f"p95 {pct(lat, 95) * 1000:.0f} ms, p99 {pct(lat, 99) * 1000:.0f} ms, max {max(lat):.2f} s")
    log(f"   incl. held-back reports: p95 {pct(lat_all, 95) * 1000:.0f} ms; load {len(stored) / duration:.0f} "
        f"reports/s; queue depth max {max(depth)}, median {statistics.median(depth):.0f}; queue empty "
        f"{drain_s:.1f} s after the last upload")

    # Fleet summary from the API (SQL) against ground truth from the robots' own ledger.
    summary = requests.get(f"{rig.api_url}/fleet/summary", timeout=10).json()
    by_state: dict = {}
    for ev in truth.values():
        by_state[ev["state"]] = by_state.get(ev["state"], 0) + 1
    low = sorted(r for r, ev in truth.items() if ev["battery"] < 20)
    faults: dict = {}
    for ev in ledger.events.values():
        if ev["state"] == "FAULT" and ev["event_id"] in ledger.accepted:
            faults[ev["fault_code"]] = faults.get(ev["fault_code"], 0) + 1
    checks = {
        "robots": summary["robots"] == len(truth),
        "events": summary["events"] == len(ledger.accepted),
        "by_state": summary["by_state"] == dict(sorted(by_state.items())),
        "low_battery": summary["low_battery"] == low,
        "faults": summary["fault_reports_by_code"] == dict(sorted(faults.items())),
        "latest_seq": all(summary["latest"][r]["seq"] == ev["seq"] for r, ev in truth.items()),
    }
    log(f"   /fleet/summary vs ground truth: {sum(checks.values())}/{len(checks)} fields match "
        f"(by state {summary['by_state']}, low battery {len(low)} robots, fault reports {sum(faults.values())})")
    state = requests.get(f"{rig.api_url}/robots/AR-0001", timeout=5).json()
    log(f"   /robots/AR-0001 -> seq {state['seq']} {state['state']} battery {state['battery']:.1f} %")

    R["R1"] = ("Every acknowledged report stored exactly once", f"{len(missing)} missing, {len(extra)} extra",
               "0, 0", not missing and not extra and len(stored) == len(stored_set) and not unacked)
    R["R2"] = ("Duplicate deliveries absorbed (re-sends + crashes)", f"{stats['duplicates']} absorbed",
               "> 0, R1 holds", stats["duplicates"] > 0 and stats["crashes"] > 0 and not missing and not extra)
    R["R3"] = ("Latest state right despite late reports", f"{cond_ok}/{len(truth)} (naive {naive_ok})",
               "all", cond_ok == len(truth) and stats["stale_skipped"] > 0)
    R["R4"] = ("Poison messages in DLQ, no valid ones", f"{len(poison_dead)}/{len(poison)}, {len(valid_dead)} valid",
               f"{len(poison)}/{len(poison)}, 0", len(poison_dead) == len(poison) and not valid_dead)
    R["R5"] = ("p95 robot -> database latency", f"{pct(lat, 95) * 1000:.0f} ms", "<= 1000 ms", pct(lat, 95) <= 1.0)
    R["R8"] = ("Workers keep up: queue depth bounded", f"max {max(depth)}, empty {drain_s:.1f} s after",
               "<= 200, <= 10 s", max(depth) <= 200 and drain_s <= 10)
    R["R9"] = ("Fleet summary SQL matches ground truth", f"{sum(checks.values())}/{len(checks)} fields",
               "all", all(checks.values()))


def experiment_sqs_partition(rig: Rig, R: dict) -> None:
    n_robots, hz, duration, cut_at, cut_s = 25, 4.0, 12.0, 4.0, 3.0
    log("")
    log(f"[2  API cannot reach SQS for {cut_s:.0f} s ({n_robots} robots, {duration:.0f} s run)]")
    rig.reset()
    pool = rig.pool(4)

    def chaos(t0):
        time.sleep(cut_at)
        rig.sqs_proxy.cut()
        time.sleep(cut_s)
        rig.sqs_proxy.restore()

    ledger = robots.run_fleet(rig.api_url, n_robots, hz, duration, seed=22, during=chaos)
    rig.wait_drained()
    stats = pool.shutdown()
    stored = set(rig.stored_ids())
    missing = ledger.accepted - stored
    unacked = set(ledger.events) - ledger.accepted
    log(f"   uploads {ledger.posts}: failed {ledger.posts_failed} (503 or connection refused), robots buffered up "
        f"to {ledger.max_buffer} reports each")
    log(f"   reports {len(ledger.events)}, stored {len(stored)}, missing {len(missing)}, never acknowledged "
        f"{len(unacked)}; duplicates absorbed {stats['duplicates']}")
    R["R6"] = (f"SQS unreachable {cut_s:.0f} s: no report lost", f"{len(missing) + len(unacked)} lost, "
               f"{ledger.posts_failed} failed uploads", "0 lost, > 0 failed",
               not missing and not unacked and ledger.posts_failed > 0)


def experiment_db_outage(rig: Rig, R: dict) -> None:
    n_robots, hz, duration, cut_at, cut_s = 15, 4.0, 12.0, 3.0, 5.0
    log("")
    log(f"[3  PostgreSQL unreachable for {cut_s:.0f} s ({n_robots} robots, {duration:.0f} s run, 4 workers)]")
    results = {}
    for mode in ("naive retry", "circuit breaker"):
        rig.reset()
        pool = rig.pool(4, via_proxy=True, circuit_breaker=(mode == "circuit breaker"))

        def chaos(t0):
            time.sleep(cut_at)
            rig.pg_proxy.cut()
            time.sleep(cut_s)
            rig.pg_proxy.restore()

        ledger = robots.run_fleet(rig.api_url, n_robots, hz, duration, seed=33, during=chaos)
        rig.wait_drained()
        valid_dead, _ = rig.dead_letters()
        stored = set(rig.stored_ids())
        missing_before = len(ledger.accepted - stored)
        moved = 0
        if valid_dead:
            # Recovery an operator would do: put the dead-lettered reports back on the queue.
            for body in valid_dead:
                rig.sqs.send_message(QueueUrl=rig.queue_url, MessageBody=body)
            moved = len(valid_dead)
            rig.wait_drained()
        stats = pool.shutdown()
        missing_after = len(ledger.accepted - set(rig.stored_ids()))
        results[mode] = dict(dlq=len(valid_dead), missing_before=missing_before, missing_after=missing_after,
                             db_errors=stats["db_errors"], breaker_s=stats["breaker_open_s"],
                             received=stats["received"], reports=len(ledger.accepted))
        log(f"   {mode:15s}: {len(ledger.accepted)} reports, DB errors {stats['db_errors']}, receives "
            f"{stats['received']}, valid reports dead-lettered {len(valid_dead)}, missing {missing_before}"
            + (f" -> {missing_after} after redriving {moved} from the DLQ" if moved else ""))
    nv, cb = results["naive retry"], results["circuit breaker"]
    R["R7"] = (f"DB down {cut_s:.0f} s: breaker keeps data out of DLQ", f"{cb['dlq']} in DLQ (naive {nv['dlq']})",
               "0, 0 missing", cb["dlq"] == 0 and cb["missing_before"] == 0 and nv["missing_after"] == 0)


def experiment_index(rig: Rig, R: dict) -> None:
    robots_n, per_robot = 200, 2000
    log("")
    log(f"[4  SQL index: one robot's last 100 reports from {robots_n * per_robot:,} rows]")
    c = rig.conn
    c.execute("DROP TABLE IF EXISTS telemetry_bench")
    c.execute("CREATE TABLE telemetry_bench (LIKE telemetry_events INCLUDING DEFAULTS)")
    c.execute(f"""INSERT INTO telemetry_bench (event_id, robot_id, seq, ts, x, y, battery, state, accepted_at)
                  SELECT md5(r::text || '-' || s::text), 'AR-' || lpad(r::text, 4, '0'), s,
                         now() - (({per_robot} - s) * interval '250 milliseconds'), random() * 100, random() * 60,
                         random() * 100, 'MOVING', now()
                    FROM generate_series(1, {robots_n}) r, generate_series(1, {per_robot}) s""")
    c.execute("ANALYZE telemetry_bench")
    query = ("SELECT seq, ts, x, y, battery, state FROM telemetry_bench "
             "WHERE robot_id = 'AR-0042' ORDER BY ts DESC LIMIT 100")

    def measure() -> tuple[float, str]:
        times, node = [], ""
        for _ in range(15):
            plan = c.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + query).fetchone()[0][0]
            times.append(plan["Execution Time"])
            node = json.dumps(plan["Plan"])
        kind = "Index Scan" if "Index Scan" in node else ("Seq Scan" if "Seq Scan" in node else "other")
        return statistics.median(times), kind

    no_idx, plan_a = measure()
    c.execute("CREATE INDEX telemetry_bench_robot_ts ON telemetry_bench (robot_id, ts DESC)")
    c.execute("ANALYZE telemetry_bench")
    with_idx, plan_b = measure()
    size = c.execute("SELECT pg_size_pretty(pg_relation_size('telemetry_bench')), "
                     "pg_size_pretty(pg_relation_size('telemetry_bench_robot_ts'))").fetchone()
    c.execute("DROP TABLE telemetry_bench")
    speedup = no_idx / with_idx
    log(f"   without index: {no_idx:.2f} ms ({plan_a}); with index on (robot_id, ts): {with_idx:.3f} ms ({plan_b})")
    log(f"   table {size[0]}, index {size[1]}; speed-up {speedup:.0f}x")
    R["R10"] = ("Index speeds up robot history query", f"{speedup:.0f}x, {plan_b}", ">= 10x, index used",
                speedup >= 10 and plan_b == "Index Scan")


def experiment_infra_and_api(rig: Rig, R: dict) -> None:
    log("")
    log("[5  Infrastructure and API checks]")
    attrs = rig.sqs.get_queue_attributes(QueueUrl=rig.queue_url, AttributeNames=["All"])["Attributes"]
    policy = json.loads(attrs["RedrivePolicy"])
    key = rig.ddb.meta.client.describe_table(TableName=rig.out["TableName"])["Table"]["KeySchema"]
    ok_stack = (int(policy["maxReceiveCount"]) == 3 and policy["deadLetterTargetArn"].endswith("fleet-telemetry-dlq")
                and key == [{"AttributeName": "robot_id", "KeyType": "HASH"}]
                and attrs["VisibilityTimeout"] == str(VISIBILITY_S))
    log(f"   CloudFormation stack deployed in {rig.deploy_s:.1f} s: queue -> DLQ after "
        f"{policy['maxReceiveCount']} receives, visibility {attrs['VisibilityTimeout']} s, table key robot_id")
    good = {"event_id": uuid.uuid4().hex, "robot_id": "AR-0001", "seq": 1, "ts": time.time(), "x": 1.0, "y": 1.0,
            "battery": 50.0, "state": "IDLE", "fault_code": None}
    bad = [{"events": []}, {"events": [{**good, "battery": 150}]}, {"events": [{**good, "state": "FAULT"}]},
           {"events": [{**good, "seq": "seven"}]}, {"events": [{**good, "x": None}]}, {"evts": [good]},
           {"events": [good] * 501}]
    codes = [requests.post(f"{rig.api_url}/telemetry", json=b, timeout=5).status_code for b in bad]
    rejected = sum(code == 422 for code in codes)
    log(f"   malformed uploads rejected with 422: {rejected}/{len(bad)}")
    R["R11"] = ("CloudFormation stack: queue, DLQ, table", "as specified" if ok_stack else "differs",
                "maxReceive 3, key robot_id", ok_stack)
    R["R12"] = ("Malformed uploads rejected (HTTP 422)", f"{rejected}/{len(bad)}", "all", rejected == len(bad))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--logdir", default=str(ROOT / "logs"))
    ap.add_argument("--only", type=int, nargs="*", help="run only these experiments (1-5)")
    args = ap.parse_args()
    logdir = Path(args.logdir)
    logdir.mkdir(parents=True, exist_ok=True)
    log(f"PostgreSQL: {make_conninfo(config.pg_dsn(), password='***') if 'password' in config.pg_dsn() else config.pg_dsn()}")
    log("AWS: local moto server (SQS, DynamoDB, CloudFormation); no AWS account used")
    log("")
    R: dict = {}
    rig = Rig(logdir)
    try:
        experiments = [experiment_main, experiment_sqs_partition, experiment_db_outage, experiment_index,
                       experiment_infra_and_api]
        for i, fn in enumerate(experiments, start=1):
            if not args.only or i in args.only:
                fn(rig, R)
    finally:
        rig.close()
    log("")
    log(f"{'ID':5}{'Requirement':52}{'Result':30}{'Target':28}Status")
    order = sorted(R, key=lambda k: int(k[1:]))
    for k in order:
        text, result, target, ok = R[k]
        log(f"{k:5}{text:52}{result:30}{target:28}{'PASS' if ok else 'FAIL'}")
    passed = sum(1 for k in order if R[k][3])
    log(f"{passed}/{len(order)} requirements pass")
    return 0 if passed == len(order) else 1


if __name__ == "__main__":
    sys.exit(main())
