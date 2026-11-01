"""Simulated warehouse robots that report telemetry to the ingest API.

Each robot drives between random waypoints in a 100 m x 60 m floor, drains and recharges its battery, and
now and then reports a fault. It behaves like real robot firmware on unreliable Wi-Fi:
  * store-and-forward: reports are buffered and re-sent until the API answers 202
  * lost acknowledgements: with probability `dup_prob` the robot re-sends a batch that was in fact accepted
  * late reports: with probability `hold_prob` a report is held back 1-3 s and sent after newer ones
The fleet keeps a ledger of every report, which is the ground truth the experiments check against.
"""
from __future__ import annotations

import math
import random
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import requests

FAULT_CODES = ["ESTOP_PRESSED", "LIDAR_TIMEOUT", "DRIVE_OVERCURRENT", "LOCALIZATION_LOST"]


@dataclass
class FleetLedger:
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    events: dict = field(default_factory=dict)        # event_id -> report dict (every report generated)
    accepted: set = field(default_factory=set)        # event_ids the API acknowledged with 202
    held_back: set = field(default_factory=set)       # event_ids deliberately sent late
    posts: int = 0
    posts_ok: int = 0
    posts_failed: int = 0                             # 503, timeouts, refused connections
    dup_resends: int = 0                              # batches re-sent after a "lost" acknowledgement
    max_buffer: int = 0                               # largest number of reports a robot was holding

    def latest_by_robot(self) -> dict:
        out: dict = {}
        for ev in self.events.values():
            if ev["event_id"] in self.accepted:
                cur = out.get(ev["robot_id"])
                if cur is None or ev["seq"] > cur["seq"]:
                    out[ev["robot_id"]] = ev
        return out


class Robot(threading.Thread):
    def __init__(self, robot_id: str, api: str, ledger: FleetLedger, hz: float, duration_s: float,
                 seed: int, upload_period_s: float = 0.25, hold_prob: float = 0.0, dup_prob: float = 0.0,
                 flush_deadline_s: float = 60.0):
        super().__init__(name=robot_id, daemon=True)
        self.robot_id = robot_id
        self.api = api.rstrip("/")
        self.ledger = ledger
        self.hz = hz
        self.duration_s = duration_s
        self.rng = random.Random(seed)
        self.upload_period_s = upload_period_s
        self.hold_prob = hold_prob
        self.dup_prob = dup_prob
        self.flush_deadline_s = flush_deadline_s
        self.session = requests.Session()
        self.seq = 0
        self.x, self.y = self.rng.uniform(0, 100), self.rng.uniform(0, 60)
        self.goal = self._new_goal()
        self.battery = self.rng.uniform(8, 100)
        self.state = "IDLE"
        self.fault_left = 0
        self.fault_code: Optional[str] = None
        self.outbox: list[dict] = []
        self.delayed: list[tuple[float, dict]] = []

    def _new_goal(self) -> tuple[float, float]:
        return self.rng.uniform(0, 100), self.rng.uniform(0, 60)

    def _tick(self, dt: float) -> dict:
        """Advance the robot's motion and battery model by dt seconds and produce one report."""
        if self.fault_left > 0:
            self.fault_left -= 1
            if self.fault_left == 0:
                self.state, self.fault_code = "IDLE", None
        elif self.rng.random() < 0.004:
            self.state, self.fault_code, self.fault_left = "FAULT", self.rng.choice(FAULT_CODES), self.rng.randint(3, 10)
        elif self.state == "CHARGING":
            self.battery = min(100.0, self.battery + 2.0 * dt)
            if self.battery >= 95:
                self.state = "IDLE"
        elif self.battery < 12:
            self.state = "CHARGING"
        else:
            gx, gy = self.goal
            dist = math.hypot(gx - self.x, gy - self.y)
            if dist < 0.5:
                self.goal = self._new_goal()
                self.state = "IDLE" if self.rng.random() < 0.3 else "MOVING"
            else:
                step = min(dist, 1.5 * dt)
                self.x += (gx - self.x) / dist * step
                self.y += (gy - self.y) / dist * step
                self.state = "MOVING"
            self.battery = max(0.0, self.battery - (0.4 if self.state == "MOVING" else 0.05) * dt)
        self.seq += 1
        ev = {"event_id": uuid.UUID(int=self.rng.getrandbits(128)).hex, "robot_id": self.robot_id,
              "seq": self.seq, "ts": time.time(), "x": round(self.x, 3), "y": round(self.y, 3),
              "battery": round(self.battery, 2), "state": self.state, "fault_code": self.fault_code}
        with self.ledger.lock:
            self.ledger.events[ev["event_id"]] = ev
        return ev

    def _post(self, batch: list[dict]) -> bool:
        with self.ledger.lock:
            self.ledger.posts += 1
        try:
            r = self.session.post(f"{self.api}/telemetry", json={"events": batch}, timeout=5)
            ok = r.status_code == 202
        except requests.RequestException:
            ok = False
        with self.ledger.lock:
            if ok:
                self.ledger.posts_ok += 1
                self.ledger.accepted.update(e["event_id"] for e in batch)
            else:
                self.ledger.posts_failed += 1
        return ok

    def _upload(self) -> None:
        now = time.time()
        due = [e for t, e in self.delayed if t <= now]
        self.delayed = [(t, e) for t, e in self.delayed if t > now]
        self.outbox.extend(due)
        with self.ledger.lock:
            self.ledger.max_buffer = max(self.ledger.max_buffer, len(self.outbox))
        if not self.outbox:
            return
        batch = self.outbox[:500]
        if self._post(batch):
            self.outbox = self.outbox[len(batch):]
            self.backoff = self.upload_period_s
            if self.dup_prob and self.rng.random() < self.dup_prob:
                with self.ledger.lock:
                    self.ledger.dup_resends += 1
                self._post(batch)                 # the ack was "lost": the same reports go out again
        else:
            self.backoff = min(2.0, self.backoff * 2)  # keep the reports and retry later

    def run(self) -> None:
        start = time.time()
        period = 1.0 / self.hz
        next_tick = start + self.rng.uniform(0, period)
        next_upload = start + self.rng.uniform(0, self.upload_period_s)
        self.backoff = self.upload_period_s
        while time.time() - start < self.duration_s:
            now = time.time()
            if now >= next_tick:
                ev = self._tick(period)
                next_tick += period
                if self.hold_prob and self.rng.random() < self.hold_prob:
                    self.delayed.append((now + self.rng.uniform(1.0, 3.0), ev))
                    with self.ledger.lock:
                        self.ledger.held_back.add(ev["event_id"])
                else:
                    self.outbox.append(ev)
            if now >= next_upload:
                self._upload()
                next_upload = time.time() + self.backoff
            time.sleep(max(0.0, min(next_tick, next_upload) - time.time()))
        # Shift over: send everything still buffered, including held-back reports.
        deadline = time.time() + self.flush_deadline_s
        while (self.outbox or self.delayed) and time.time() < deadline:
            if self.delayed and not self.outbox:
                time.sleep(max(0.0, min(t for t, _ in self.delayed) - time.time()))
            self._upload()
            if self.outbox:
                time.sleep(self.backoff)


def run_fleet(api: str, n_robots: int, hz: float, duration_s: float, seed: int = 1, hold_prob: float = 0.0,
              dup_prob: float = 0.0, during=None) -> FleetLedger:
    """Runs n robots for duration_s and waits until every report was acknowledged (or 60 s passed).
    `during(t0)` is an optional callback run in its own thread, used to inject faults mid-run."""
    ledger = FleetLedger()
    robots = [Robot(f"AR-{i + 1:04d}", api, ledger, hz, duration_s, seed * 1000 + i,
                    hold_prob=hold_prob, dup_prob=dup_prob) for i in range(n_robots)]
    t0 = time.time()
    for r in robots:
        r.start()
    chaos = None
    if during is not None:
        chaos = threading.Thread(target=during, args=(t0,), daemon=True)
        chaos.start()
    for r in robots:
        r.join()
    if chaos is not None:
        chaos.join()
    return ledger
