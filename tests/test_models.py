import json
import unittest

from pydantic import ValidationError

from fleet.models import PoisonMessage, TelemetryBatch, TelemetryEvent, encode_message, parse_message

GOOD = {"event_id": "0123456789abcdef", "robot_id": "AR-0001", "seq": 7, "ts": 1_790_000_000.5,
        "x": 12.5, "y": 40.25, "battery": 64.2, "state": "MOVING", "fault_code": None}


def event(**changes) -> dict:
    d = dict(GOOD)
    d.update(changes)
    return d


class TestTelemetryEvent(unittest.TestCase):
    def test_valid_event(self):
        ev = TelemetryEvent.model_validate(GOOD)
        self.assertEqual(ev.seq, 7)
        self.assertEqual(ev.state.value, "MOVING")

    def test_rejects_bad_values(self):
        bad = [
            event(battery=101), event(battery=-1), event(seq=-1), event(ts=0),
            event(x=float("nan")), event(y=float("inf")), event(state="FLYING"),
            event(event_id="short"), event(event_id="has spaces in it"), event(robot_id=""),
            event(state="FAULT", fault_code=None), event(state="IDLE", fault_code="ESTOP_PRESSED"),
            event(extra_field=1),
        ]
        for d in bad:
            with self.subTest(d=d), self.assertRaises(ValidationError):
                TelemetryEvent.model_validate(d)

    def test_fault_needs_code(self):
        ev = TelemetryEvent.model_validate(event(state="FAULT", fault_code="LIDAR_TIMEOUT"))
        self.assertEqual(ev.fault_code, "LIDAR_TIMEOUT")

    def test_batch_limits(self):
        with self.assertRaises(ValidationError):
            TelemetryBatch.model_validate({"events": []})
        with self.assertRaises(ValidationError):
            TelemetryBatch.model_validate({"events": [GOOD] * 501})
        self.assertEqual(len(TelemetryBatch.model_validate({"events": [GOOD] * 500}).events), 500)


class TestMessages(unittest.TestCase):
    def test_round_trip(self):
        ev = TelemetryEvent.model_validate(GOOD)
        back, accepted_at = parse_message(encode_message(ev, 1_790_000_001.25))
        self.assertEqual(back, ev)
        self.assertEqual(accepted_at, 1_790_000_001.25)

    def test_poison(self):
        poison = ["not json", "[]", json.dumps({"event": GOOD}), json.dumps({"event": event(battery=150),
                  "accepted_at": 1.0}), json.dumps({"event": {"robot_id": "AR-0001"}, "accepted_at": 1.0}),
                  json.dumps({"event": GOOD, "accepted_at": "yesterday"})]
        for body in poison:
            with self.subTest(body=body), self.assertRaises(PoisonMessage):
                parse_message(body)


if __name__ == "__main__":
    unittest.main()
