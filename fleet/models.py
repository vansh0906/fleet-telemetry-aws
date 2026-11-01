"""Telemetry message format, validation, and the SQS message body encoding."""
from __future__ import annotations

import json
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


class RobotState(str, Enum):
    IDLE = "IDLE"
    MOVING = "MOVING"
    CHARGING = "CHARGING"
    FAULT = "FAULT"


class TelemetryEvent(BaseModel):
    """One status report from one robot. `seq` counts up by one per report on each robot."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=8, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    robot_id: str = Field(min_length=1, max_length=32, pattern=r"^[A-Za-z0-9_-]+$")
    seq: int = Field(ge=0)
    ts: float = Field(gt=0, allow_inf_nan=False, description="robot clock, Unix seconds")
    x: float = Field(ge=-10_000, le=10_000, allow_inf_nan=False, description="metres")
    y: float = Field(ge=-10_000, le=10_000, allow_inf_nan=False, description="metres")
    battery: float = Field(ge=0, le=100, allow_inf_nan=False, description="percent")
    state: RobotState
    fault_code: Optional[str] = Field(default=None, max_length=40)

    @model_validator(mode="after")
    def fault_code_only_in_fault(self) -> "TelemetryEvent":
        if self.state == RobotState.FAULT and not self.fault_code:
            raise ValueError("state FAULT needs a fault_code")
        if self.state != RobotState.FAULT and self.fault_code:
            raise ValueError("fault_code is only allowed in state FAULT")
        return self


class TelemetryBatch(BaseModel):
    """What a robot POSTs: the reports it has buffered since its last successful upload."""

    model_config = ConfigDict(extra="forbid")

    events: list[TelemetryEvent] = Field(min_length=1, max_length=500)


class PoisonMessage(Exception):
    """A queue message that can never be processed (bad JSON, missing or out-of-range fields)."""


def encode_message(event: TelemetryEvent, accepted_at: float) -> str:
    """SQS message body: the event plus the time the ingest API accepted it."""
    return json.dumps({"event": event.model_dump(mode="json"), "accepted_at": accepted_at}, separators=(",", ":"))


def parse_message(body: str) -> tuple[TelemetryEvent, float]:
    """Inverse of encode_message. Raises PoisonMessage for anything that will never parse."""
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, TypeError) as exc:
        raise PoisonMessage(f"not JSON: {exc}") from exc
    if not isinstance(data, dict) or "event" not in data or "accepted_at" not in data:
        raise PoisonMessage("body must be an object with 'event' and 'accepted_at'")
    try:
        event = TelemetryEvent.model_validate(data["event"])
        accepted_at = float(data["accepted_at"])
    except (ValidationError, TypeError, ValueError) as exc:
        raise PoisonMessage(f"invalid event: {str(exc).splitlines()[0]}") from exc
    return event, accepted_at
