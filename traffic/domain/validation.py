"""Validation of raw inputs. Returns (value, None) or (None, (code, message)); never raises."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from .config import JunctionConfig
from .models import Direction, SensorEvent, SensorEventType, SignalState, VehicleType


def parse_enum(enum_cls, value: Any):
    if isinstance(value, enum_cls):
        return value
    if not isinstance(value, str):
        return None
    try:
        return enum_cls(value.strip().upper())
    except ValueError:
        return None


def parse_ts(value: Any) -> Optional[float]:
    """ISO-8601 -> epoch seconds. Naive timestamps are treated as UTC."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _good_id(value: Any) -> bool:
    return isinstance(value, str) and 0 < len(value.strip()) <= 128


def parse_signals(cfg: JunctionConfig, raw: Any) -> Optional[dict]:
    if not isinstance(raw, dict):
        return None
    out = {}
    for k, v in raw.items():
        d, s = parse_enum(Direction, k), parse_enum(SignalState, v)
        if d is None or s is None or d not in cfg.directions:
            return None
        out[d] = s
    return out


@dataclass(frozen=True)
class ValidSensorEvent:
    event_id: str
    event_type: SensorEventType
    direction: Direction
    vehicle_id: str
    vehicle_type: Optional[VehicleType]
    sequence_no: Optional[int]
    sensor_ts: float


def validate_sensor_event(ev: SensorEvent, cfg: JunctionConfig):
    def bad(code, msg):
        return None, (code, msg)

    if ev.junction_id is not None and ev.junction_id != cfg.junction_id:
        return bad("UNKNOWN_JUNCTION", f"event is for junction {ev.junction_id!r}")
    if not _good_id(ev.event_id):
        return bad("INVALID_EVENT_ID", "event_id is required (non-empty string, <=128 chars)")
    etype = parse_enum(SensorEventType, ev.event_type)
    if etype is None:
        return bad("INVALID_EVENT_TYPE", f"unsupported event_type {ev.event_type!r}")
    direction = parse_enum(Direction, ev.direction)
    if direction is None or direction not in cfg.directions:
        return bad("INVALID_DIRECTION", f"unsupported direction {ev.direction!r}")
    if not _good_id(ev.vehicle_id):
        return bad("INVALID_VEHICLE_ID", "vehicle_id is required")

    vtype = None
    if isinstance(ev.vehicle_type, str):
        raw = ev.vehicle_type.strip().upper()
        vtype = cfg.vehicle_type_aliases.get(raw) or parse_enum(VehicleType, raw)
    if etype == SensorEventType.VEHICLE_ARRIVED and vtype is None:
        return bad("UNKNOWN_VEHICLE_TYPE", f"unsupported vehicle_type {ev.vehicle_type!r}")

    seq = ev.sequence_no
    if seq is not None and (isinstance(seq, bool) or not isinstance(seq, int) or seq < 0):
        return bad("INVALID_SEQUENCE_NO", "sequence_no must be a non-negative integer")

    ts = parse_ts(ev.timestamp)
    if ts is None:
        return bad("INVALID_TIMESTAMP", "timestamp must be ISO-8601, e.g. 2026-10-05T10:15:20Z")

    return ValidSensorEvent(ev.event_id.strip(), etype, direction, ev.vehicle_id.strip(), vtype, seq, ts), None
