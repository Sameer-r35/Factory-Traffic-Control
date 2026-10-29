"""Domain models: pure data. No HTTP / DB / MQTT / framework imports anywhere in traffic/domain."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


# ----------------------------------------------------------------------------- enums
class Direction(str, Enum):
    NORTH = "NORTH"
    SOUTH = "SOUTH"
    EAST = "EAST"
    WEST = "WEST"


class SignalState(str, Enum):
    RED = "RED"
    YELLOW = "YELLOW"
    GREEN = "GREEN"
    UNKNOWN = "UNKNOWN"  # only ever used for ACTUAL state, never for desired state


class Mode(str, Enum):
    AUTOMATIC = "AUTOMATIC"
    MANUAL = "MANUAL"
    EMERGENCY = "EMERGENCY"
    DEGRADED = "DEGRADED"


class Step(str, Enum):
    """Where the junction is in the safe cycle: GREEN(P) -> YELLOW(P) -> ALL_RED -> GREEN(Q)."""
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    ALL_RED = "ALL_RED"


class VehicleType(str, Enum):
    EMERGENCY = "EMERGENCY"
    TRUCK = "TRUCK"
    FORKLIFT = "FORKLIFT"
    EMPLOYEE_VEHICLE = "EMPLOYEE_VEHICLE"


class SensorEventType(str, Enum):
    VEHICLE_ARRIVED = "VEHICLE_ARRIVED"
    VEHICLE_CLEARED = "VEHICLE_CLEARED"


class DeviceStatus(str, Enum):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"
    DEGRADED = "DEGRADED"
    WARNING = "WARNING"
    UNKNOWN = "UNKNOWN"


Signals = dict  # dict[Direction, SignalState]


def device_key(device_type: str, direction: Optional[Direction] = None) -> str:
    return f"{device_type}:{direction.value}" if direction else device_type


# ----------------------------------------------------------------------------- state
@dataclass
class QueuedVehicle:
    vehicle_id: str
    vehicle_type: VehicleType
    arrived_at: float   # SERVER receive time: drives waiting-time and TTL logic
    sensor_ts: float    # sensor-reported time: audit + ordering of arrive/clear for one vehicle
    event_id: str


@dataclass
class PendingCommand:
    command_id: str
    signals: Signals
    sent_at: float
    attempts: int
    purpose: str  # TRANSITION | RECOVERY | DEGRADED | PROBE


@dataclass
class JunctionState:
    junction_id: str
    mode: Mode
    phase: Optional[str]            # phase currently/last GREEN (outgoing phase while in YELLOW/ALL_RED)
    step: Step
    step_started_at: float
    desired_signals: Signals        # what the BACKEND wants
    actual_signals: Signals         # what the CONTROLLER last confirmed (UNKNOWN until confirmed)
    queues: dict                    # dict[Direction, dict[vehicle_id, QueuedVehicle]]
    tombstones: dict                # dict[Direction, dict[vehicle_id, sensor_ts_of_clear]]
    target_phase: Optional[str] = None
    pending: Optional[PendingCommand] = None
    processed_events: dict = field(default_factory=dict)   # event_id -> server time
    last_sequence: dict = field(default_factory=dict)      # Direction -> highest seq seen
    command_seq: int = 0
    recent_commands: dict = field(default_factory=dict)    # command_id -> ACKED|SUPERSEDED|TIMEOUT
    manual_phase: Optional[str] = None
    manual_expires_at: Optional[float] = None
    manual_by: Optional[str] = None
    emergency_phase: Optional[str] = None
    device_status: dict = field(default_factory=dict)      # device_key -> DeviceStatus
    degraded_reason: Optional[str] = None
    next_probe_at: Optional[float] = None
    version: int = 0


# ----------------------------------------------------------------------------- inputs
# Raw / loosely typed on purpose: the domain validates, so rejected inputs can be audited
# and the API layer stays thin.
@dataclass(frozen=True)
class SensorEvent:
    event_id: Any = None
    junction_id: Any = None
    direction: Any = None
    event_type: Any = None
    vehicle_id: Any = None
    vehicle_type: Any = None
    sequence_no: Any = None
    timestamp: Any = None


@dataclass(frozen=True)
class StatusEvent:
    event_id: Any = None
    device_type: Any = None
    direction: Any = None
    status: Any = None
    timestamp: Any = None


@dataclass(frozen=True)
class Command:
    command: Any = None
    direction: Any = None
    issued_by: Any = None


@dataclass(frozen=True)
class ControllerAck:
    command_id: Any = None
    status: Any = None                 # ACK | NACK | FAILED
    actual_signals: Any = None         # optional dict[str, str]; None on ACK => "executed as requested"


@dataclass(frozen=True)
class Tick:
    """Time passing. Timers live here, never in request handlers."""


@dataclass(frozen=True)
class Startup:
    """Process (re)start: persisted physical state is NOT trusted."""


# ----------------------------------------------------------------------------- effects & results
@dataclass(frozen=True)
class SendCommand:
    command_id: str
    junction_id: str
    signals: Signals
    attempt: int


@dataclass(frozen=True)
class AuditRecord:
    junction_id: str
    event_type: str
    timestamp: float
    direction: Optional[str] = None
    previous_state: Optional[str] = None
    new_state: Optional[str] = None
    command_id: Optional[str] = None
    detail: Optional[dict] = None


class ResultKind(str, Enum):
    OK = "OK"
    DUPLICATE = "DUPLICATE"   # idempotent repeat: nothing changed
    IGNORED = "IGNORED"       # valid but had no effect (late, unmatched, already queued ...)
    INVALID = "INVALID"       # malformed / unknown values
    CONFLICT = "CONFLICT"     # valid but not allowed in the current mode


@dataclass(frozen=True)
class Result:
    kind: ResultKind
    code: str
    message: str = ""


@dataclass
class EngineResult:
    state: JunctionState
    effects: list
    result: Result
