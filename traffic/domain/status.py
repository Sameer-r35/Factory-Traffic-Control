"""Read-side view of a junction: alerts + the JSON-friendly status the API/dashboard shows.
The UI only renders this; it never decides sequencing."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .config import JunctionConfig
from .engine import active_emergencies
from .models import DeviceStatus, JunctionState, Mode, SignalState, Step


@dataclass(frozen=True)
class Alert:
    level: str            # INFO | WARNING | CRITICAL
    code: str
    message: str
    direction: Optional[str] = None


def queue_counts(state: JunctionState) -> dict:
    return {d.value: len(q) for d, q in state.queues.items()}


def compute_alerts(state: JunctionState, cfg: JunctionConfig, now: float) -> list:
    out = []
    ds = state.device_status
    for key, st in ds.items():
        dtype, _, dirn = key.partition(":")
        where = dirn or None
        if dtype == "SIGNAL_CONTROLLER":
            if st == DeviceStatus.OFFLINE:
                code = "SIGNAL_FAILURE" if where else "CONTROLLER_OFFLINE"
                out.append(Alert("CRITICAL", code, f"{key} is OFFLINE", where))
            elif st in (DeviceStatus.DEGRADED, DeviceStatus.WARNING):
                out.append(Alert("WARNING", "CONTROLLER_" + st.value, f"{key} reports {st.value}", where))
        elif dtype == "SENSOR" and st in (DeviceStatus.OFFLINE, DeviceStatus.DEGRADED, DeviceStatus.WARNING):
            out.append(Alert("WARNING", "SENSOR_FAILURE", f"{key} is {st.value}: queue data may be wrong", where))
    if ds.get("SIGNAL_CONTROLLER") == DeviceStatus.UNKNOWN or any(
            v == SignalState.UNKNOWN for v in state.actual_signals.values()):
        out.append(Alert("WARNING", "UNKNOWN_DEVICE_STATE", "physical signal state is not confirmed"))
    if state.mode == Mode.DEGRADED:
        out.append(Alert("CRITICAL", "DEGRADED_MODE", state.degraded_reason or "degraded"))
    p = state.pending
    if p is not None:
        if p.attempts > 1:
            out.append(Alert("WARNING", "COMMAND_TIMEOUT",
                             f"{p.command_id} unacknowledged, attempt {p.attempts}/{cfg.max_retries + 1}"))
        else:
            out.append(Alert("INFO", "AWAITING_ACK", f"{p.command_id} sent, waiting for controller"))
    if state.actual_signals != state.desired_signals and state.mode != Mode.DEGRADED and p is None:
        out.append(Alert("CRITICAL", "STATE_MISMATCH", "desired and confirmed signal states differ"))
    if state.mode == Mode.EMERGENCY:
        dirs = sorted({d.value for d, _ in active_emergencies(state)})
        out.append(Alert("CRITICAL", "EMERGENCY_ACTIVE", f"emergency vehicle(s) from {', '.join(dirs)}"))
    if state.mode == Mode.MANUAL:
        out.append(Alert("INFO", "MANUAL_OVERRIDE_ACTIVE", f"manual control of {state.manual_phase}"))
    return out


def status_view(state: JunctionState, cfg: JunctionConfig, now: float) -> dict:
    ctl = state.device_status.get("SIGNAL_CONTROLLER", DeviceStatus.UNKNOWN)
    controller_status = ctl.value
    if ctl != DeviceStatus.OFFLINE and state.mode == Mode.DEGRADED:
        controller_status = "DEGRADED"
    p = state.pending
    ems = active_emergencies(state)
    return {
        "junction_id": state.junction_id,
        "mode": state.mode.value,
        "phase": state.phase,
        "step": state.step.value,
        "target_phase": state.target_phase,
        "step_elapsed_s": round(now - state.step_started_at, 1),
        "controller_status": controller_status,
        "desired_signals": {d.value: s.value for d, s in state.desired_signals.items()},
        "actual_signals": {d.value: s.value for d, s in state.actual_signals.items()},
        "queues": queue_counts(state),
        "pending_command": None if p is None else {
            "command_id": p.command_id, "attempts": p.attempts, "purpose": p.purpose,
            "age_s": round(now - p.sent_at, 1), "signals": {d.value: s.value for d, s in p.signals.items()},
        },
        "emergency": {
            "active": state.mode == Mode.EMERGENCY,
            "vehicles": [{"direction": d.value, "vehicle_id": vid} for d, vid in ems],
            "serving_phase": state.emergency_phase,
        },
        "manual": {
            "active": state.mode == Mode.MANUAL, "phase": state.manual_phase, "by": state.manual_by,
            "expires_in_s": None if state.manual_expires_at is None else max(0.0, round(state.manual_expires_at - now, 1)),
        },
        "degraded_reason": state.degraded_reason,
        "alerts": [a.__dict__ for a in compute_alerts(state, cfg, now)],
        "version": state.version,
    }
