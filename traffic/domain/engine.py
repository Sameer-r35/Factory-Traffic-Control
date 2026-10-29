"""Traffic domain engine.

    handle(state, config, input, now) -> EngineResult(new_state, effects, result)

PURE: no I/O, no clock, no threads, no HTTP/DB/MQTT. `now` is injected, the input state is never
mutated (we work on a deep copy), and the effects (SendCommand, AuditRecord) are returned as data for
the application layer to execute. Same state + same input + same `now` => same output.

Safe cycle (one step at a time, never skipped):
    GREEN(P) -> YELLOW(P) -> ALL_RED -> GREEN(Q)
  * YELLOW->ALL_RED requires YELLOW to be CONFIRMED by the controller and >= yellow_s elapsed.
  * ALL_RED->GREEN requires ALL_RED to be CONFIRMED by the controller and >= all_red_s elapsed.
  * Emergency / manual only change WHICH phase is the target and whether min_green is waived.
  * Only exception: Startup and DEGRADED jump straight to ALL_RED. RED is always the safe state.
"""
from __future__ import annotations

import copy
from typing import Optional

from . import safety, scheduler
from .config import JunctionConfig
from .models import (
    AuditRecord, Command, ControllerAck, DeviceStatus, Direction, EngineResult, JunctionState, Mode,
    PendingCommand, QueuedVehicle, Result, ResultKind, SensorEvent, SensorEventType, SendCommand,
    SignalState, Startup, StatusEvent, Step, Tick, VehicleType, device_key,
)
from .validation import parse_enum, parse_signals, parse_ts, validate_sensor_event

RECENT_COMMANDS_MAX = 100
CONTROLLER = "SIGNAL_CONTROLLER"


# ============================================================================ public API
def initial_state(cfg: JunctionConfig, now: float) -> JunctionState:
    """Brand-new junction. Nothing is confirmed, so the first Startup input drives it to ALL_RED."""
    return JunctionState(
        junction_id=cfg.junction_id,
        mode=Mode.AUTOMATIC,
        phase=None,
        step=Step.ALL_RED,
        step_started_at=now,
        desired_signals={d: SignalState.RED for d in cfg.directions},
        actual_signals={d: SignalState.UNKNOWN for d in cfg.directions},
        queues={d: {} for d in cfg.directions},
        tombstones={d: {} for d in cfg.directions},
    )


def signals_for(cfg: JunctionConfig, step: Step, phase: Optional[str]) -> dict:
    out = {d: SignalState.RED for d in cfg.directions}
    if step == Step.ALL_RED or phase is None:
        return out
    show = SignalState.GREEN if step == Step.GREEN else SignalState.YELLOW
    for d in cfg.directions_of(phase):
        out[d] = show
    return out


def active_emergencies(state: JunctionState) -> list:
    """Emergency vehicles currently queued, oldest (server time) first. Stale ones are removed in _expire."""
    found = [
        (v.arrived_at, v.vehicle_id, d)
        for d, q in state.queues.items() for v in q.values()
        if v.vehicle_type == VehicleType.EMERGENCY
    ]
    return [(d, vid) for _, vid, d in sorted(found, key=lambda t: (t[0], t[1]))]


def handle(state: JunctionState, config: JunctionConfig, inp, now: float) -> EngineResult:
    ctx = _Ctx(copy.deepcopy(state), config, now)
    if isinstance(inp, SensorEvent):
        res = _on_sensor_event(ctx, inp)
    elif isinstance(inp, ControllerAck):
        res = _on_ack(ctx, inp)
    elif isinstance(inp, Command):
        res = _on_command(ctx, inp)
    elif isinstance(inp, StatusEvent):
        res = _on_status_event(ctx, inp)
    elif isinstance(inp, Tick):
        res = _on_tick(ctx)
    elif isinstance(inp, Startup):
        res = _on_startup(ctx)
    else:
        raise TypeError(f"unsupported input {type(inp).__name__}")

    # Rejected / duplicate / ignored inputs must not move the control loop.
    if isinstance(inp, (Tick, Startup)) or res.kind == ResultKind.OK:
        _advance(ctx)
    ctx.s.version += 1
    return EngineResult(ctx.s, ctx.effects, res)


# ============================================================================ context
class _Ctx:
    def __init__(self, s: JunctionState, cfg: JunctionConfig, now: float):
        self.s, self.cfg, self.now, self.effects = s, cfg, now, []

    def audit(self, event_type, direction=None, previous_state=None, new_state=None, command_id=None, **detail):
        self.effects.append(AuditRecord(
            junction_id=self.s.junction_id, event_type=event_type, timestamp=self.now,
            direction=_v(direction), previous_state=_v(previous_state), new_state=_v(new_state),
            command_id=command_id, detail=detail or None,
        ))


def _v(x):
    return x.value if hasattr(x, "value") else x


# ============================================================================ sensor events
def _on_sensor_event(ctx: _Ctx, ev: SensorEvent) -> Result:
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    v, err = validate_sensor_event(ev, cfg)
    if err:
        code, msg = err
        ctx.audit("SENSOR_EVENT_REJECTED", code=code, message=msg, event_id=_safe_str(ev.event_id))
        return Result(ResultKind.INVALID, code, msg)

    # 1) idempotency: event_id is authoritative for duplicate detection
    if v.event_id in s.processed_events:
        ctx.audit("DUPLICATE_EVENT_REJECTED", direction=v.direction, event_id=v.event_id)
        return Result(ResultKind.DUPLICATE, "DUPLICATE_EVENT", "event_id already processed; no change")

    # 2) time window: sensor clock is only used to reject absurd values; server time drives logic
    age = now - v.sensor_ts
    if age < -cfg.future_skew_s:
        ctx.audit("SENSOR_EVENT_REJECTED", direction=v.direction, code="TIMESTAMP_IN_FUTURE", event_id=v.event_id)
        return Result(ResultKind.INVALID, "TIMESTAMP_IN_FUTURE", "sensor timestamp is too far in the future")
    s.processed_events[v.event_id] = now
    if age > cfg.stale_event_s:
        ctx.audit("SENSOR_EVENT_REJECTED", direction=v.direction, code="STALE_EVENT",
                  event_id=v.event_id, age_s=round(age, 1))
        return Result(ResultKind.IGNORED, "STALE_EVENT", f"event is {age:.0f}s old (limit {cfg.stale_event_s:.0f}s)")

    # 3) sequence_no: diagnostics only (arrive/clear are order-independent, see below)
    seq_note = None
    if v.sequence_no is not None:
        last = s.last_sequence.get(v.direction)
        if last is not None:
            if v.sequence_no <= last:
                seq_note = "OUT_OF_ORDER"
            elif v.sequence_no > last + 1:
                seq_note = "SEQUENCE_GAP"
        if last is None or v.sequence_no > last:
            s.last_sequence[v.direction] = v.sequence_no
    info = dict(event_id=v.event_id, vehicle_id=v.vehicle_id, sequence_no=v.sequence_no, seq_note=seq_note)

    q = s.queues[v.direction]
    tomb = s.tombstones[v.direction]

    if v.event_type == SensorEventType.VEHICLE_ARRIVED:
        if v.vehicle_id in q:
            ctx.audit("DUPLICATE_EVENT_REJECTED", direction=v.direction, reason="ALREADY_QUEUED", **info)
            return Result(ResultKind.IGNORED, "ALREADY_QUEUED", "vehicle is already waiting in this queue")
        cleared_at = tomb.get(v.vehicle_id)
        if cleared_at is not None and v.sensor_ts <= cleared_at:
            ctx.audit("SENSOR_EVENT_REJECTED", direction=v.direction, code="LATE_ARRIVAL", **info)
            return Result(ResultKind.IGNORED, "LATE_ARRIVAL", "arrival is older than this vehicle's recorded clearance")
        tomb.pop(v.vehicle_id, None)  # newer arrival = a new visit
        q[v.vehicle_id] = QueuedVehicle(v.vehicle_id, v.vehicle_type, now, v.sensor_ts, v.event_id)
        ctx.audit("VEHICLE_DETECTED", direction=v.direction, vehicle_type=v.vehicle_type.value, **info)
        if v.vehicle_type == VehicleType.EMERGENCY:
            ctx.audit("EMERGENCY_DETECTED", direction=v.direction, **info)
        return Result(ResultKind.OK, "VEHICLE_QUEUED", seq_note or "")

    # VEHICLE_CLEARED
    if v.vehicle_id in q:
        gone = q.pop(v.vehicle_id)
        tomb[v.vehicle_id] = max(v.sensor_ts, tomb.get(v.vehicle_id, 0.0))
        ctx.audit("VEHICLE_CLEARED", direction=v.direction, waited_s=round(now - gone.arrived_at, 1),
                  emergency=gone.vehicle_type == VehicleType.EMERGENCY, **info)
        return Result(ResultKind.OK, "VEHICLE_CLEARED", seq_note or "")
    if v.vehicle_id in tomb and v.sensor_ts <= tomb[v.vehicle_id]:
        ctx.audit("DUPLICATE_EVENT_REJECTED", direction=v.direction, reason="ALREADY_CLEARED", **info)
        return Result(ResultKind.IGNORED, "ALREADY_CLEARED", "vehicle was already cleared")
    # CLEARED before ARRIVED (or never arrived): leave a tombstone so a late ARRIVED is not resurrected.
    tomb[v.vehicle_id] = max(v.sensor_ts, tomb.get(v.vehicle_id, 0.0))
    ctx.audit("SENSOR_EVENT_REJECTED", direction=v.direction, code="UNMATCHED_CLEAR", **info)
    return Result(ResultKind.IGNORED, "UNMATCHED_CLEAR", "no matching arrival; queue unchanged")


def _safe_str(x):
    return x if isinstance(x, str) else repr(x)


# ============================================================================ device status
def _on_status_event(ctx: _Ctx, ev: StatusEvent) -> Result:
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    if not isinstance(ev.event_id, str) or not ev.event_id.strip():
        return _reject(ctx, "INVALID_EVENT_ID", "event_id is required")
    dtype = ev.device_type.strip().upper() if isinstance(ev.device_type, str) else None
    if dtype not in (CONTROLLER, "SENSOR"):
        return _reject(ctx, "INVALID_DEVICE_TYPE", f"unsupported device_type {ev.device_type!r}")
    status = parse_enum(DeviceStatus, ev.status)
    if status is None:
        return _reject(ctx, "INVALID_STATUS", f"unsupported status {ev.status!r}")
    direction = None
    if ev.direction is not None:
        direction = parse_enum(Direction, ev.direction)
        if direction is None or direction not in cfg.directions:
            return _reject(ctx, "INVALID_DIRECTION", f"unsupported direction {ev.direction!r}")
    if ev.event_id in s.processed_events:
        ctx.audit("DUPLICATE_EVENT_REJECTED", direction=direction, event_id=ev.event_id)
        return Result(ResultKind.DUPLICATE, "DUPLICATE_EVENT", "event_id already processed")
    s.processed_events[ev.event_id] = now

    key = device_key(dtype, direction)
    prev = s.device_status.get(key, DeviceStatus.UNKNOWN)
    s.device_status[key] = status
    ctx.audit("DEVICE_STATUS_CHANGED", direction=direction, previous_state=prev, new_state=status,
              device=key, event_id=ev.event_id)

    if dtype == CONTROLLER:
        if status in (DeviceStatus.OFFLINE, DeviceStatus.UNKNOWN):
            ctx.audit("SIGNAL_FAILURE" if direction else "CONTROLLER_OFFLINE", direction=direction, device=key)
            _degrade(ctx, f"{key} is {status.value}")
        elif status == DeviceStatus.ONLINE and s.mode == Mode.DEGRADED and s.pending is None \
                and not _signal_devices_down(s):
            ctx.audit("CONTROLLER_RECONNECTED", device=key)
            _probe(ctx)
    elif status == DeviceStatus.OFFLINE:
        ctx.audit("SENSOR_FAILURE", direction=direction, device=key)
    return Result(ResultKind.OK, "STATUS_RECORDED")


def _reject(ctx, code, msg):
    ctx.audit("INPUT_REJECTED", code=code, message=msg)
    return Result(ResultKind.INVALID, code, msg)


def _signal_devices_down(s: JunctionState) -> bool:
    return any(k.startswith(CONTROLLER) and st == DeviceStatus.OFFLINE for k, st in s.device_status.items())


# ============================================================================ manual commands
def _on_command(ctx: _Ctx, cmd: Command) -> Result:
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    name = cmd.command.strip().upper() if isinstance(cmd.command, str) else None
    who = cmd.issued_by if isinstance(cmd.issued_by, str) else None

    if name == "MANUAL_GREEN_REQUEST":
        direction = parse_enum(Direction, cmd.direction)
        if direction is None or direction not in cfg.directions:
            ctx.audit("COMMAND_REJECTED", code="INVALID_DIRECTION", command=name, issued_by=who)
            return Result(ResultKind.INVALID, "INVALID_DIRECTION", f"unsupported direction {cmd.direction!r}")
        if s.mode == Mode.EMERGENCY:
            ctx.audit("COMMAND_REJECTED", code="EMERGENCY_ACTIVE", command=name, issued_by=who)
            return Result(ResultKind.CONFLICT, "EMERGENCY_ACTIVE", "emergency mode overrides manual control")
        if s.mode == Mode.DEGRADED:
            ctx.audit("COMMAND_REJECTED", code="JUNCTION_DEGRADED", command=name, issued_by=who)
            return Result(ResultKind.CONFLICT, "JUNCTION_DEGRADED", "controller is not confirming commands")
        phase = cfg.phase_of(direction)
        _set_mode(ctx, Mode.MANUAL, f"manual request by {who or 'unknown'}")
        s.manual_phase, s.manual_by = phase, who
        s.manual_expires_at = now + cfg.manual_lease_s   # re-issuing refreshes the lease; last valid command wins
        ctx.audit("MANUAL_OVERRIDE", direction=direction, phase=phase, issued_by=who, expires_at=s.manual_expires_at)
        return Result(ResultKind.OK, "MANUAL_ACCEPTED", f"{phase} will be served via the safe transition sequence")

    if name == "RETURN_TO_AUTOMATIC":
        if s.mode == Mode.MANUAL:
            _set_mode(ctx, Mode.AUTOMATIC, f"returned to automatic by {who or 'unknown'}")
            ctx.audit("RETURN_TO_AUTOMATIC", issued_by=who)
            return Result(ResultKind.OK, "RETURNED_TO_AUTOMATIC")
        reason = {Mode.AUTOMATIC: "ALREADY_AUTOMATIC", Mode.EMERGENCY: "EMERGENCY_ACTIVE",
                  Mode.DEGRADED: "JUNCTION_DEGRADED"}[s.mode]
        ctx.audit("COMMAND_REJECTED", code=reason, command=name, issued_by=who)
        return Result(ResultKind.IGNORED, reason, f"junction is {s.mode.value}; nothing to return from")

    ctx.audit("COMMAND_REJECTED", code="UNKNOWN_COMMAND", command=_safe_str(cmd.command), issued_by=who)
    return Result(ResultKind.INVALID, "UNKNOWN_COMMAND", f"unsupported command {cmd.command!r}")


# ============================================================================ controller acknowledgements
def _on_ack(ctx: _Ctx, ack: ControllerAck) -> Result:
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    cid = ack.command_id
    status = ack.status.strip().upper() if isinstance(ack.status, str) else None
    if not isinstance(cid, str) or not cid.strip():
        return _reject(ctx, "INVALID_COMMAND_ID", "command_id is required")
    if status not in ("ACK", "NACK", "FAILED"):
        return _reject(ctx, "INVALID_STATUS", f"unsupported status {ack.status!r}")
    reported = None
    if ack.actual_signals is not None:
        reported = parse_signals(cfg, ack.actual_signals)
        if reported is None:
            return _reject(ctx, "INVALID_ACTUAL_SIGNALS", "actual_signals must map known directions to known states")

    p = s.pending
    if p is None or p.command_id != cid:
        prior = s.recent_commands.get(cid)
        if prior is None:
            ctx.audit("CONTROLLER_ACK_REJECTED", command_id=cid, code="UNKNOWN_COMMAND")
            return Result(ResultKind.INVALID, "UNKNOWN_COMMAND", "no such command")
        if prior == "ACKED":
            ctx.audit("DUPLICATE_ACK", command_id=cid)
            return Result(ResultKind.DUPLICATE, "DUPLICATE_ACK", "command was already acknowledged")
        # Late reply to a command we already gave up on / replaced. It may describe a stale physical
        # state, so it is recorded but NEVER used to update actual state.
        ctx.audit("LATE_ACK", command_id=cid, command_status=prior)
        return Result(ResultKind.IGNORED, "LATE_ACK", f"command was {prior}; reply ignored")

    if status != "ACK":
        ctx.audit("CONTROLLER_COMMAND_FAILED", command_id=cid, status=status)
        _command_failed(ctx, f"controller replied {status}")
        return Result(ResultKind.OK, "COMMAND_FAILURE_RECORDED")

    actual = {d: SignalState.UNKNOWN for d in cfg.directions}
    actual.update(reported if reported is not None else p.signals)
    for d in cfg.directions:
        if actual[d] != s.actual_signals.get(d):
            ctx.audit("SIGNAL_CHANGED", direction=d, previous_state=s.actual_signals.get(d),
                      new_state=actual[d], command_id=cid)
    s.actual_signals = actual
    s.device_status[CONTROLLER] = DeviceStatus.ONLINE  # an answer proves reachability
    ctx.audit("CONTROLLER_ACK", command_id=cid)

    if actual != p.signals:
        ctx.audit("SIGNAL_MISMATCH", command_id=cid,
                  desired={d.value: x.value for d, x in p.signals.items()},
                  actual={d.value: x.value for d, x in actual.items()})
        _command_failed(ctx, "controller state does not match the request")
        return Result(ResultKind.OK, "MISMATCH_RECORDED")

    s.recent_commands[cid] = "ACKED"
    _trim_recent(s)
    s.pending = None
    ctx.audit("SIGNAL_CONFIRMED", command_id=cid, step=s.step.value, phase=s.phase)

    if s.mode == Mode.DEGRADED and not _signal_devices_down(s):
        _set_mode(ctx, Mode.AUTOMATIC, "controller confirmed ALL_RED; recovered")
        s.degraded_reason, s.next_probe_at = None, None
        s.step, s.step_started_at, s.target_phase = Step.ALL_RED, now, None
        ctx.audit("RECOVERED_FROM_DEGRADED", command_id=cid)
    return Result(ResultKind.OK, "ACK_ACCEPTED")


def _on_tick(ctx: _Ctx) -> Result:
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    p = s.pending
    if p is not None and now - p.sent_at >= cfg.ack_timeout_s:
        ctx.audit("COMMAND_TIMEOUT", command_id=p.command_id, attempt=p.attempts)
        _command_failed(ctx, "no ACK within timeout")
    return Result(ResultKind.OK, "TICK")


def _command_failed(ctx: _Ctx, reason: str) -> None:
    """Retry with the SAME command_id (so a late ACK of any attempt correlates), then fail safe."""
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    p = s.pending
    if p is None:
        return
    if p.attempts <= cfg.max_retries:
        p.attempts += 1
        p.sent_at = now
        ctx.effects.append(SendCommand(p.command_id, s.junction_id, dict(p.signals), p.attempts))
        ctx.audit("COMMAND_RETRY", command_id=p.command_id, attempt=p.attempts, reason=reason)
        return
    s.recent_commands[p.command_id] = "TIMEOUT"
    _trim_recent(s)
    s.pending = None
    ctx.audit("COMMAND_FAILED", command_id=p.command_id, reason=reason, attempts=p.attempts)
    if s.mode == Mode.DEGRADED:
        s.next_probe_at = now + cfg.probe_interval_s
    else:
        _degrade(ctx, f"command {p.command_id} not confirmed: {reason}")


# ============================================================================ startup / recovery
def _on_startup(ctx: _Ctx) -> Result:
    """Persisted physical state is NOT trusted after a (re)start: actual => UNKNOWN, pending commands
    abandoned, manual/degraded state dropped, and the junction is driven to ALL_RED and re-confirmed."""
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    ctx.audit("SYSTEM_STARTED", previous_mode=s.mode, previous_step=s.step, previous_phase=s.phase,
              previous_desired={d.value: x.value for d, x in s.desired_signals.items()},
              had_pending=s.pending.command_id if s.pending else None)
    if s.pending is not None:
        s.recent_commands[s.pending.command_id] = "SUPERSEDED"
        ctx.audit("COMMAND_ABANDONED_ON_RESTART", command_id=s.pending.command_id)
        s.pending = None
    s.actual_signals = {d: SignalState.UNKNOWN for d in cfg.directions}
    for k in list(s.device_status):
        if k.startswith(CONTROLLER):
            s.device_status[k] = DeviceStatus.UNKNOWN
    s.device_status[CONTROLLER] = DeviceStatus.UNKNOWN
    if s.mode != Mode.AUTOMATIC:
        _set_mode(ctx, Mode.AUTOMATIC, "restart: previous mode is not carried over")
    s.degraded_reason, s.next_probe_at, s.emergency_phase = None, None, None
    s.step, s.step_started_at, s.target_phase = Step.ALL_RED, now, None
    _set_desired(ctx, signals_for(cfg, Step.ALL_RED, None), "RECOVERY")
    return Result(ResultKind.OK, "STARTED")


# ============================================================================ degraded mode
def _degrade(ctx: _Ctx, reason: str) -> None:
    """Fail safe: stop sequencing, ask for ALL_RED, treat the physical state as UNKNOWN, alert."""
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    if s.mode == Mode.DEGRADED:
        return
    _set_mode(ctx, Mode.DEGRADED, reason)
    s.degraded_reason = reason
    s.emergency_phase, s.target_phase = None, None
    s.actual_signals = {d: SignalState.UNKNOWN for d in cfg.directions}
    s.step, s.step_started_at = Step.ALL_RED, now
    can_send = not _signal_devices_down(s)
    _set_desired(ctx, signals_for(cfg, Step.ALL_RED, None), "DEGRADED", send=can_send)
    s.next_probe_at = None if (s.pending is not None or not can_send) else now + cfg.probe_interval_s


def _probe(ctx: _Ctx) -> None:
    ctx.s.next_probe_at = None
    _set_desired(ctx, signals_for(ctx.cfg, Step.ALL_RED, None), "PROBE")


# ============================================================================ control loop
def _advance(ctx: _Ctx) -> None:
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    _expire(ctx)

    if s.mode == Mode.DEGRADED:
        if s.pending is None and s.next_probe_at is not None and now >= s.next_probe_at \
                and not _signal_devices_down(s):
            _probe(ctx)
        return

    if active_emergencies(s):
        if s.mode != Mode.EMERGENCY:
            if s.mode == Mode.MANUAL:
                ctx.audit("MANUAL_OVERRIDE_PREEMPTED", by="emergency")
            _set_mode(ctx, Mode.EMERGENCY, "emergency vehicle detected")
    else:
        if s.mode == Mode.EMERGENCY:
            ctx.audit("EMERGENCY_CLEARED")
            _set_mode(ctx, Mode.AUTOMATIC, "emergency cleared")
        elif s.mode == Mode.MANUAL and s.manual_expires_at is not None and now >= s.manual_expires_at:
            ctx.audit("MANUAL_OVERRIDE_EXPIRED")
            _set_mode(ctx, Mode.AUTOMATIC, "manual lease expired")

    target, bypass_min_green = _target(ctx)
    _step(ctx, target, bypass_min_green)


def _target(ctx: _Ctx):
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    if s.mode == Mode.EMERGENCY:
        t = _emergency_target(ctx)
        s.emergency_phase = t
        return t, True            # emergencies skip min_green but NEVER skip yellow / all-red
    if s.mode == Mode.MANUAL:
        return s.manual_phase, False
    if s.step == Step.GREEN:
        return scheduler.decide(s, cfg, now), False
    return s.target_phase, False


def _emergency_target(ctx: _Ctx) -> Optional[str]:
    """Policy for competing emergencies: serve the phase of the OLDEST (server-time) emergency. While
    the served phase still has an emergency, keep it GREEN (they share it) - but at most max_green_s
    when a conflicting emergency is waiting, so neither side can be held forever."""
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    phases = []
    for d, _vid in active_emergencies(s):
        p = cfg.phase_of(d)
        if p not in phases:
            phases.append(p)
    if not phases:
        return None
    if s.step == Step.GREEN and s.phase in phases:
        others = [p for p in phases if p != s.phase]
        if others and now - s.step_started_at >= cfg.max_green_s:
            return others[0]
        return s.phase
    # Mid-transition: stay committed to the phase we already started moving to (otherwise the
    # "oldest emergency" rule would flip the target back during YELLOW and the hand-over never completes).
    if s.step != Step.GREEN and s.target_phase in phases:
        return s.target_phase
    return phases[0]


def _confirmed(s: JunctionState) -> bool:
    return s.pending is None and s.actual_signals == s.desired_signals


def _step(ctx: _Ctx, target: Optional[str], bypass_min_green: bool) -> None:
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    elapsed = now - s.step_started_at

    if s.step == Step.GREEN:
        if s.phase is None or target is None or target == s.phase:
            return
        if not bypass_min_green and elapsed < cfg.min_green_s:
            return
        # Leaving GREEN is always safe even if the GREEN command was never confirmed (YELLOW is "more red").
        s.target_phase, s.step, s.step_started_at = target, Step.YELLOW, now
        ctx.audit("SIGNAL_TRANSITION_STARTED", from_phase=s.phase, to_phase=target, mode=s.mode.value)
        _set_desired(ctx, signals_for(cfg, Step.YELLOW, s.phase), "TRANSITION")

    elif s.step == Step.YELLOW:
        if not _confirmed(s) or elapsed < cfg.yellow_s:
            return
        s.step, s.step_started_at = Step.ALL_RED, now
        _set_desired(ctx, signals_for(cfg, Step.ALL_RED, None), "TRANSITION")

    else:  # ALL_RED
        if not _confirmed(s) or elapsed < cfg.all_red_s:      # GREEN only after ALL_RED is CONFIRMED
            return
        nxt = target or s.target_phase or scheduler.choose_best(s, cfg, now)
        s.phase, s.step, s.target_phase, s.step_started_at = nxt, Step.GREEN, None, now
        _set_desired(ctx, signals_for(cfg, Step.GREEN, nxt), "TRANSITION")


def _set_desired(ctx: _Ctx, signals: dict, purpose: str, send: bool = True) -> None:
    """The ONLY place desired signals change and commands are created. Every plan is safety-checked."""
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    safety.assert_safe_signals(cfg, signals)
    safety.assert_safe_transition(cfg, s.desired_signals, signals)

    if s.pending is not None:
        s.recent_commands[s.pending.command_id] = "SUPERSEDED"
        ctx.audit("COMMAND_SUPERSEDED", command_id=s.pending.command_id, by=purpose)
        _trim_recent(s)
        s.pending = None

    prev = s.desired_signals
    s.desired_signals = dict(signals)
    cid = None
    if send:
        s.command_seq += 1
        cid = f"cmd-{s.junction_id}-{s.command_seq}"
    for d in cfg.directions:
        if prev.get(d) != signals[d]:
            ctx.audit("SIGNAL_REQUESTED", direction=d, previous_state=prev.get(d), new_state=signals[d],
                      command_id=cid, purpose=purpose, sent=send)
    if send:
        s.pending = PendingCommand(cid, dict(signals), now, 1, purpose)
        ctx.effects.append(SendCommand(cid, s.junction_id, dict(signals), 1))


def _set_mode(ctx: _Ctx, new: Mode, reason: str) -> None:
    s = ctx.s
    if s.mode == new:
        return
    prev = s.mode
    if prev == Mode.MANUAL:
        s.manual_phase = s.manual_expires_at = s.manual_by = None
    if prev == Mode.EMERGENCY:
        s.emergency_phase = None
    s.mode = new
    ctx.audit("MODE_CHANGE", previous_state=prev, new_state=new, reason=reason)


def _trim_recent(s: JunctionState) -> None:
    while len(s.recent_commands) > RECENT_COMMANDS_MAX:
        del s.recent_commands[next(iter(s.recent_commands))]


def _expire(ctx: _Ctx) -> None:
    s, cfg, now = ctx.s, ctx.cfg, ctx.now
    for d, q in s.queues.items():
        for vid, veh in list(q.items()):
            age = now - veh.arrived_at
            if veh.vehicle_type == VehicleType.EMERGENCY and age > cfg.emergency_ttl_s:
                del q[vid]
                s.tombstones[d][vid] = max(veh.sensor_ts, s.tombstones[d].get(vid, 0.0))
                ctx.audit("EMERGENCY_EXPIRED", direction=d, vehicle_id=vid, age_s=round(age, 1))
            elif age > cfg.vehicle_max_age_s:
                del q[vid]
                s.tombstones[d][vid] = max(veh.sensor_ts, s.tombstones[d].get(vid, 0.0))
                ctx.audit("VEHICLE_EXPIRED", direction=d, vehicle_id=vid, age_s=round(age, 1))
    for d, tomb in s.tombstones.items():
        for vid, ts in list(tomb.items()):
            if now - ts > cfg.stale_event_s:   # an arrival older than this would be rejected as stale anyway
                del tomb[vid]
    for eid, t in list(s.processed_events.items()):
        if now - t > cfg.processed_retention_s:
            del s.processed_events[eid]
