"""Test harness: fake clock + simulated controller + an INDEPENDENT safety checker.

The checker deliberately does not reuse traffic.domain.safety, so it validates the engine rather than
echoing it. It runs after EVERY input."""
from datetime import datetime, timezone

from traffic.domain import (
    AuditRecord, Command, ControllerAck, Direction, JunctionConfig, SendCommand, SensorEvent, SignalState,
    Startup, StatusEvent, Tick, handle, initial_state, parse_ts,
)

BASE = parse_ts("2026-10-05T10:00:00Z")
RED, YELLOW, GREEN, UNKNOWN = SignalState.RED, SignalState.YELLOW, SignalState.GREEN, SignalState.UNKNOWN
N, S, E, W = Direction.NORTH, Direction.SOUTH, Direction.EAST, Direction.WEST


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Harness:
    def __init__(self, cfg=None, auto_ack=True, boot=True):
        self.cfg = cfg or JunctionConfig("A")
        self.now = BASE
        self.state = initial_state(self.cfg, self.now)
        self.auto_ack = auto_ack
        self.sent, self.audit, self.timeline = [], [], []   # timeline: (time, shape)
        self._prev_desired = dict(self.state.desired_signals)
        self._n = 0
        if boot:
            self.feed(Startup())

    # ---- driving
    def feed(self, inp, settle=True):
        r = self._raw(inp)
        if settle:
            self.settle()
        return r.result

    def _raw(self, inp):
        before = self.state
        r = handle(self.state, self.cfg, inp, self.now)
        self.state = r.state
        for e in r.effects:
            (self.sent if isinstance(e, SendCommand) else self.audit).append(e)
        self._check(before)
        return r

    def settle(self):
        guard = 0
        while self.auto_ack and self.state.pending is not None and guard < 20:
            self._raw(ControllerAck(self.state.pending.command_id, "ACK"))
            guard += 1

    def run(self, seconds, step=1.0):
        t = 0.0
        while t < seconds - 1e-9:
            self.now += step
            t += step
            self.feed(Tick())

    def run_until(self, pred, max_s=300):
        for _ in range(int(max_s)):
            if pred(self):
                return True
            self.run(1)
        return pred(self)

    def ack(self, **kw):
        cid = kw.pop("command_id", None)
        if cid is None:
            cid = self.state.pending.command_id
        return self._raw(ControllerAck(cid, kw.pop("status", "ACK"), **kw)).result

    # ---- event helpers
    def _id(self, prefix):
        self._n += 1
        return f"{prefix}-{self._n}"

    def arrive(self, direction, vtype="TRUCK", vid=None, eid=None, seq=None, ts=None, settle=True):
        return self.feed(SensorEvent(
            event_id=eid or self._id("evt"), junction_id=self.cfg.junction_id,
            direction=direction.value if hasattr(direction, "value") else direction,
            event_type="VEHICLE_ARRIVED", vehicle_id=vid or self._id("VH"), vehicle_type=vtype,
            sequence_no=seq, timestamp=iso(self.now if ts is None else ts)), settle)

    def clear(self, direction, vid, eid=None, seq=None, ts=None):
        return self.feed(SensorEvent(
            event_id=eid or self._id("evt"), junction_id=self.cfg.junction_id,
            direction=direction.value if hasattr(direction, "value") else direction,
            event_type="VEHICLE_CLEARED", vehicle_id=vid, sequence_no=seq,
            timestamp=iso(self.now if ts is None else ts)))

    def manual(self, direction, who="admin1"):
        return self.feed(Command("MANUAL_GREEN_REQUEST", direction.value, who))

    def auto(self, who="admin1"):
        return self.feed(Command("RETURN_TO_AUTOMATIC", None, who))

    def status(self, dtype, st, direction=None, eid=None):
        return self.feed(StatusEvent(eid or self._id("status"), dtype, direction, st, iso(self.now)))

    # ---- views
    def q(self):
        return {d.value: len(v) for d, v in self.state.queues.items()}

    def desired(self):
        return {d: s for d, s in self.state.desired_signals.items()}

    def shape(self, signals=None):
        sig = signals or self.state.desired_signals
        green = sorted(d.value for d, s in sig.items() if s == GREEN)
        yellow = sorted(d.value for d, s in sig.items() if s == YELLOW)
        if green:
            return "GREEN:" + "+".join(green)
        if yellow:
            return "YELLOW:" + "+".join(yellow)
        return "ALL_RED"

    def shapes(self):
        out = []
        for _, sh in self.timeline:
            if not out or out[-1] != sh:
                out.append(sh)
        return out

    def audit_types(self):
        return [a.event_type for a in self.audit]

    def green_phase_reached(self, phase_dirs):
        return lambda h: h.state.step.value == "GREEN" and h.state.phase == phase_dirs and h.state.actual_signals == h.state.desired_signals

    # ---- independent safety checker
    def _check(self, before):
        cfg, st = self.cfg, self.state
        d = st.desired_signals
        phase_of = cfg.phase_of
        active = {phase_of(x) for x, s in d.items() if s != RED}
        assert len(active) <= 1, f"CONFLICTING non-RED phases desired: {d}"
        newly_green = [x for x, s in d.items() if s == GREEN and self._prev_desired[x] != GREEN]
        for x in newly_green:
            for o in cfg.directions:
                if phase_of(o) != phase_of(x):
                    assert self._prev_desired[o] == RED, f"GREEN {x} entered while {o} was {self._prev_desired[o]}"
            # GREEN only ever follows a CONFIRMED all-red
            assert all(a == RED for a in st.actual_signals.values()), \
                f"GREEN requested while controller-confirmed state was {st.actual_signals}"
        assert all(len(q) >= 0 for q in st.queues.values())
        if d != self._prev_desired or not self.timeline:
            self.timeline.append((self.now, self.shape()))
        self._prev_desired = dict(d)
