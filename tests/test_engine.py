import copy
import random

import pytest

from traffic.domain import (
    Command, ControllerAck, JunctionConfig, Mode, ResultKind, SensorEvent, Startup, Step, Tick, VehicleType,
    SafetyViolation, assert_safe_signals, assert_safe_transition, compute_alerts, handle, status_view,
)
from traffic.domain import scheduler
from harness import BASE, E, GREEN, N, RED, S, UNKNOWN, W, YELLOW, Harness, iso

NS, EW = "NORTH_SOUTH", "EAST_WEST"


def green_ns(h=None, **kw):
    """Boot and run until NORTH_SOUTH has a confirmed GREEN."""
    h = h or Harness(**kw)
    assert h.run_until(lambda x: x.state.step == Step.GREEN and x.state.phase == NS)
    return h


# =========================================================== safety invariants
class TestSafetyPrimitives:
    cfg = JunctionConfig("A")

    def plan(self, **kw):
        base = {N: RED, S: RED, E: RED, W: RED}
        base.update({getattr(__import__("harness"), k): v for k, v in kw.items()})
        return base

    def test_conflicting_greens_rejected(self):
        with pytest.raises(SafetyViolation):
            assert_safe_signals(self.cfg, {N: GREEN, S: GREEN, E: GREEN, W: RED})

    def test_green_plus_conflicting_yellow_rejected(self):
        with pytest.raises(SafetyViolation):
            assert_safe_signals(self.cfg, {N: GREEN, S: GREEN, E: YELLOW, W: RED})

    def test_unknown_desired_rejected(self):
        with pytest.raises(SafetyViolation):
            assert_safe_signals(self.cfg, {N: UNKNOWN, S: RED, E: RED, W: RED})

    def test_direct_green_to_conflicting_green_rejected(self):
        prev = {N: GREEN, S: GREEN, E: RED, W: RED}
        new = {N: RED, S: RED, E: GREEN, W: GREEN}
        with pytest.raises(SafetyViolation):
            assert_safe_transition(self.cfg, prev, new)

    def test_yellow_to_conflicting_green_rejected(self):
        prev = {N: YELLOW, S: YELLOW, E: RED, W: RED}
        with pytest.raises(SafetyViolation):
            assert_safe_transition(self.cfg, prev, {N: RED, S: RED, E: GREEN, W: GREEN})

    def test_all_red_to_green_allowed(self):
        assert_safe_transition(self.cfg, {d: RED for d in (N, S, E, W)}, {N: RED, S: RED, E: GREEN, W: GREEN})


# =========================================================== state machine
class TestStateMachine:
    def test_startup_goes_all_red_then_green(self):
        h = Harness()
        assert h.shape() == "ALL_RED"                 # first command is ALL_RED, never GREEN
        assert h.state.actual_signals == h.state.desired_signals
        h.run(1)
        assert h.shape() == "ALL_RED"                 # all_red_s (2s) not yet elapsed
        h.run(1)
        assert h.shape().startswith("GREEN")

    def test_full_safe_sequence_and_minimum_durations(self):
        h = green_ns()
        h.arrive(E, "TRUCK")
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST")
        assert h.shapes()[-4:] == ["GREEN:NORTH+SOUTH", "YELLOW:NORTH+SOUTH", "ALL_RED", "GREEN:EAST+WEST"]
        tl = h.timeline
        # durations of the last yellow and all-red
        y = next(i for i in range(len(tl) - 1, -1, -1) if tl[i][1] == "YELLOW:NORTH+SOUTH")
        assert tl[y + 1][0] - tl[y][0] >= 5          # YELLOW >= 5s
        assert tl[y + 2][0] - tl[y + 1][0] >= 2      # ALL_RED >= 2s

    def test_min_green_respected_for_normal_traffic(self):
        h = green_ns()
        started = h.now
        h.arrive(E, "TRUCK")                          # NS has no demand: wants to switch right away...
        h.run(8)
        assert h.shape() == "GREEN:NORTH+SOUTH"       # ...but min_green (10s) has not elapsed
        assert h.now - started < 10

    def test_no_switch_when_nobody_waiting_on_other_side(self):
        h = green_ns()
        for _ in range(5):
            h.arrive(N)
        h.run(200)
        assert h.shape() == "GREEN:NORTH+SOUTH"
        assert h.shapes().count("YELLOW:NORTH+SOUTH") == 0

    def test_state_is_not_mutated_and_engine_is_deterministic(self):
        h = green_ns()
        snapshot = copy.deepcopy(h.state)
        ev = SensorEvent("evt-x", "A", "EAST", "VEHICLE_ARRIVED", "VH-9", "TRUCK", 1, iso(h.now))
        r1 = handle(h.state, h.cfg, ev, h.now)
        r2 = handle(h.state, h.cfg, ev, h.now)
        assert h.state == snapshot                    # input untouched
        assert r1.state == r2.state and r1.effects == r2.effects


# =========================================================== scheduling
class TestScheduling:
    def test_custom_vehicle_weights_change_phase_selection(self):
        cfg = JunctionConfig("A", vehicle_weights={
            "TRUCK": 1,
            "FORKLIFT": 2,
            "EMPLOYEE_VEHICLE": 20,
            "EMERGENCY": 100,
        })
        h = green_ns(cfg=cfg)
        h.arrive(N, "TRUCK"); h.arrive(N, "TRUCK")                              # NS score 2
        h.arrive(E, "EMPLOYEE_VEHICLE")                                         # EW score 20
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 30)

    def test_partial_priority_overrides_keep_defaults_for_other_types(self):
        cfg = JunctionConfig("A", vehicle_weights={VehicleType.TRUCK: 7})
        assert cfg.vehicle_weights == {
            VehicleType.EMERGENCY: 100.0,
            VehicleType.TRUCK: 7.0,
            VehicleType.FORKLIFT: 2.0,
            VehicleType.EMPLOYEE_VEHICLE: 1.0,
        }

    @pytest.mark.parametrize("weights", [
        {"BOAT": 2},
        {"TRUCK": -1},
        {"TRUCK": float("nan")},
        {"TRUCK": float("inf")},
        {"TRUCK": True},
        {"TRUCK": "high"},
    ])
    def test_invalid_vehicle_priority_configuration_is_rejected(self, weights):
        with pytest.raises(ValueError, match="priority weight|unknown vehicle type"):
            JunctionConfig("A", vehicle_weights=weights)

    def test_truck_and_forklift_beat_employee_traffic(self):
        h = green_ns()
        h.arrive(N, "EMPLOYEE_VEHICLE"); h.arrive(N, "EMPLOYEE_VEHICLE")      # NS score 2
        h.arrive(E, "TRUCK"); h.arrive(W, "FORKLIFT")                          # EW score 5
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 60)

    def test_higher_priority_type_wins_over_equal_count(self):
        h = green_ns()
        h.arrive(N, "TRUCK")                                                   # 3 (x1.25 = 3.75)
        h.arrive(E, "EMPLOYEE_VEHICLE"); h.arrive(E, "EMPLOYEE_VEHICLE")       # 2
        h.run(25)
        assert h.shape() == "GREEN:NORTH+SOUTH"                                # no needless switch

    def test_larger_queue_can_outweigh_priority(self):
        h = green_ns()
        h.arrive(N, "TRUCK")                                                   # 3
        for _ in range(5):
            h.arrive(E, "EMPLOYEE_VEHICLE")                                    # 5 > 3.75
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 60)

    def test_starvation_protection(self):
        h = green_ns()
        for _ in range(12):
            h.arrive(N, "TRUCK")                                               # heavy NS demand (36)
        arrived = h.now
        h.arrive(E, "EMPLOYEE_VEHICLE")                                        # one lonely car
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 200)
        assert h.now - arrived <= h.cfg.max_green_s + h.cfg.yellow_s + h.cfg.all_red_s + 3

    def test_sensor_failure_falls_back_to_fixed_time(self):
        h = green_ns()
        h.status("SENSOR", "OFFLINE", "EAST")
        assert any(a.code == "SENSOR_FAILURE" for a in compute_alerts(h.state, h.cfg, h.now))
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 80)       # served although queue shows 0

    def test_scheduler_is_pure_function_of_state(self):
        h = green_ns()
        h.arrive(E, "TRUCK")
        a = scheduler.decide(h.state, h.cfg, h.now + 20)
        b = scheduler.decide(h.state, h.cfg, h.now + 20)
        assert a == b == EW


# =========================================================== sensor events
class TestSensorEvents:
    def test_duplicate_event_id_changes_queue_once(self):
        h = green_ns()
        assert h.arrive(N, eid="evt-1", vid="VH-1").kind == ResultKind.OK
        assert h.arrive(N, eid="evt-1", vid="VH-1").kind == ResultKind.DUPLICATE
        assert h.arrive(N, eid="evt-1", vid="VH-1").kind == ResultKind.DUPLICATE
        assert h.q()["NORTH"] == 1
        assert "DUPLICATE_EVENT_REJECTED" in h.audit_types()

    def test_same_vehicle_new_event_id_not_double_counted(self):
        h = green_ns()
        h.arrive(N, vid="VH-1")
        assert h.arrive(N, vid="VH-1").code == "ALREADY_QUEUED"
        assert h.q()["NORTH"] == 1

    def test_clear_reduces_queue_and_never_goes_negative(self):
        h = green_ns()
        h.arrive(N, vid="VH-1"); h.arrive(N, vid="VH-2")
        assert h.clear(N, "VH-1").kind == ResultKind.OK and h.q()["NORTH"] == 1
        assert h.clear(N, "VH-1").kind == ResultKind.IGNORED                   # repeat clear
        assert h.clear(N, "VH-2").kind == ResultKind.OK
        assert h.clear(N, "VH-2").kind == ResultKind.IGNORED
        assert h.q()["NORTH"] == 0
        assert h.clear(N, "VH-NEVER").code == "UNMATCHED_CLEAR"
        assert h.q()["NORTH"] == 0

    def test_late_arrival_after_clear_is_not_resurrected(self):
        h = green_ns()
        t_arrive, t_clear = h.now - 20, h.now - 10
        h.clear(N, "VH-7", ts=t_clear)                                         # clear overtakes the arrival
        assert h.arrive(N, vid="VH-7", ts=t_arrive).code == "LATE_ARRIVAL"
        assert h.q()["NORTH"] == 0

    def test_vehicle_can_return_later(self):
        h = green_ns()
        h.arrive(N, vid="VH-7", ts=h.now - 5)
        h.clear(N, "VH-7", ts=h.now - 4)
        h.run(10)
        assert h.arrive(N, vid="VH-7").kind == ResultKind.OK                   # newer sensor ts => new visit
        assert h.q()["NORTH"] == 1

    @pytest.mark.parametrize("kw,code", [
        (dict(direction="UP"), "INVALID_DIRECTION"),
        (dict(direction=None), "INVALID_DIRECTION"),
        (dict(vtype="BICYCLE"), "UNKNOWN_VEHICLE_TYPE"),
        (dict(vtype=None), "UNKNOWN_VEHICLE_TYPE"),
        (dict(eid=""), "INVALID_EVENT_ID"),
        (dict(vid=""), "INVALID_VEHICLE_ID"),
        (dict(ts="yesterday"), "INVALID_TIMESTAMP"),
        (dict(seq=-5), "INVALID_SEQUENCE_NO"),
        (dict(seq="12"), "INVALID_SEQUENCE_NO"),
    ])
    def test_invalid_events_rejected_without_state_change(self, kw, code):
        h = green_ns()
        before = copy.deepcopy(h.state.queues)
        ev = dict(event_id="evt-bad", junction_id="A", direction="NORTH", event_type="VEHICLE_ARRIVED",
                  vehicle_id="VH-1", vehicle_type="TRUCK", sequence_no=1, timestamp=iso(h.now))
        ev.update({{"direction": "direction", "vtype": "vehicle_type", "eid": "event_id", "vid": "vehicle_id",
                    "ts": "timestamp", "seq": "sequence_no"}[k]: v for k, v in kw.items()})
        r = h.feed(SensorEvent(**ev))
        assert r.kind == ResultKind.INVALID and r.code == code
        assert h.state.queues == before
        assert "SENSOR_EVENT_REJECTED" in h.audit_types()

    def test_unknown_junction_and_event_type(self):
        h = green_ns()
        assert h.feed(SensorEvent("e1", "Z", "NORTH", "VEHICLE_ARRIVED", "V", "TRUCK", 1, iso(h.now))).code == "UNKNOWN_JUNCTION"
        assert h.feed(SensorEvent("e2", "A", "NORTH", "EXPLODED", "V", "TRUCK", 1, iso(h.now))).code == "INVALID_EVENT_TYPE"

    def test_material_carrier_alias_maps_to_truck(self):
        h = green_ns()
        assert h.arrive(N, "material_carrier").kind == ResultKind.OK
        assert list(h.state.queues[N].values())[0].vehicle_type.value == "TRUCK"

    def test_stale_and_future_timestamps(self):
        h = green_ns()
        assert h.arrive(N, ts=h.now - 600).code == "STALE_EVENT"
        assert h.arrive(N, ts=h.now + 3600).code == "TIMESTAMP_IN_FUTURE"
        assert h.q()["NORTH"] == 0

    def test_out_of_order_sequence_is_processed_but_flagged(self):
        h = green_ns()
        h.arrive(N, seq=1501); h.arrive(N, seq=1503)
        r = h.arrive(N, seq=1502)
        assert r.kind == ResultKind.OK and r.message == "OUT_OF_ORDER"
        assert h.q()["NORTH"] == 3
        notes = [a.detail.get("seq_note") for a in h.audit if a.event_type == "VEHICLE_DETECTED"]
        assert "SEQUENCE_GAP" in notes and "OUT_OF_ORDER" in notes

    def test_waiting_time_uses_server_time_not_sensor_time(self):
        h = green_ns()
        h.arrive(N, vid="V1", ts=h.now - 200)                                  # sensor claims 200s ago
        assert h.state.queues[N]["V1"].arrived_at == h.now


# =========================================================== emergency
class TestEmergency:
    def test_preemption_starts_immediately_but_never_skips_sequence(self):
        h = green_ns()
        h.run(1)                                                               # well inside min_green
        r = h.arrive(E, "EMERGENCY", vid="AMB-1")
        assert r.kind == ResultKind.OK
        assert h.state.mode == Mode.EMERGENCY
        assert h.shape() == "YELLOW:NORTH+SOUTH"                               # started NOW...
        assert h.state.desired_signals[E] == RED                               # ...but EAST is NOT green yet
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 30)
        assert h.shapes()[-4:] == ["GREEN:NORTH+SOUTH", "YELLOW:NORTH+SOUTH", "ALL_RED", "GREEN:EAST+WEST"]

    def test_emergency_in_already_green_phase_holds_green(self):
        h = green_ns()
        h.arrive(N, "EMERGENCY", vid="AMB-1")
        h.arrive(E, "TRUCK")
        h.run(40)
        assert h.shape() == "GREEN:NORTH+SOUTH" and h.state.mode == Mode.EMERGENCY

    def test_repeated_emergency_event_is_idempotent(self):
        h = green_ns()
        assert h.arrive(E, "EMERGENCY", vid="AMB-1", eid="evt-E").kind == ResultKind.OK
        assert h.arrive(E, "EMERGENCY", vid="AMB-1", eid="evt-E").kind == ResultKind.DUPLICATE
        assert h.arrive(E, "EMERGENCY", vid="AMB-1", eid="evt-E2").code == "ALREADY_QUEUED"
        assert h.q()["EAST"] == 1

    def test_emergency_cleared_returns_to_automatic(self):
        h = green_ns()
        h.arrive(E, "EMERGENCY", vid="AMB-1")
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 30)
        h.clear(E, "AMB-1")
        assert h.state.mode == Mode.AUTOMATIC
        assert "EMERGENCY_CLEARED" in h.audit_types()

    def test_stale_emergency_expires(self):
        h = green_ns()
        h.arrive(E, "EMERGENCY", vid="AMB-1")
        h.run(h.cfg.emergency_ttl_s + 2)
        assert h.state.mode == Mode.AUTOMATIC and h.q()["EAST"] == 0
        assert "EMERGENCY_EXPIRED" in h.audit_types()

    def test_conflicting_emergencies_first_come_first_served(self):
        h = green_ns()
        h.arrive(E, "EMERGENCY", vid="AMB-E")
        h.run(1)
        h.arrive(N, "EMERGENCY", vid="AMB-N")                                  # conflicting, arrived later
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 30)
        h.run(10)
        assert h.shape() == "GREEN:EAST+WEST"                                  # N waits its turn
        h.clear(E, "AMB-E")
        assert h.state.mode == Mode.EMERGENCY                                  # N still active
        assert h.run_until(lambda x: x.shape() == "GREEN:NORTH+SOUTH", 30)
        h.clear(N, "AMB-N")
        assert h.state.mode == Mode.AUTOMATIC

    def test_conflicting_emergency_not_starved_forever(self):
        h = green_ns()
        h.arrive(E, "EMERGENCY", vid="E1")
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 30)
        h.arrive(N, "EMERGENCY", vid="N1")
        assert h.run_until(lambda x: x.shape() == "GREEN:NORTH+SOUTH", h.cfg.max_green_s + 30)

    def test_emergency_overrides_manual_and_blocks_new_manual(self):
        h = green_ns()
        assert h.manual(W).code == "MANUAL_ACCEPTED"
        h.arrive(N, "EMERGENCY", vid="AMB-1")
        assert h.state.mode == Mode.EMERGENCY and h.state.manual_phase is None
        assert "MANUAL_OVERRIDE_PREEMPTED" in h.audit_types()
        r = h.manual(W)
        assert r.kind == ResultKind.CONFLICT and r.code == "EMERGENCY_ACTIVE"
        assert h.auto().kind == ResultKind.IGNORED


# =========================================================== manual control
class TestManual:
    def test_manual_uses_safe_sequence_and_holds(self):
        h = green_ns()
        assert h.manual(W).kind == ResultKind.OK and h.state.mode == Mode.MANUAL
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 60)
        assert h.shapes()[-4:] == ["GREEN:NORTH+SOUTH", "YELLOW:NORTH+SOUTH", "ALL_RED", "GREEN:EAST+WEST"]
        for _ in range(8):
            h.arrive(N, "TRUCK")
        h.run(120)
        assert h.shape() == "GREEN:EAST+WEST"                                  # manual wins over scheduler

    def test_return_to_automatic_resumes_scheduling(self):
        h = green_ns()
        h.manual(W)
        h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 60)
        for _ in range(4):
            h.arrive(N, "TRUCK")
        assert h.auto().code == "RETURNED_TO_AUTOMATIC" and h.state.mode == Mode.AUTOMATIC
        assert "RETURN_TO_AUTOMATIC" in h.audit_types()
        assert h.run_until(lambda x: x.shape() == "GREEN:NORTH+SOUTH", 120)

    def test_manual_lease_expires(self):
        h = green_ns(cfg=JunctionConfig("A", manual_lease_s=20))
        h.manual(W)
        h.run(25)
        assert h.state.mode == Mode.AUTOMATIC and "MANUAL_OVERRIDE_EXPIRED" in h.audit_types()

    def test_two_admins_last_valid_command_wins_and_both_audited(self):
        h = green_ns()
        h.manual(W, "alice")
        h.manual(N, "bob")
        assert h.state.manual_phase == NS and h.state.manual_by == "bob"
        assert [a.detail["issued_by"] for a in h.audit if a.event_type == "MANUAL_OVERRIDE"] == ["alice", "bob"]

    def test_invalid_commands(self):
        h = green_ns()
        assert h.feed(Command("MANUAL_GREEN_REQUEST", "UP", "a")).code == "INVALID_DIRECTION"
        assert h.feed(Command("SET_SIGNAL_STATE", "NORTH", "a")).code == "UNKNOWN_COMMAND"
        assert h.feed(Command(None)).code == "UNKNOWN_COMMAND"
        assert h.auto().code == "ALREADY_AUTOMATIC"
        assert h.state.mode == Mode.AUTOMATIC


# =========================================================== controller / failure
class TestController:
    def test_green_is_never_requested_without_confirmed_all_red(self):
        h = Harness(auto_ack=False)                                            # controller never answers
        h.run(8)
        assert all(sh == "ALL_RED" for sh in h.shapes())
        assert h.state.desired_signals[N] == RED

    def test_delayed_yellow_and_all_red_acknowledgements_gate_next_green(self):
        h = green_ns()
        h.auto_ack = False
        h.arrive(E, "EMERGENCY", vid="AMB-ACK", settle=False)
        yellow_command = h.state.pending.command_id
        assert h.shape() == "YELLOW:NORTH+SOUTH"

        h.now += h.cfg.yellow_s
        h.ack(command_id=yellow_command)
        all_red_command = h.state.pending.command_id
        assert h.shape() == "ALL_RED"
        h.now += h.cfg.all_red_s
        assert h.shape() == "ALL_RED"
        assert all(signal == RED for signal in h.state.desired_signals.values())

        h.ack(command_id=all_red_command)
        assert h.shape() == "GREEN:EAST+WEST"
        assert h.state.pending is not None
        assert h.state.pending.signals[E] == GREEN

    def test_timeout_retries_same_command_id_then_degrades_safely(self):
        h = Harness(auto_ack=False)
        first = h.state.pending.command_id
        h.run(3); h.run(3)
        assert [s.command_id for s in h.sent] == [first] * 3                    # 1 send + 2 retries, same id
        assert [s.attempt for s in h.sent] == [1, 2, 3]
        h.run(3)
        assert h.state.mode == Mode.DEGRADED
        assert h.state.actual_signals == {d: UNKNOWN for d in h.cfg.directions}
        assert h.state.desired_signals == {d: RED for d in h.cfg.directions}
        codes = {a.code for a in compute_alerts(h.state, h.cfg, h.now)}
        assert {"DEGRADED_MODE", "UNKNOWN_DEVICE_STATE"} <= codes
        assert {"COMMAND_TIMEOUT", "COMMAND_RETRY", "COMMAND_FAILED", "MODE_CHANGE"} <= set(h.audit_types())

    def test_degraded_rejects_manual_and_recovers_when_controller_answers(self):
        h = Harness(auto_ack=False)
        h.run(10)
        assert h.state.mode == Mode.DEGRADED
        assert h.manual(W).code == "JUNCTION_DEGRADED"
        h.ack()                                                                # controller finally replies
        assert h.state.mode == Mode.AUTOMATIC
        h.auto_ack = True
        h.settle()
        assert h.run_until(lambda x: x.shape().startswith("GREEN"), 10)

    def test_degraded_keeps_probing(self):
        h = Harness(auto_ack=False)
        h.run(10)
        n = len(h.sent)
        h.run(h.cfg.ack_timeout_s * 4 + h.cfg.probe_interval_s * 2)
        assert len(h.sent) > n                                                 # probes continue
        assert all(sh == "ALL_RED" for sh in h.shapes())

    def test_controller_offline_event_degrades_without_sending(self):
        h = green_ns()
        sent = len(h.sent)
        h.status("SIGNAL_CONTROLLER", "OFFLINE")
        assert h.state.mode == Mode.DEGRADED and h.shape() == "ALL_RED"
        assert len(h.sent) == sent                                             # nobody to send to
        assert "CONTROLLER_OFFLINE" in h.audit_types()
        status = status_view(h.state, h.cfg, h.now)
        assert status["controller_status"] == "OFFLINE"
        h.status("SIGNAL_CONTROLLER", "ONLINE")                                # reconnect -> probe -> recover
        assert len(h.sent) == sent + 1 and h.state.mode == Mode.AUTOMATIC
        assert h.run_until(lambda x: x.shape().startswith("GREEN"), 10)

    def test_single_signal_head_failure_degrades(self):
        h = green_ns()
        h.status("SIGNAL_CONTROLLER", "OFFLINE", "SOUTH")
        assert h.state.mode == Mode.DEGRADED
        assert any(a.code == "SIGNAL_FAILURE" and a.direction == "SOUTH" for a in compute_alerts(h.state, h.cfg, h.now))

    def test_duplicate_ack_is_ignored(self):
        h = Harness(auto_ack=False)
        cid = h.state.pending.command_id
        assert h.ack().kind == ResultKind.OK
        assert h.ack(command_id=cid).kind == ResultKind.DUPLICATE
        assert h.state.pending is None

    def test_unknown_ack_rejected(self):
        h = Harness(auto_ack=False)
        r = h.ack(command_id="cmd-nope")
        assert r.kind == ResultKind.INVALID and r.code == "UNKNOWN_COMMAND"
        assert h.state.actual_signals[N] == UNKNOWN                            # not assumed

    def test_late_ack_of_superseded_command_does_not_change_actual_state(self):
        h = green_ns()
        h.auto_ack = False
        h.arrive(E, "EMERGENCY", vid="AMB-1")                                  # sends YELLOW, left unacked
        stale = h.state.pending.command_id
        h.status("SIGNAL_CONTROLLER", "OFFLINE")                               # supersedes it
        before = dict(h.state.actual_signals)
        r = h.ack(command_id=stale, actual_signals={"NORTH": "GREEN", "SOUTH": "GREEN", "EAST": "RED", "WEST": "RED"})
        assert r.code == "LATE_ACK" and h.state.actual_signals == before

    def test_mismatching_ack_is_not_trusted(self):
        h = Harness(auto_ack=False)
        bad = {"NORTH": "GREEN", "SOUTH": "RED", "EAST": "RED", "WEST": "RED"}
        h.ack(actual_signals=bad)
        assert h.state.pending is not None                                     # still unconfirmed, retried
        assert "SIGNAL_MISMATCH" in h.audit_types() and "COMMAND_RETRY" in h.audit_types()
        h.ack(actual_signals=bad); h.ack(actual_signals=bad)
        assert h.state.mode == Mode.DEGRADED
        assert all(sh == "ALL_RED" for sh in h.shapes())

    def test_nack_triggers_retry(self):
        h = Harness(auto_ack=False)
        h.ack(status="NACK")
        assert h.state.pending.attempts == 2 and "CONTROLLER_COMMAND_FAILED" in h.audit_types()

    def test_invalid_ack_payloads(self):
        h = Harness(auto_ack=False)
        assert h.feed(ControllerAck(None, "ACK")).code == "INVALID_COMMAND_ID"
        assert h.feed(ControllerAck("x", "MAYBE")).code == "INVALID_STATUS"
        assert h.feed(ControllerAck(h.state.pending.command_id, "ACK", {"NORTH": "PURPLE"})).code == "INVALID_ACTUAL_SIGNALS"


# =========================================================== restart / recovery
class TestRestart:
    def test_restart_during_yellow_with_pending_command(self):
        h = green_ns()
        h.arrive(E, "EMERGENCY", vid="AMB-1")                                  # YELLOW issued, auto-acked
        h.auto_ack = False
        h.run(5)                                                               # yellow elapsed; ALL_RED sent, not acked
        pending = h.state.pending.command_id
        h.arrive(N, "TRUCK", settle=False)
        queues = h.q()
        h.now += 30                                                            # "server down" for 30s
        h.feed(Startup())                                                      # restart
        s = h.state
        assert s.pending is not None and s.pending.command_id != pending
        assert s.pending.purpose == "RECOVERY"
        assert s.actual_signals == {d: UNKNOWN for d in h.cfg.directions}      # old physical state NOT trusted
        assert h.shape() == "ALL_RED" and s.step == Step.ALL_RED
        assert h.q() == queues                                                 # queues survive
        assert "COMMAND_ABANDONED_ON_RESTART" in h.audit_types()
        assert h.shapes()[-1] == "ALL_RED"

    def test_restart_drops_manual_and_degraded_state(self):
        h = green_ns()
        h.manual(W)
        h.feed(Startup())
        assert h.state.mode == Mode.AUTOMATIC and h.state.manual_phase is None

    def test_restart_keeps_active_emergency(self):
        h = green_ns()
        h.arrive(E, "EMERGENCY", vid="AMB-1")
        h.feed(Startup())
        assert h.state.mode == Mode.EMERGENCY                                  # re-derived from persisted queue
        assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 20)

    def test_green_resumes_only_after_recovery_ack(self):
        h = green_ns()
        h.auto_ack = False
        mark = len(h.timeline)
        h.feed(Startup())
        h.run(30)                                                              # controller never confirms ALL_RED
        assert h.state.mode == Mode.DEGRADED
        assert all(sh == "ALL_RED" for _, sh in h.timeline[mark:])            # no GREEN after the restart


# =========================================================== concurrency scenario from the spec
def test_spec_concurrent_sequence_stays_consistent():
    h = green_ns()
    h.auto_ack = False
    h.now += 0.000; r1 = h.arrive(N, "TRUCK", vid="VH-T", settle=False)
    h.now += 0.004; r2 = h.arrive(E, "EMERGENCY", vid="AMB-1", eid="evt-EM", settle=False)
    h.now += 0.004; r3 = h.manual(W)
    h.now += 0.004; r4 = h.arrive(E, "EMERGENCY", vid="AMB-1", eid="evt-EM", settle=False)
    h.now += 0.005; r5 = h.ack()
    assert (r1.kind, r2.kind, r3.kind, r4.kind, r5.kind) == (
        ResultKind.OK, ResultKind.OK, ResultKind.CONFLICT, ResultKind.DUPLICATE, ResultKind.OK)
    assert h.state.mode == Mode.EMERGENCY
    assert h.q() == {"NORTH": 1, "SOUTH": 0, "EAST": 1, "WEST": 0}
    h.auto_ack = True
    assert h.run_until(lambda x: x.shape() == "GREEN:EAST+WEST", 30)


# =========================================================== read model
def test_status_view_shape():
    h = green_ns()
    h.arrive(E, "EMERGENCY", vid="AMB-1")
    v = status_view(h.state, h.cfg, h.now)
    for key in ("junction_id", "mode", "phase", "controller_status", "desired_signals", "actual_signals", "queues",
                "pending_command", "emergency", "manual", "alerts"):
        assert key in v
    assert v["mode"] == "EMERGENCY" and v["emergency"]["vehicles"][0]["direction"] == "EAST"
    assert v["desired_signals"] != v["actual_signals"] or v["pending_command"] is None


def test_config_allows_other_junctions_without_engine_changes():
    from traffic.domain import Direction
    cfg = JunctionConfig("B", phases={"NS": (Direction.NORTH, Direction.SOUTH), "EW": (Direction.EAST, Direction.WEST)},
                         min_green_s=5)
    h = Harness(cfg)
    h.run(3)
    assert h.shape().startswith("GREEN") and h.state.junction_id == "B"
    with pytest.raises(ValueError):
        JunctionConfig("X", phases={"P": (Direction.NORTH,), "Q": (Direction.NORTH,)})


# =========================================================== property / fuzz test
def run_fuzz(seed, steps=400):
    """Random arrivals, clears, emergencies, manual commands, status flaps, dropped/duplicate/bad ACKs,
    restarts and time jumps. The harness checks, after EVERY input, that conflicting phases are never
    non-RED together, no GREEN is entered from a non-RED conflicting state, and GREEN is only requested
    while the controller-CONFIRMED state is ALL_RED."""
    rnd = random.Random(seed)
    h = Harness(auto_ack=False)
    live = []
    for _ in range(400):
        a = rnd.random()
        d = rnd.choice([N, S, E, W])
        if a < 0.20:
            vt = rnd.choice(["TRUCK", "FORKLIFT", "EMPLOYEE_VEHICLE", "EMERGENCY", "EMERGENCY", "BOGUS"])
            vid = f"V{rnd.randint(1, 15)}"
            r = h.arrive(d, vt, vid=vid, eid=rnd.choice([None, None, f"dup{rnd.randint(1, 5)}"]),
                         ts=h.now - rnd.choice([0, 0, 5, 400]), settle=False)
            live.append((d, vid))
        elif a < 0.30 and live:
            dd, vid = rnd.choice(live)
            h.clear(dd, vid)
        elif a < 0.38:
            h.feed(Command(rnd.choice(["MANUAL_GREEN_REQUEST", "RETURN_TO_AUTOMATIC", "NOPE"]), d.value, "fz"))
        elif a < 0.42:
            h.status("SIGNAL_CONTROLLER", rnd.choice(["OFFLINE", "ONLINE", "UNKNOWN", "WARNING"]),
                     rnd.choice([None, "SOUTH"]))
        elif a < 0.44:
            h.status("SENSOR", rnd.choice(["OFFLINE", "ONLINE"]), d.value)
        elif a < 0.47:
            h.feed(Startup())
        elif a < 0.80 and h.state.pending is not None:                         # controller behaviour
            kind = rnd.random()
            p = h.state.pending
            if kind < 0.6:
                h.ack()
            elif kind < 0.7:
                h.ack(status=rnd.choice(["NACK", "FAILED"]))
            elif kind < 0.85:
                h.ack(actual_signals={x.value: rnd.choice(["RED", "GREEN", "YELLOW"]) for x in h.cfg.directions})
            else:
                h.ack(command_id=rnd.choice(["cmd-A-1", "cmd-zzz", p.command_id]))
        else:
            h.now += rnd.choice([0.5, 1, 1, 2, 5, 31])
            h.feed(Tick())
        assert all(len(q) >= 0 for q in h.state.queues.values())
        assert not (h.state.mode == Mode.DEGRADED and h.state.desired_signals != {x: RED for x in h.cfg.directions})
    return h


@pytest.mark.parametrize("seed", range(25))
def test_fuzz_safety_invariants_hold_under_random_inputs(seed):
    run_fuzz(seed)


def test_fuzz_is_not_vacuous():
    """Guard: across all seeds the fuzz must actually reach the interesting states."""
    shapes, audits, modes = set(), set(), set()
    for seed in range(25):
        h = run_fuzz(seed)
        shapes |= {sh.split(":")[0] for sh in h.shapes()}
        audits |= set(h.audit_types())
        modes |= {a.new_state for a in h.audit if a.event_type == "MODE_CHANGE"}
    assert {"GREEN", "YELLOW", "ALL_RED"} <= shapes
    assert {"EMERGENCY", "MANUAL", "DEGRADED", "AUTOMATIC"} <= modes
    assert {"SIGNAL_TRANSITION_STARTED", "RECOVERED_FROM_DEGRADED", "COMMAND_RETRY", "SIGNAL_MISMATCH",
            "SYSTEM_STARTED", "DUPLICATE_EVENT_REJECTED", "EMERGENCY_EXPIRED"} <= audits, audits
