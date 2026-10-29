import sqlite3

import pytest

from traffic.domain import (
    Command,
    ControllerAck,
    Direction,
    JunctionConfig,
    SensorEvent,
    SignalState,
    Tick,
    handle,
    parse_ts,
)
from traffic.persistence import SQLiteRepository
from harness import E, iso


BASE = parse_ts("2026-10-05T10:00:00Z")


def _apply(repository, state, config, inp, now):
    result = handle(state, config, inp, now)
    repository.save_result(result)
    return result


def test_state_processed_events_audit_and_pending_command_round_trip(tmp_path):
    config = JunctionConfig("A")
    database = tmp_path / "traffic.db"
    event = SensorEvent(
        event_id="event-1",
        junction_id="A",
        direction="EAST",
        event_type="VEHICLE_ARRIVED",
        vehicle_id="truck-1",
        vehicle_type="TRUCK",
        sequence_no=7,
        timestamp=iso(BASE),
    )

    with SQLiteRepository(database) as repository:
        started = repository.recover(config, BASE)
        result = _apply(repository, started.state, config, event, BASE + 1)

        restored = repository.load_state("A")
        assert restored == result.state
        assert restored.processed_events == {"event-1": BASE + 1}
        assert "VEHICLE_DETECTED" in [record.event_type for record in repository.list_audit("A")]
        assert repository.pending_commands("A")[0].command_id == restored.pending.command_id
    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1


def test_recovery_uses_startup_and_does_not_restore_actual_signals(tmp_path):
    config = JunctionConfig("A")
    database = tmp_path / "traffic.db"
    with SQLiteRepository(database) as repository:
        first = repository.recover(config, BASE)
        red_ack = handle(
            first.state,
            config,
            ControllerAck(first.state.pending.command_id, "ACK"),
            BASE + config.all_red_s,
        )
        repository.save_result(red_ack)
        green_ack = handle(
            red_ack.state,
            config,
            ControllerAck(red_ack.state.pending.command_id, "ACK"),
            BASE + config.all_red_s,
        )
        repository.save_result(green_ack)
        assert any(signal == SignalState.GREEN for signal in green_ack.state.actual_signals.values())

        queued = handle(
            green_ack.state,
            config,
            SensorEvent(
                event_id="queued-before-restart",
                junction_id="A",
                direction="NORTH",
                event_type="VEHICLE_ARRIVED",
                vehicle_id="truck-before-restart",
                vehicle_type="TRUCK",
                timestamp=iso(BASE + 3),
            ),
            BASE + 3,
        )
        repository.save_result(queued)

    with SQLiteRepository(database) as reopened:
        recovered = reopened.recover(config, BASE + 10)
        assert len(recovered.state.queues[Direction.NORTH]) == 1
        assert all(signal == SignalState.UNKNOWN for signal in recovered.state.actual_signals.values())
        assert all(signal == SignalState.RED for signal in recovered.state.desired_signals.values())
        assert recovered.state.pending.purpose == "RECOVERY"
        assert reopened.pending_commands("A")[0].command_id == recovered.state.pending.command_id


def test_duplicate_sensor_event_remains_idempotent_after_reload(tmp_path):
    config = JunctionConfig("A")
    event = SensorEvent(
        event_id="event-duplicate",
        junction_id="A",
        direction="EAST",
        event_type="VEHICLE_ARRIVED",
        vehicle_id="truck-1",
        vehicle_type="TRUCK",
        timestamp=iso(BASE),
    )

    with SQLiteRepository(tmp_path / "traffic.db") as repository:
        started = repository.recover(config, BASE)
        result = _apply(repository, started.state, config, event, BASE + 1)
        ack = handle(
            result.state,
            config,
            ControllerAck(result.state.pending.command_id, "ACK"),
            BASE + 1,
        )
        repository.save_result(ack)
        loaded = repository.load_state("A")
        duplicate = handle(loaded, config, event, BASE + 2)

        assert duplicate.result.code == "DUPLICATE_EVENT"
        assert len(duplicate.state.queues[E]) == 1


def test_failed_effect_commit_rolls_back_state_audit_and_event_ids(tmp_path):
    database = tmp_path / "traffic.db"
    config = JunctionConfig("A")
    with SQLiteRepository(database) as repository:
        started = repository.recover(config, BASE)
        before = repository.load_state("A")
        audit_before = repository.list_audit("A")
        commands_before = repository.pending_commands("A")
        connection = sqlite3.connect(database)
        try:
            connection.execute(
                """
                CREATE TRIGGER reject_audit BEFORE INSERT ON audit_records
                BEGIN SELECT RAISE(ABORT, 'audit storage unavailable'); END
                """
            )
            connection.commit()
        finally:
            connection.close()

        event = SensorEvent(
            event_id="event-rollback",
            junction_id="A",
            direction="NORTH",
            event_type="VEHICLE_ARRIVED",
            vehicle_id="truck-rollback",
            vehicle_type="TRUCK",
            timestamp=iso(BASE),
        )
        changed = handle(started.state, config, event, BASE + 1)
        with pytest.raises(sqlite3.IntegrityError, match="audit storage unavailable"):
            repository.save_result(changed)

        after = repository.load_state("A")
        assert after == before
        assert repository.list_audit("A") == audit_before
        assert repository.pending_commands("A") == commands_before
        assert "event-rollback" not in after.processed_events


def test_manual_state_is_persisted_before_restart_discards_override(tmp_path):
    config = JunctionConfig("A")
    with SQLiteRepository(tmp_path / "traffic.db") as repository:
        started = repository.recover(config, BASE)
        manual = _apply(repository, started.state, config, Command(
            "MANUAL_GREEN_REQUEST", "WEST", "operator"
        ), BASE + 1)
        assert manual.state.manual_phase == "EAST_WEST"
        assert repository.load_state("A").manual_phase == "EAST_WEST"

        recovered = repository.recover(config, BASE + 2)
        assert recovered.state.manual_phase is None
        assert recovered.state.mode.value == "AUTOMATIC"
