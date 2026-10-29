"""SQLite persistence for junction snapshots, event deduplication, audit, and command outbox."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from enum import Enum
from pathlib import Path
from typing import Optional

from traffic.domain.config import JunctionConfig
from traffic.domain.engine import handle, initial_state
from traffic.domain.models import (
    AuditRecord,
    DeviceStatus,
    Direction,
    EngineResult,
    JunctionState,
    Mode,
    PendingCommand,
    QueuedVehicle,
    SendCommand,
    SignalState,
    Startup,
    Step,
    VehicleType,
)


_SCHEMA_VERSION = 1


def _json_value(value):
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {
            (key.value if isinstance(key, Enum) else key): _json_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _dump(value) -> str:
    return json.dumps(_json_value(value), separators=(",", ":"), allow_nan=False)


def _signals_to_data(signals: dict) -> dict:
    return {direction.value: signal.value for direction, signal in signals.items()}


def _signals_from_data(signals: dict) -> dict:
    return {Direction(direction): SignalState(signal) for direction, signal in signals.items()}


def _state_to_data(state: JunctionState) -> dict:
    return {
        "junction_id": state.junction_id,
        "mode": state.mode.value,
        "phase": state.phase,
        "step": state.step.value,
        "step_started_at": state.step_started_at,
        "desired_signals": _signals_to_data(state.desired_signals),
        "actual_signals": _signals_to_data(state.actual_signals),
        "queues": {
            direction.value: {
                vehicle_id: {
                    "vehicle_id": vehicle.vehicle_id,
                    "vehicle_type": vehicle.vehicle_type.value,
                    "arrived_at": vehicle.arrived_at,
                    "sensor_ts": vehicle.sensor_ts,
                    "event_id": vehicle.event_id,
                }
                for vehicle_id, vehicle in queue.items()
            }
            for direction, queue in state.queues.items()
        },
        "tombstones": {
            direction.value: tombstones
            for direction, tombstones in state.tombstones.items()
        },
        "target_phase": state.target_phase,
        "pending": None if state.pending is None else {
            "command_id": state.pending.command_id,
            "signals": _signals_to_data(state.pending.signals),
            "sent_at": state.pending.sent_at,
            "attempts": state.pending.attempts,
            "purpose": state.pending.purpose,
        },
        "last_sequence": {
            direction.value: sequence for direction, sequence in state.last_sequence.items()
        },
        "command_seq": state.command_seq,
        "recent_commands": state.recent_commands,
        "manual_phase": state.manual_phase,
        "manual_expires_at": state.manual_expires_at,
        "manual_by": state.manual_by,
        "emergency_phase": state.emergency_phase,
        "device_status": {
            key: status.value for key, status in state.device_status.items()
        },
        "degraded_reason": state.degraded_reason,
        "next_probe_at": state.next_probe_at,
        "version": state.version,
    }


def _state_from_data(data: dict, processed_events: dict) -> JunctionState:
    queues = {
        Direction(direction): {
            vehicle_id: QueuedVehicle(
                vehicle_id=vehicle["vehicle_id"],
                vehicle_type=VehicleType(vehicle["vehicle_type"]),
                arrived_at=vehicle["arrived_at"],
                sensor_ts=vehicle["sensor_ts"],
                event_id=vehicle["event_id"],
            )
            for vehicle_id, vehicle in queue.items()
        }
        for direction, queue in data["queues"].items()
    }
    pending_data = data["pending"]
    pending = None if pending_data is None else PendingCommand(
        command_id=pending_data["command_id"],
        signals=_signals_from_data(pending_data["signals"]),
        sent_at=pending_data["sent_at"],
        attempts=pending_data["attempts"],
        purpose=pending_data["purpose"],
    )
    return JunctionState(
        junction_id=data["junction_id"],
        mode=Mode(data["mode"]),
        phase=data["phase"],
        step=Step(data["step"]),
        step_started_at=data["step_started_at"],
        desired_signals=_signals_from_data(data["desired_signals"]),
        actual_signals=_signals_from_data(data["actual_signals"]),
        queues=queues,
        tombstones={
            Direction(direction): tombstones
            for direction, tombstones in data["tombstones"].items()
        },
        target_phase=data["target_phase"],
        pending=pending,
        processed_events=processed_events,
        last_sequence={
            Direction(direction): sequence
            for direction, sequence in data["last_sequence"].items()
        },
        command_seq=data["command_seq"],
        recent_commands=data["recent_commands"],
        manual_phase=data["manual_phase"],
        manual_expires_at=data["manual_expires_at"],
        manual_by=data["manual_by"],
        emergency_phase=data["emergency_phase"],
        device_status={
            key: DeviceStatus(status)
            for key, status in data["device_status"].items()
        },
        degraded_reason=data["degraded_reason"],
        next_probe_at=data["next_probe_at"],
        version=data["version"],
    )


class SQLiteRepository:
    """Durably saves each engine result and its audit/outgoing-command effects atomically."""

    def __init__(self, database_path: str | Path):
        self._connection = sqlite3.connect(str(database_path), timeout=5.0, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._initialize_schema()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "SQLiteRepository":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def _initialize_schema(self) -> None:
        with self._lock:
            version = self._connection.execute("PRAGMA user_version").fetchone()[0]
            if version > _SCHEMA_VERSION:
                raise RuntimeError(f"database schema version {version} is newer than supported version {_SCHEMA_VERSION}")
            if version == 0:
                migration = Path(__file__).parent / "migrations" / "001_initial.sql"
                script = migration.read_text(encoding="utf-8")
                try:
                    self._connection.executescript(
                        f"BEGIN IMMEDIATE;\n{script}\n"
                        f"PRAGMA user_version = {_SCHEMA_VERSION};\nCOMMIT;"
                    )
                except sqlite3.Error:
                    self._connection.rollback()
                    raise

    def load_state(self, junction_id: str) -> Optional[JunctionState]:
        """Return the persisted logical state, or None for a junction with no saved state."""
        with self._lock:
            row = self._connection.execute(
                "SELECT snapshot_json FROM junction_state WHERE junction_id = ?",
                (junction_id,),
            ).fetchone()
            if row is None:
                return None
            event_rows = self._connection.execute(
                "SELECT event_id, processed_at FROM processed_events WHERE junction_id = ?",
                (junction_id,),
            ).fetchall()
            data = json.loads(row["snapshot_json"])
            events = {event["event_id"]: event["processed_at"] for event in event_rows}
            state = _state_from_data(data, events)
            if state.junction_id != junction_id:
                raise ValueError("stored junction ID does not match the requested junction")
            return state

    def recover(self, config: JunctionConfig, now: float) -> EngineResult:
        """Load logical state and pass it through the domain's safe Startup transition."""
        state = self.load_state(config.junction_id)
        if state is None:
            state = initial_state(config, now)
        result = handle(state, config, Startup(), now)
        self.save_result(result)
        return result

    def save_result(self, result: EngineResult) -> None:
        """Commit a post-input state, event dedup keys, audit records, and command outbox together."""
        state = result.state
        with self._lock:
            connection = self._connection
            saved_at = time.time()
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """
                    INSERT INTO junction_state (junction_id, version, snapshot_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(junction_id) DO UPDATE SET
                        version = excluded.version,
                        snapshot_json = excluded.snapshot_json,
                        updated_at = excluded.updated_at
                    """,
                    (state.junction_id, state.version, _dump(_state_to_data(state)), saved_at),
                )
                connection.execute(
                    "DELETE FROM processed_events WHERE junction_id = ?",
                    (state.junction_id,),
                )
                connection.executemany(
                    """
                    INSERT INTO processed_events (junction_id, event_id, processed_at)
                    VALUES (?, ?, ?)
                    """,
                    [
                        (state.junction_id, event_id, processed_at)
                        for event_id, processed_at in state.processed_events.items()
                    ],
                )

                for effect in result.effects:
                    if isinstance(effect, SendCommand):
                        if effect.junction_id != state.junction_id:
                            raise ValueError("command effect belongs to a different junction")
                        connection.execute(
                            """
                            INSERT INTO controller_commands
                                (junction_id, command_id, signals_json, attempts, purpose, status, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?)
                            ON CONFLICT(junction_id, command_id) DO UPDATE SET
                                signals_json = excluded.signals_json,
                                attempts = excluded.attempts,
                                purpose = excluded.purpose,
                                status = 'PENDING',
                                updated_at = excluded.updated_at
                            """,
                            (
                                effect.junction_id,
                                effect.command_id,
                                _dump(_signals_to_data(effect.signals)),
                                effect.attempt,
                                state.pending.purpose if state.pending and state.pending.command_id == effect.command_id
                                else "RETRY",
                                state.pending.sent_at if state.pending and state.pending.command_id == effect.command_id
                                else saved_at,
                                state.pending.sent_at if state.pending and state.pending.command_id == effect.command_id
                                else saved_at,
                            ),
                        )

                pending_id = state.pending.command_id if state.pending is not None else None
                command_rows = connection.execute(
                    """
                    SELECT command_id FROM controller_commands
                    WHERE junction_id = ? AND status = 'PENDING'
                    """,
                    (state.junction_id,),
                ).fetchall()
                for row in command_rows:
                    command_id = row["command_id"]
                    if command_id == pending_id:
                        continue
                    final_status = state.recent_commands.get(command_id, "ABANDONED")
                    connection.execute(
                        """
                        UPDATE controller_commands SET status = ?, updated_at = ?
                        WHERE junction_id = ? AND command_id = ?
                        """,
                        (final_status, saved_at, state.junction_id, command_id),
                    )

                for effect in result.effects:
                    if isinstance(effect, AuditRecord):
                        connection.execute(
                            """
                            INSERT INTO audit_records
                                (junction_id, event_type, timestamp, direction, previous_state,
                                 new_state, command_id, detail_json)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                effect.junction_id,
                                effect.event_type,
                                effect.timestamp,
                                effect.direction,
                                effect.previous_state,
                                effect.new_state,
                                effect.command_id,
                                None if effect.detail is None else _dump(effect.detail),
                            ),
                        )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def list_audit(self, junction_id: str, limit: int = 100) -> list[AuditRecord]:
        if limit < 1:
            raise ValueError("limit must be positive")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT junction_id, event_type, timestamp, direction, previous_state,
                       new_state, command_id, detail_json
                FROM audit_records
                WHERE junction_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (junction_id, limit),
            ).fetchall()
        return [
            AuditRecord(
                junction_id=row["junction_id"],
                event_type=row["event_type"],
                timestamp=row["timestamp"],
                direction=row["direction"],
                previous_state=row["previous_state"],
                new_state=row["new_state"],
                command_id=row["command_id"],
                detail=None if row["detail_json"] is None else json.loads(row["detail_json"]),
            )
            for row in reversed(rows)
        ]

    def pending_commands(self, junction_id: str) -> list[SendCommand]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT command_id, signals_json, attempts
                FROM controller_commands
                WHERE junction_id = ? AND status = 'PENDING'
                ORDER BY created_at, command_id
                """,
                (junction_id,),
            ).fetchall()
        return [
            SendCommand(
                command_id=row["command_id"],
                junction_id=junction_id,
                signals=_signals_from_data(json.loads(row["signals_json"])),
                attempt=row["attempts"],
            )
            for row in rows
        ]
