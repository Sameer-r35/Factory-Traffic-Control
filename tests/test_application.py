import time

import pytest
from fastapi.testclient import TestClient

from traffic import api
from traffic.domain.models import VehicleType


def test_http_dashboard_sensor_commands_and_controller_recovery(tmp_path, monkeypatch):
    monkeypatch.setattr(api, "DATABASE_PATH", tmp_path / "api.db")
    monkeypatch.delenv("TRAFFIC_VEHICLE_WEIGHTS", raising=False)
    with TestClient(api.app) as client:
        page = client.get("/")
        assert page.status_code == 200
        assert "Factory Traffic Control" in page.text

        status = client.get("/api/status")
        assert status.status_code == 200
        assert status.json()["junction_id"] == "A"

        arrival = client.post("/api/sensor-events", json={
            "event_id": "http-arrival-1",
            "junction_id": "A",
            "direction": "EAST",
            "event_type": "VEHICLE_ARRIVED",
            "vehicle_id": "truck-http-1",
            "vehicle_type": "TRUCK",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        assert arrival.status_code == 200
        assert arrival.json()["code"] == "VEHICLE_QUEUED"
        assert client.get("/api/status").json()["queues"]["EAST"] == 1

        manual = client.post("/api/commands", json={
            "command": "MANUAL_GREEN_REQUEST",
            "direction": "WEST",
            "issued_by": "test",
        })
        assert manual.status_code == 200
        assert manual.json()["code"] == "MANUAL_ACCEPTED"

        offline = client.post("/api/simulator", json={"online": False})
        assert offline.status_code == 200
        assert client.get("/api/status").json()["mode"] == "DEGRADED"

        online = client.post("/api/simulator", json={"online": True})
        assert online.status_code == 200
        assert online.json()["simulator_online"] is True

        audit = client.get("/api/audit?limit=100")
        assert audit.status_code == 200
        event_types = [record["event_type"] for record in audit.json()]
        assert "VEHICLE_DETECTED" in event_types
        assert "SIGNAL_FAILURE" not in event_types


def test_http_validation_and_unknown_junction_are_explicit(tmp_path, monkeypatch):
    monkeypatch.setattr(api, "DATABASE_PATH", tmp_path / "api.db")
    monkeypatch.delenv("TRAFFIC_VEHICLE_WEIGHTS", raising=False)
    with TestClient(api.app) as client:
        unknown = client.post("/api/sensor-events", json={
            "event_id": "unknown-junction",
            "junction_id": "B",
            "direction": "NORTH",
            "event_type": "VEHICLE_ARRIVED",
            "vehicle_id": "truck",
            "vehicle_type": "TRUCK",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        assert unknown.status_code == 404

        invalid = client.post("/api/sensor-events", json={
            "event_id": "invalid-direction",
            "junction_id": "A",
            "direction": "UP",
            "event_type": "VEHICLE_ARRIVED",
            "vehicle_id": "truck",
            "vehicle_type": "TRUCK",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        assert invalid.status_code == 200
        assert invalid.json()["kind"] == "INVALID"


def test_vehicle_priority_weights_are_configurable_from_environment(tmp_path, monkeypatch):
    monkeypatch.setattr(api, "DATABASE_PATH", tmp_path / "api.db")
    monkeypatch.setenv("TRAFFIC_VEHICLE_WEIGHTS", '{"TRUCK": 8, "EMPLOYEE_VEHICLE": 4}')
    with TestClient(api.app):
        weights = api.app.state.actor.config.vehicle_weights
        assert weights[VehicleType.TRUCK] == 8
        assert weights[VehicleType.EMPLOYEE_VEHICLE] == 4
        assert weights[VehicleType.FORKLIFT] == 2


def test_invalid_vehicle_priority_environment_fails_startup_clearly(monkeypatch):
    monkeypatch.setenv("TRAFFIC_VEHICLE_WEIGHTS", '{"UNKNOWN": 5}')
    with pytest.raises(RuntimeError, match="Invalid TRAFFIC_VEHICLE_WEIGHTS"):
        with TestClient(api.app):
            pass
