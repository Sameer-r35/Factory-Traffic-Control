"""HTTP API and browser dashboard for the Junction A traffic-management demo."""
from __future__ import annotations

import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from traffic.application import JunctionActor
from traffic.application.controller_simulator import ControllerSimulator
from traffic.domain import JunctionConfig, status_view
from traffic.domain.models import Command, ControllerAck, SensorEvent, StatusEvent
from traffic.persistence import SQLiteRepository

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATABASE_PATH = (
    Path("/tmp/traffic.db")
    if os.environ.get("VERCEL") == "1"
    else PROJECT_ROOT / "traffic.db"
)
DATABASE_PATH = Path(os.environ.get("TRAFFIC_DB_PATH", DEFAULT_DATABASE_PATH))
JUNCTION_ID = os.environ.get("TRAFFIC_JUNCTION_ID", "A")
WEB_ROOT = PROJECT_ROOT / "web"


class SensorEventBody(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    junction_id: str = JUNCTION_ID
    direction: str
    event_type: str
    vehicle_id: str = Field(min_length=1, max_length=128)
    vehicle_type: Optional[str] = None
    sequence_no: Optional[int] = Field(default=None, ge=0, strict=True)
    timestamp: str


class CommandBody(BaseModel):
    command: str
    direction: Optional[str] = None
    issued_by: str = "dashboard"


class ControllerEventBody(BaseModel):
    command_id: str
    status: str
    actual_signals: Optional[dict[str, str]] = None


class SimulatorBody(BaseModel):
    online: bool


@asynccontextmanager
async def lifespan(app: FastAPI):
    weights_raw = os.environ.get("TRAFFIC_VEHICLE_WEIGHTS")
    if weights_raw is None:
        config = JunctionConfig.default(JUNCTION_ID)
    else:
        try:
            weights = json.loads(weights_raw)
            if not isinstance(weights, dict):
                raise ValueError("expected a JSON object mapping vehicle types to weights")
            config = JunctionConfig(junction_id=JUNCTION_ID, vehicle_weights=weights)
        except (json.JSONDecodeError, ValueError) as exc:
            raise RuntimeError(f"Invalid TRAFFIC_VEHICLE_WEIGHTS: {exc}") from exc
    repository = SQLiteRepository(DATABASE_PATH)
    actor = JunctionActor(config, repository, dispatch_command)
    simulator = ControllerSimulator(actor.submit)
    app.state.config = config
    app.state.repository = repository
    app.state.actor = actor
    app.state.simulator = simulator
    await actor.start()
    try:
        yield
    finally:
        await simulator.close()
        await actor.close()
        repository.close()


async def dispatch_command(command) -> None:
    await app.state.simulator.send(command)


app = FastAPI(title="Factory Traffic Management System", version="1.0.0", lifespan=lifespan)


def _actor(request, junction_id: Optional[str] = None) -> JunctionActor:
    actor: JunctionActor = request.app.state.actor
    if junction_id is not None and junction_id != actor.config.junction_id:
        raise HTTPException(status_code=404, detail=f"Unknown junction {junction_id!r}")
    return actor


def _result_payload(result) -> dict:
    return {
        "kind": result.result.kind.value,
        "code": result.result.code,
        "message": result.result.message,
        "version": result.state.version,
    }


@app.get("/", include_in_schema=False)
async def dashboard():
    return FileResponse(WEB_ROOT / "index.html")


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/status")
@app.get("/api/junctions/{junction_id}/status")
async def get_status(request: Request, junction_id: Optional[str] = None):
    actor = _actor(request, junction_id)
    return status_view(actor.state, actor.config, time.time())


@app.get("/api/audit")
@app.get("/api/junctions/{junction_id}/audit")
async def get_audit(
    request: Request,
    junction_id: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=500),
):
    actor = _actor(request, junction_id)
    records = request.app.state.repository.list_audit(actor.config.junction_id, limit)
    return [
        {
            "event_type": record.event_type,
            "timestamp": record.timestamp,
            "direction": record.direction,
            "previous_state": record.previous_state,
            "new_state": record.new_state,
            "command_id": record.command_id,
            "detail": record.detail,
        }
        for record in records
    ]


@app.post("/api/sensor-events")
async def sensor_event(request: Request, body: SensorEventBody):
    actor = _actor(request, body.junction_id)
    result = await actor.submit(SensorEvent(**body.model_dump()))
    return _result_payload(result)


@app.post("/api/commands")
async def command(request: Request, body: CommandBody):
    result = await _actor(request).submit(Command(**body.model_dump()))
    return _result_payload(result)


@app.post("/api/controller-events")
async def controller_event(request: Request, body: ControllerEventBody):
    result = await _actor(request).submit(ControllerAck(**body.model_dump()))
    return _result_payload(result)


@app.post("/api/simulator")
async def simulator_status(request: Request, body: SimulatorBody):
    actor = _actor(request)
    simulator: ControllerSimulator = request.app.state.simulator
    simulator.online = body.online
    event = StatusEvent(
        event_id=f"sim-status-{uuid.uuid4()}",
        device_type="SIGNAL_CONTROLLER",
        status="ONLINE" if body.online else "OFFLINE",
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )
    result = await actor.submit(event)
    return {**_result_payload(result), "simulator_online": simulator.online}
