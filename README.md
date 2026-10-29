# Factory Traffic Management System

## Requirements

- Python 3.10 or newer
- PowerShell (Windows)

## Install

Open PowerShell in this project folder:

```powershell
cd "D:\sameer_files\assessment\backend project\files\traffic-mgmt"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If PowerShell blocks virtual-environment activation, run the commands below using `.venv\Scripts\python.exe` instead of `python`.

## Start the application

From the project folder, with the virtual environment active:

```powershell
python -m uvicorn traffic.api:app --reload
```

Open these URLs in your browser:

- Dashboard: [http://127.0.0.1:8000](http://127.0.0.1:8000)
- Interactive API documentation: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

Keep the terminal open while using the application. Press `Ctrl+C` in that terminal to stop it.

## Deploy a public demo on Render

The repository includes a Render Blueprint in `render.yaml`.

1. Sign in to [Render](https://render.com/) and connect your GitHub account.
2. Open [Render's Blueprint deployment page](https://render.com/deploy), select this repository, and deploy the Blueprint.
3. Wait for the deployment to finish, then open the `onrender.com` URL shown for the service. Add `/docs` to that URL to open the API documentation.

The free demo uses the local SQLite database. Its data is temporary and can be lost when the service restarts or is redeployed; the free plan is for trying the app, not durable storage. The app uses a simulated signal controller and has no authentication, so do not use it to control real traffic or submit sensitive data.

The app creates `traffic.db` in the project folder on first run. To use a different database path, set `TRAFFIC_DB_PATH` before starting:

```powershell
$env:TRAFFIC_DB_PATH = "D:\traffic-data\traffic.db"
python -m uvicorn traffic.api:app --reload
```

Vehicle priority weights can also be overridden before starting. Supply a JSON object with supported vehicle names and non-negative numeric weights; omitted vehicle types keep their defaults:

```powershell
$env:TRAFFIC_VEHICLE_WEIGHTS = '{"EMERGENCY": 100, "TRUCK": 8, "FORKLIFT": 2, "EMPLOYEE_VEHICLE": 4}'
python -m uvicorn traffic.api:app --reload
```

Changing the weights affects future scheduling decisions. Invalid JSON, unsupported vehicle names, or invalid weights stop startup with an error.

## Run tests

From the project folder:

```powershell
python -m pytest -q
```

## Assumptions / Questions / Requirement Issues

- **Emergency preemption vs. safe sequencing (contradictory):** “Immediately preempt” can conflict with the required safe transition. Safety takes priority: emergencies may bypass minimum green, but still require YELLOW, ALL_RED, and controller confirmation before the conflicting phase turns GREEN.
- **Per-direction commands vs. conflicting-green invariant (technically problematic):** Issuing separate commands for each signal head could create an unsafe intermediate plan. The controller receives one atomic plan containing all four signal states.
- **Supported vehicle types (unclear):** The scenario mentions material-carrying vehicles, but the listed types do not define that category. The implementation uses the specified FORKLIFT, TRUCK, EMPLOYEE_VEHICLE, and EMERGENCY types, with material-carrier labels mapped to TRUCK.
- **Event identity and ordering (unclear):** The specification does not establish whether event IDs, sequence numbers, or timestamps are authoritative. `event_id` is used for idempotency; sequence numbers are diagnostic; sensor timestamps help reject stale per-vehicle arrival/clearance ordering. Server receipt time drives waits and leases.
- **Clearance without a matching arrival (unclear):** Such a clearance is ignored and audited; it never decrements a queue below zero.
- **Emergency conflicts and expiry (business decision/missing):** The policy for simultaneous emergencies and stale detections is unspecified. The oldest received emergency phase is served first, the in-progress transition target is held, and an emergency expires after 120 seconds without a newer arrival.
- **Emergency vs. manual control (business decision):** Priority is unspecified. Emergency mode overrides manual control; manual requests cannot interrupt an active emergency.
- **Manual override lifetime (missing):** No expiry is specified. Manual control uses a five-minute lease to avoid an indefinite override if the operator disconnects.
- **Restart during a transition (safety-critical/missing):** The physical signal state cannot be trusted after process restart. Actual state becomes UNKNOWN and the controller is asked to confirm ALL_RED before normal sequencing resumes; queues and event deduplication are retained.
- **ALL_RED duration and ACK timing (missing):** The spec does not define an all-red interval or an ACK timestamp requirement. This implementation uses a two-second all-red interval and correlates ACKs by command ID, recording server time.
- **Lost sensor clearances and sensor outages (technically problematic):** A missing clearance can leave a vehicle queued indefinitely, while an offline sensor makes demand unknown. Vehicles expire after 15 minutes; dead sensors use a fixed-time scheduling fallback. These are demo policies that need validation against real sensor behavior.
- **Deployment and access control (missing):** Authentication and production controller transport are not specified. This implementation is a local single-process demo with SQLite and an in-process controller simulator; it should not be exposed as a production control system without appropriate security and hardware integration.
