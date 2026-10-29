"""Per-junction configuration. Nothing junction-specific is hard-coded in the engine."""
from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field

from .models import Direction, VehicleType


def _default_phases() -> dict:
    return {
        "NORTH_SOUTH": (Direction.NORTH, Direction.SOUTH),
        "EAST_WEST": (Direction.EAST, Direction.WEST),
    }


def _default_weights() -> dict:
    # EMERGENCY > TRUCK > FORKLIFT > EMPLOYEE_VEHICLE (configurable priority policy)
    return {
        VehicleType.EMERGENCY: 100.0,
        VehicleType.TRUCK: 3.0,
        VehicleType.FORKLIFT: 2.0,
        VehicleType.EMPLOYEE_VEHICLE: 1.0,
    }


def _default_aliases() -> dict:
    # Spec lists "material-carrying vehicles" in the scenario but not in the supported types.
    return {"MATERIAL_CARRIER": VehicleType.TRUCK, "DELIVERY_TRUCK": VehicleType.TRUCK}


@dataclass(frozen=True)
class JunctionConfig:
    junction_id: str
    # Two phases are "conflicting" iff they are different phases. Adding Junction B/C/D = new config.
    phases: dict = field(default_factory=_default_phases)

    # --- signal timing (seconds)
    min_green_s: float = 10.0      # anti-flapping: no voluntary switch before this
    normal_green_s: float = 30.0   # after this, the switching margin drops to 1.0
    max_green_s: float = 60.0      # hard cap when someone else is waiting
    yellow_s: float = 5.0
    all_red_s: float = 2.0

    # --- controller communication
    ack_timeout_s: float = 3.0
    max_retries: int = 2           # total sends = 1 + max_retries (same command_id)
    probe_interval_s: float = 10.0 # DEGRADED: how often to try the controller again

    # --- overrides / emergencies
    manual_lease_s: float = 300.0
    emergency_ttl_s: float = 120.0

    # --- event hygiene
    stale_event_s: float = 300.0
    future_skew_s: float = 60.0
    vehicle_max_age_s: float = 900.0     # assume a CLEARED event was lost after this long
    processed_retention_s: float = 1800.0  # must be > stale_event_s

    # --- scheduling
    starvation_s: float = 45.0     # (must be < max_green_s + yellow + all_red to matter)
    switch_margin: float = 1.25
    aging_weight: float = 0.2
    vehicle_weights: dict = field(default_factory=_default_weights)
    vehicle_type_aliases: dict = field(default_factory=_default_aliases)

    def __post_init__(self):
        if len(self.phases) < 2:
            raise ValueError("a junction needs at least two phases")
        seen = []
        for phase, dirs in self.phases.items():
            seen.extend(dirs)
        if len(seen) != len(set(seen)):
            raise ValueError("a direction may belong to only one phase")
        if self.processed_retention_s <= self.stale_event_s:
            raise ValueError("processed_retention_s must exceed stale_event_s")
        if not isinstance(self.vehicle_weights, Mapping):
            raise ValueError("vehicle_weights must map vehicle types to numeric priority weights")
        defaults = _default_weights()
        configured = {}
        for raw_type, weight in self.vehicle_weights.items():
            if isinstance(raw_type, VehicleType):
                vehicle_type = raw_type
            elif isinstance(raw_type, str):
                try:
                    vehicle_type = VehicleType(raw_type.strip().upper())
                except ValueError as exc:
                    raise ValueError(f"unknown vehicle type in priority weights: {raw_type!r}") from exc
            else:
                raise ValueError(f"invalid vehicle type in priority weights: {raw_type!r}")
            if isinstance(weight, bool) or not isinstance(weight, (int, float)):
                raise ValueError(f"priority weight for {vehicle_type.value} must be a finite non-negative number")
            try:
                normalized_weight = float(weight)
            except OverflowError as exc:
                raise ValueError(
                    f"priority weight for {vehicle_type.value} must be a finite non-negative number"
                ) from exc
            if not math.isfinite(normalized_weight) or normalized_weight < 0:
                raise ValueError(f"priority weight for {vehicle_type.value} must be a finite non-negative number")
            configured[vehicle_type] = normalized_weight
        defaults.update(configured)
        object.__setattr__(self, "vehicle_weights", defaults)

    @property
    def directions(self) -> list:
        return [d for dirs in self.phases.values() for d in dirs]

    def phase_of(self, direction: Direction) -> str:
        for phase, dirs in self.phases.items():
            if direction in dirs:
                return phase
        raise KeyError(direction)

    def directions_of(self, phase: str) -> tuple:
        return self.phases[phase]

    @staticmethod
    def default(junction_id: str = "A") -> "JunctionConfig":
        return JunctionConfig(junction_id=junction_id)
