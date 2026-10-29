"""Scheduling policy (pure functions).

phase_score = sum(vehicle weights waiting in the phase) + aging_weight * (seconds the oldest has waited)

decide() is called only while a phase is GREEN and answers "which phase should be served next, if any?"
  1. nobody else waiting                       -> stay (avoid unnecessary switching)
  2. someone waited >= starvation_s            -> serve them (starvation protection)
  3. green has lasted >= max_green_s           -> serve best waiting phase (hard cap)
  4. current phase has no demand               -> serve best waiting phase (gap-out)
  5. other score > current score * margin      -> switch (margin 1.25 before normal_green_s, 1.0 after)
The caller (engine) still enforces min_green_s, YELLOW and ALL_RED. This module never emits signals.
"""
from __future__ import annotations

from typing import Optional

from .config import JunctionConfig
from .models import DeviceStatus, JunctionState, device_key


def _vehicles(state: JunctionState, cfg: JunctionConfig, phase: str) -> list:
    return [v for d in cfg.directions_of(phase) for v in state.queues[d].values()]


def has_waiting(state, cfg, phase) -> bool:
    return bool(_vehicles(state, cfg, phase))


def oldest_wait(state, cfg, phase, now) -> float:
    vs = _vehicles(state, cfg, phase)
    return max((now - v.arrived_at for v in vs), default=0.0)


def phase_score(state, cfg, phase, now) -> float:
    vs = _vehicles(state, cfg, phase)
    if not vs:
        return 0.0
    base = sum(cfg.vehicle_weights[v.vehicle_type] for v in vs)
    return base + cfg.aging_weight * oldest_wait(state, cfg, phase, now)


def sensor_down(state, cfg, phase) -> bool:
    """A dead sensor means the queue for that direction cannot be trusted."""
    return any(
        state.device_status.get(device_key("SENSOR", d)) == DeviceStatus.OFFLINE
        or state.device_status.get("SENSOR") == DeviceStatus.OFFLINE
        for d in cfg.directions_of(phase)
    )


def choose_best(state, cfg, now) -> str:
    """Pick the phase to turn GREEN when we are coming out of ALL_RED with no pre-chosen target
    (first start-up / restart / recovery). Ties prefer the oldest waiter, then 'not the one we just served'."""
    order = list(cfg.phases)

    def key(p):
        return (phase_score(state, cfg, p, now), oldest_wait(state, cfg, p, now), p != state.phase, -order.index(p))

    return max(order, key=key)


def decide(state, cfg, now) -> Optional[str]:
    cur = state.phase
    if cur is None:
        return choose_best(state, cfg, now)
    elapsed = now - state.step_started_at

    contenders = [
        p for p in cfg.phases
        if p != cur and (
            has_waiting(state, cfg, p)
            # fixed-time fallback: a direction with a dead sensor still gets its turn
            or (sensor_down(state, cfg, p) and elapsed >= cfg.normal_green_s)
        )
    ]
    if not contenders:
        return None

    starving = [p for p in contenders if oldest_wait(state, cfg, p, now) >= cfg.starvation_s]
    if starving:
        return max(starving, key=lambda p: oldest_wait(state, cfg, p, now))

    best = max(contenders, key=lambda p: phase_score(state, cfg, p, now))
    if elapsed >= cfg.max_green_s:
        return best

    cur_score = phase_score(state, cfg, cur, now)
    if cur_score == 0.0:
        return best

    margin = cfg.switch_margin if elapsed < cfg.normal_green_s else 1.0
    if phase_score(state, cfg, best, now) > cur_score * margin:
        return best
    return None
