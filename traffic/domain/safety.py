"""Hard safety checks. Defence in depth: the engine is designed never to trip these, and every
signal plan it emits is still checked here before it can reach a controller."""
from __future__ import annotations

from .config import JunctionConfig
from .models import SignalState

NON_RED = (SignalState.GREEN, SignalState.YELLOW)


class SafetyViolation(Exception):
    pass


def _phases_showing(cfg: JunctionConfig, signals: dict, states: tuple) -> set:
    return {cfg.phase_of(d) for d, st in signals.items() if st in states}


def assert_safe_signals(cfg: JunctionConfig, signals: dict) -> None:
    """Invariant 1: at most ONE phase may show anything other than RED."""
    missing = set(cfg.directions) - set(signals)
    if missing:
        raise SafetyViolation(f"signal plan is missing directions: {sorted(d.value for d in missing)}")
    if any(st == SignalState.UNKNOWN for st in signals.values()):
        raise SafetyViolation("desired state may never be UNKNOWN")
    if len(_phases_showing(cfg, signals, NON_RED)) > 1:
        raise SafetyViolation(f"conflicting phases both non-RED: {signals}")


def assert_safe_transition(cfg: JunctionConfig, prev: dict, new: dict) -> None:
    """Invariant 2/3: a phase may only turn GREEN if every conflicting direction was RED before."""
    for phase in _phases_showing(cfg, new, (SignalState.GREEN,)):
        for d in cfg.directions:
            if cfg.phase_of(d) != phase and prev.get(d) != SignalState.RED:
                raise SafetyViolation(
                    f"unsafe transition into GREEN({phase}): {d.value} was {prev.get(d)} (must be RED)"
                )
