"""In-process physical controller simulator with delayed ACKs and online/offline control."""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from traffic.domain.models import ControllerAck, Direction, SendCommand, SignalState


AckHandler = Callable[[ControllerAck], Awaitable[object]]
logger = logging.getLogger(__name__)


class ControllerSimulator:
    def __init__(self, ack_handler: AckHandler, ack_delay_s: float = 0.05):
        if ack_delay_s < 0:
            raise ValueError("ack_delay_s cannot be negative")
        self.ack_handler = ack_handler
        self.ack_delay_s = ack_delay_s
        self.online = True
        self.actual_signals = {direction: SignalState.UNKNOWN for direction in Direction}
        self._tasks: set[asyncio.Task] = set()

    async def send(self, command: SendCommand) -> None:
        if not self.online:
            return
        self.actual_signals = dict(command.signals)
        task = asyncio.create_task(
            self._ack(command, dict(self.actual_signals)),
            name=f"controller-ack-{command.command_id}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._on_ack_done)

    def _on_ack_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Simulated controller ACK failed")

    async def _ack(self, command: SendCommand, actual_signals: dict) -> None:
        await asyncio.sleep(self.ack_delay_s)
        if not self.online:
            return
        await self.ack_handler(ControllerAck(
            command_id=command.command_id,
            status="ACK",
            actual_signals={
                direction.value: signal.value
                for direction, signal in actual_signals.items()
            },
        ))

    async def close(self) -> None:
        for task in tuple(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
