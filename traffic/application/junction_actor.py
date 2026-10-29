"""A single-junction actor serializes inputs and persists before dispatching effects."""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from traffic.domain import EngineResult, JunctionConfig, JunctionState, Tick, handle
from traffic.domain.models import SendCommand
from traffic.persistence import SQLiteRepository

logger = logging.getLogger(__name__)


CommandDispatcher = Callable[[SendCommand], Awaitable[None]]


@dataclass
class _Envelope:
    value: object
    future: asyncio.Future


class JunctionActor:
    """Owns the mutable runtime state for one junction and processes its inputs in order."""

    def __init__(
        self,
        config: JunctionConfig,
        repository: SQLiteRepository,
        dispatch_command: CommandDispatcher,
        tick_interval_s: float = 1.0,
    ):
        if tick_interval_s <= 0:
            raise ValueError("tick_interval_s must be positive")
        self.config = config
        self.repository = repository
        self.dispatch_command = dispatch_command
        self.tick_interval_s = tick_interval_s
        self.state: Optional[JunctionState] = None
        self._queue: asyncio.Queue = asyncio.Queue()
        self._worker: Optional[asyncio.Task] = None
        self._ticker: Optional[asyncio.Task] = None
        self._closed = False

    async def start(self) -> None:
        if self._worker is not None:
            return
        recovered = self.repository.recover(self.config, time.time())
        self.state = recovered.state
        self._worker = asyncio.create_task(self._run(), name=f"junction-{self.config.junction_id}")
        await self._dispatch(recovered)
        self._ticker = asyncio.create_task(
            self._tick_loop(), name=f"junction-{self.config.junction_id}-tick"
        )

    async def close(self) -> None:
        self._closed = True
        tasks = [task for task in (self._ticker, self._worker) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._ticker = self._worker = None

    async def submit(self, value: object) -> EngineResult:
        if self._closed or self._worker is None:
            raise RuntimeError(f"junction actor {self.config.junction_id} is not running")
        future = asyncio.get_running_loop().create_future()
        await self._queue.put(_Envelope(value, future))
        return await future

    async def _tick_loop(self) -> None:
        while True:
            await asyncio.sleep(self.tick_interval_s)
            try:
                await self.submit(Tick())
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Tick processing failed for junction %s", self.config.junction_id)

    async def _run(self) -> None:
        while True:
            envelope = await self._queue.get()
            try:
                state = self.state
                if state is None:
                    raise RuntimeError(f"junction actor {self.config.junction_id} has no recovered state")
                result = handle(state, self.config, envelope.value, time.time())
                self.repository.save_result(result)
                self.state = result.state
                await self._dispatch(result)
                if not envelope.future.done():
                    envelope.future.set_result(result)
            except asyncio.CancelledError:
                if not envelope.future.done():
                    envelope.future.cancel()
                raise
            except Exception as exc:
                logger.exception(
                    "Input processing failed for junction %s",
                    self.config.junction_id,
                )
                if not envelope.future.done():
                    envelope.future.set_exception(exc)
            finally:
                self._queue.task_done()

    async def _dispatch(self, result: EngineResult) -> None:
        for effect in result.effects:
            if isinstance(effect, SendCommand):
                await self.dispatch_command(effect)
