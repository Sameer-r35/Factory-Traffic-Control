import asyncio
from datetime import datetime, timezone

from traffic.application import JunctionActor
from traffic.domain import Direction, JunctionConfig, SensorEvent
from traffic.persistence import SQLiteRepository


def test_actor_serializes_concurrent_inputs_for_one_junction(tmp_path):
    async def exercise():
        config = JunctionConfig("A")

        async def dispatch(_command):
            return None

        repository = SQLiteRepository(tmp_path / "actor.db")
        actor = JunctionActor(config, repository, dispatch, tick_interval_s=3600)
        try:
            await actor.start()
            timestamp = datetime.now(timezone.utc).isoformat()
            first = SensorEvent(
                event_id="parallel-1",
                junction_id="A",
                direction="NORTH",
                event_type="VEHICLE_ARRIVED",
                vehicle_id="same-vehicle",
                vehicle_type="TRUCK",
                timestamp=timestamp,
            )
            second = SensorEvent(
                event_id="parallel-2",
                junction_id="A",
                direction="NORTH",
                event_type="VEHICLE_ARRIVED",
                vehicle_id="same-vehicle",
                vehicle_type="TRUCK",
                timestamp=timestamp,
            )

            results = await asyncio.gather(actor.submit(first), actor.submit(second))
            assert sorted(result.result.kind.value for result in results) == ["IGNORED", "OK"]
            state = repository.load_state("A")
            assert len(state.queues[Direction.NORTH]) == 1
            assert set(state.processed_events) == {"parallel-1", "parallel-2"}
        finally:
            await actor.close()
            repository.close()

    asyncio.run(exercise())
