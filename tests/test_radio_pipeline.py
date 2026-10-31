import asyncio
import time

import pytest

from livepeer_swarm_radio.radio import Radio


def entries(count, duration=0.08):
    return [
        dict(sequence=i, duration=duration, ref=str(i), captured_at=time.time())
        for i in range(count)
    ]


@pytest.mark.asyncio
async def test_prefetch_keeps_order_during_discovery_stall_without_short_backlog_skip():
    class ControlledRadio(Radio):
        async def refresh(self):
            if self.playlist is None:
                self.topic, self.generation = "broadcast", 1
                self.playlist = ("init", entries(2), False)
            else:
                # Discovery is blocked while playback consumes already-known media.
                await asyncio.Event().wait()

        async def retrieve(self, init_ref, entry):
            await asyncio.sleep(0.005)
            return str(entry["sequence"]).encode(), 0.005

    radio = ControlledRadio(None, "owner", "station")
    listener = radio.subscribe()
    task = asyncio.create_task(radio.run())
    received, timestamps = [], []
    try:
        async with asyncio.timeout(3):
            received.append(await listener.get())
            timestamps.append(time.monotonic())
            # A four-segment backlog used to trigger an audible skip.
            radio.playlist = ("init", entries(6), True)
            while (audio := await listener.get()) is not None:
                received.append(audio)
                timestamps.append(time.monotonic())
        assert received == [str(i).encode() for i in range(6)]
        assert radio.gaps == 0
        assert radio.prefetch.maxsize == 2
        assert min(b - a for a, b in zip(timestamps, timestamps[1:])) >= 0.065
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_broadcast_change_discards_old_prefetched_audio():
    radio = Radio(None, "owner", "station")
    radio.generation = 2
    queue = radio.subscribe()
    await radio.prefetch.put((1, entries(1)[0], (b"old", 0)))
    await radio.prefetch.put((2, entries(1)[0], (b"new", 0)))
    task = asyncio.create_task(radio.play())
    try:
        # A terminating marker allows the one-segment broadcast to prime immediately.
        await radio.prefetch.put((2, None, None))
        async with asyncio.timeout(1):
            assert await queue.get() == b"new"
            assert await queue.get() is None
        assert radio.ended
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancellation_stops_all_three_stages():
    stopped = set()

    class WaitingRadio(Radio):
        async def wait(self, name):
            try:
                await asyncio.Event().wait()
            finally:
                stopped.add(name)

        async def discover(self):
            await self.wait("discovery")

        async def fetch(self):
            await self.wait("fetch")

        async def play(self):
            await self.wait("playback")

    radio = WaitingRadio(None, "owner", "station")
    task = asyncio.create_task(radio.run())
    await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert stopped == {"discovery", "fetch", "playback"}
