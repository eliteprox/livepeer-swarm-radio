"""Client-owned playlist timeline. The runner never constructs a feed update."""

import asyncio
import json
import time
import uuid

from .hls import master_playlist, media_playlist


class Broadcast:
    def __init__(
        self,
        state,
        station,
        *,
        first_index=0,
        gateway="https://api.gateway.ethswarm.org",
    ):
        if not station or len(station) > 128:
            raise ValueError("station must contain 1..128 characters")
        self.state, self.station, self.first_index = state, station, first_index
        self.gateway = gateway
        self.topic = f"{station}/{int(time.time())}-{uuid.uuid4().hex[:12]}"
        self.entries = []
        self.init_ref = None
        self.source_sequence = None
        self.last_source_id = None
        self.discontinuities = 0
        self.next_sequence = 0
        self.announced = False
        self.master_written = False

    def snapshot(self):
        return {
            "topic": self.topic,
            "entries": self.entries,
            "init_ref": self.init_ref,
            "next_sequence": self.next_sequence,
            "discontinuities": self.discontinuities,
            "master_written": self.master_written,
            "announced": self.announced,
        }

    async def save(self):
        await asyncio.to_thread(
            self.state.set, "broadcast:" + self.station, self.snapshot()
        )

    async def reconcile(self, transport):
        for command in await asyncio.to_thread(self.state.pending_feeds):
            await transport.feed(command)

    async def publish_feed(self, transport, topic, body, **kwargs):
        command = await asyncio.to_thread(
            self.state.prepare_feed, topic, body, **kwargs
        )
        await transport.feed(command)

    async def announce(self, transport):
        if not self.master_written:
            await self.publish_feed(transport, self.topic, master_playlist().encode())
            self.master_written = True
            await self.save()
        if not self.announced:
            await self.publish_feed(
                transport,
                self.station,
                json.dumps({"topic": self.topic}).encode(),
                first_index=self.first_index,
            )
            self.announced = True
            await self.save()

    async def append(self, transport, segment):
        source_id = (segment.sequence, segment.captured_at)
        if source_id == self.last_source_id:
            # Media succeeded before a lost feed acknowledgment. Reconcile first.
            await self.reconcile(transport)
            await self.announce(transport)
            return
        if self.init_ref is None:
            self.init_ref = (
                await transport.upload(segment.init, {"kind": "init", "duration": 0})
            )["ref"]
        result = await transport.upload(
            segment.data, {"kind": "media", "duration": segment.duration}
        )
        discontinuity = (
            self.source_sequence is not None
            and segment.sequence != self.source_sequence + 1
        )
        if discontinuity:
            self.discontinuities += 1
        self.entries.append(
            {
                "sequence": self.next_sequence,
                "duration": segment.duration,
                "ref": result["ref"],
                "captured_at": segment.captured_at,
                "discontinuity": discontinuity,
                "discontinuity_sequence": self.discontinuities,
            }
        )
        self.entries = self.entries[-12:]
        self.next_sequence += 1
        self.source_sequence = segment.sequence
        self.last_source_id = source_id
        # Persist the exact playlist as a pending signed write before moving on.
        command = await asyncio.to_thread(
            self.state.prepare_feed,
            self.topic + "#source",
            media_playlist(self.entries, self.init_ref, gateway=self.gateway).encode(),
        )
        await self.save()
        await transport.feed(command)
        await self.announce(transport)

    async def finish(self, transport):
        if self.entries:
            await self.publish_feed(
                transport,
                self.topic + "#source",
                media_playlist(
                    self.entries, self.init_ref, end=True, gateway=self.gateway
                ).encode(),
            )
            await self.save()
