"""One bounded Swarm retrieval pipeline, fanned out as continuous ADTS AAC."""

import asyncio
import json
import logging
import time
from contextlib import suppress

from aiohttp import web
from livepeer_swarm.bee import BeeClient

from .audio import remux_adts
from .hls import parse_media

log = logging.getLogger(__name__)


class Radio:
    def __init__(self, bee, owner, station):
        self.bee, self.owner, self.station = bee, owner, station
        self.topic = None
        self.pointer_index = self.feed_index = 0
        self.next_pointer_check = 0.0
        self.sequence = -1
        self.listeners = set()
        self.init_ref = None
        self.init = None
        self.last_audio = 0.0
        self.failures = 0
        self.missing = {}
        self.ended = False
        self.latency_s = None
        self.dropped_listeners = 0
        self.gaps = 0
        self.generation = 0
        self.playlist = None
        self.prefetch = asyncio.Queue(maxsize=2)
        self.underruns = 0
        self.skipped_segments = 0

    def subscribe(self):
        queue = asyncio.Queue(maxsize=3)
        self.listeners.add(queue)
        return queue

    def unsubscribe(self, queue):
        self.listeners.discard(queue)

    def emit(self, audio):
        for queue in list(self.listeners):
            if queue.full():
                self.listeners.discard(queue)
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(None)
                self.dropped_listeners += 1
            else:
                queue.put_nowait(audio)

    async def refresh(self):
        """Discover authenticated playlists independently of audio deadlines."""
        if self.topic is None or time.monotonic() >= self.next_pointer_check:
            pointer = await self.bee.latest_feed(
                self.owner, self.station, floor=self.pointer_index
            )
            if pointer:
                index, body = pointer
                topic = json.loads(body)["topic"]
                if not isinstance(topic, str) or not 1 <= len(topic) <= 256:
                    raise ValueError("invalid station topic")
                if topic != self.topic:
                    master = await self.bee.latest_feed(self.owner, topic)
                    if not master or "source.m3u8" not in master[1].decode():
                        raise ValueError(
                            "station master has no supported source rendition"
                        )
                    self.generation += 1
                    self.topic, self.sequence, self.feed_index = topic, -1, 0
                    self.init_ref, self.init, self.ended = None, None, False
                    self.playlist = None
                    self.missing.clear()
                    while not self.prefetch.empty():
                        self.prefetch.get_nowait()
                self.pointer_index = index
            self.next_pointer_check = time.monotonic() + 5
        if self.topic is not None:
            latest = await self.bee.latest_feed(
                self.owner, self.topic + "#source", floor=self.feed_index
            )
            if latest:
                index, body = latest
                playlist = parse_media(body.decode())
                self.feed_index, self.playlist = index, playlist

    async def retrieve(self, init_ref, entry):
        started = time.monotonic()
        if init_ref != self.init_ref:
            init = await self.bee.read_bytes(init_ref, limit=65536)
            if init is None:
                return None
            self.init_ref, self.init = init_ref, init
        init = self.init
        fragment = await self.bee.read_bytes(entry["ref"], limit=256 * 1024)
        if fragment is None:
            return None
        audio = await asyncio.to_thread(remux_adts, init, fragment)
        return audio, time.monotonic() - started

    def deliver(self, entry, result):
        audio, retrieval = result
        if self.sequence >= 0 and entry["sequence"] != self.sequence + 1:
            self.gaps += 1
            self.skipped_segments += max(0, entry["sequence"] - self.sequence - 1)
        self.sequence = entry["sequence"]
        self.last_audio = time.time()
        self.latency_s = (
            None
            if entry["captured_at"] is None
            else self.last_audio - entry["captured_at"]
        )
        self.emit(audio)
        log.info(
            "play sequence=%s retrieval_s=%.3f capture_to_emit_s=%s",
            self.sequence,
            retrieval,
            self.latency_s,
        )

    async def poll(self):
        """Single-step helper; the running service uses the independent stages below."""
        started = time.monotonic()
        await self.refresh()
        if self.playlist is None:
            return 0.5
        init_ref, entries, ended = self.playlist
        remaining = [e for e in entries if e["sequence"] > self.sequence]
        if not remaining:
            if ended and not self.ended:
                self.ended = True
                self.emit(None)
            return 0.5
        if self.sequence < 0:
            remaining = remaining[-2:]
        entry = remaining[0]
        result = await self.retrieve(init_ref, entry)
        if result is not None:
            self.deliver(entry, result)
        return max(0.01, entry["duration"] - (time.monotonic() - started))

    async def discover(self):
        while True:
            try:
                await self.refresh()
            except Exception:
                self.failures += 1
                log.exception("Swarm playlist discovery failed")
            await asyncio.sleep(0.25)

    async def fetch(self):
        generation, cursor, ended_sent = -1, -1, False
        while True:
            if generation != self.generation:
                generation, cursor, ended_sent = self.generation, -1, False
            if self.playlist is None:
                await asyncio.sleep(0.05)
                continue
            init_ref, entries, ended = self.playlist
            remaining = [e for e in entries if e["sequence"] > cursor]
            if not remaining:
                if ended and not ended_sent:
                    await self.prefetch.put((generation, None, None))
                    ended_sent = True
                await asyncio.sleep(0.05)
                continue
            if cursor < 0:
                # A cold Bee can expose an authenticated but stale frontier.
                # Wait for recent live metadata instead of buffering obsolete audio.
                newest = remaining[-1]
                captured = newest["captured_at"]
                if not ended and captured is not None and time.time() - captured > 8:
                    await asyncio.sleep(0.1)
                    continue
                remaining = remaining[-2:] if ended else remaining[-1:]
            # Tolerate short jitter; only abandon content after >12s backlog.
            elif sum(e["duration"] for e in remaining) > 12:
                log.warning("rejoining live edge after >12s retrieval backlog")
                remaining = remaining[-2:]
            entry = remaining[0]
            try:
                result = await self.retrieve(init_ref, entry)
            except Exception:
                result = None
                log.exception("Swarm audio retrieval failed")
            if generation != self.generation:
                continue
            if result is None:
                self.failures += 1
                count = self.missing.get(entry["sequence"], 0) + 1
                self.missing = {entry["sequence"]: count}
                if count >= 6:
                    cursor = entry["sequence"]
                await asyncio.sleep(0.25)
                continue
            self.missing.clear()
            await self.prefetch.put((generation, entry, result))
            cursor = entry["sequence"]

    async def play(self):
        deadline, generation = None, -1
        while True:
            if deadline is not None:
                await asyncio.sleep(max(0, deadline - time.monotonic()))
                if self.prefetch.empty():
                    self.underruns += 1
            item_generation, entry, result = await self.prefetch.get()
            if item_generation != self.generation:
                continue
            if generation != item_generation:
                generation, deadline = item_generation, None
            if entry is None:
                self.ended = True
                self.emit(None)
                deadline = None
                continue
            # Prime one segment ahead, with a finite wait for sparse/short streams.
            if deadline is None:
                until = time.monotonic() + 2
                while self.prefetch.empty() and time.monotonic() < until:
                    await asyncio.sleep(0.02)
                if item_generation != self.generation:
                    continue
            self.deliver(entry, result)
            # Reset after a real stall: never burst several delayed segments at VLC.
            deadline = max(deadline or 0, time.monotonic()) + entry["duration"]

    async def run(self):
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self.discover())
            tasks.create_task(self.fetch())
            tasks.create_task(self.play())

    def status(self):
        return {
            "owner": self.owner,
            "station": self.station,
            "topic": self.topic,
            "sequence": self.sequence,
            "feed_index": self.feed_index,
            "ended": self.ended,
            "playing": time.time() - self.last_audio < 10 and not self.ended,
            "retrieval_failures": self.failures,
            "gaps": self.gaps,
            "skipped_segments": self.skipped_segments,
            "playback_underruns": self.underruns,
            "prefetched_segments": self.prefetch.qsize(),
            "listeners": len(self.listeners),
            "slow_listeners_disconnected": self.dropped_listeners,
            "buffered_seconds": max((q.qsize() * 2 for q in self.listeners), default=0),
            "capture_to_emit_seconds": self.latency_s,
        }


def build_radio_app(bee_url, owner, station):
    app = web.Application()

    async def startup(app):
        app["bee"] = BeeClient(bee_url)
        app["radio"] = Radio(app["bee"], owner, station)
        app["task"] = asyncio.create_task(app["radio"].run())

    async def cleanup(app):
        app["task"].cancel()
        with suppress(asyncio.CancelledError):
            await app["task"]
        app["radio"].emit(None)
        await app["bee"].close()

    async def status(request):
        state = request.app["radio"].status()
        return web.json_response(
            state, status=200 if request.path == "/status" or state["playing"] else 503
        )

    async def stream(request):
        radio = request.app["radio"]
        queue = radio.subscribe()
        response = web.StreamResponse(
            headers={
                "Content-Type": "audio/aac",
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
            }
        )
        try:
            try:
                first = await asyncio.wait_for(queue.get(), 10)
            except TimeoutError:
                raise web.HTTPServiceUnavailable(
                    text="station has no available audio"
                ) from None
            if first is None:
                raise web.HTTPServiceUnavailable(text="broadcast ended")
            await response.prepare(request)
            await asyncio.wait_for(response.write(first), 5)
            while True:
                audio = await asyncio.wait_for(queue.get(), 15)
                if audio is None:
                    break
                await asyncio.wait_for(response.write(audio), 5)
        except (ConnectionError, TimeoutError):
            pass
        finally:
            radio.unsubscribe(queue)
        return response

    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)
    app.router.add_get("/stream.aac", stream)
    app.router.add_get("/healthz", status)
    app.router.add_get("/status", status)
    return app
