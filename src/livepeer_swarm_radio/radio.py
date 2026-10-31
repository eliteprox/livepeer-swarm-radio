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

    async def poll(self):
        poll_started = time.monotonic()
        if self.topic is None or poll_started >= self.next_pointer_check:
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
                    self.topic, self.sequence, self.feed_index = topic, -1, 0
                    self.init_ref, self.init, self.ended = None, None, False
                    self.missing.clear()
                self.pointer_index = index
            self.next_pointer_check = time.monotonic() + 5
        if self.topic is None:
            return 0.5
        latest = await self.bee.latest_feed(
            self.owner, self.topic + "#source", floor=self.feed_index
        )
        if not latest:
            return 0.5
        self.feed_index, body = latest
        init_ref, entries, ended = parse_media(body.decode())
        remaining = [e for e in entries if e["sequence"] > self.sequence]
        if not remaining:
            if ended and not self.ended:
                self.ended = True
                self.emit(None)
            return 0.5
        # Start near live; after falling behind rejoin the latest two segments.
        if self.sequence < 0 or len(remaining) > 3:
            remaining = remaining[-2:]
        entry = remaining[0]
        started = time.monotonic()
        if init_ref != self.init_ref:
            init = await self.bee.read_bytes(init_ref, limit=65536)
            if init is None:
                return 0.5
            self.init_ref, self.init = init_ref, init
        fragment = await self.bee.read_bytes(entry["ref"], limit=256 * 1024)
        if fragment is None:
            self.failures += 1
            count = self.missing.get(entry["sequence"], 0) + 1
            self.missing = {entry["sequence"]: count}
            if count >= 6:
                self.sequence = entry["sequence"]
                self.gaps += 1
            return 0.5
        audio = await asyncio.to_thread(remux_adts, self.init, fragment)
        if self.sequence >= 0 and entry["sequence"] != self.sequence + 1:
            self.gaps += 1
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
            time.monotonic() - started,
            self.latency_s,
        )
        return max(0.01, entry["duration"] - (time.monotonic() - poll_started))

    async def run(self):
        while True:
            try:
                delay = await self.poll()
            except Exception:
                self.failures += 1
                log.exception("Swarm radio retrieval failed")
                delay = 0.5
            await asyncio.sleep(delay)

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
