"""Gateway-side Trickle transport. The publisher and Livepeer payer are separate."""

import asyncio
import logging
import time
import uuid
from contextlib import suppress

from livepeer_gateway.channel_reader import JSONLReader
from livepeer_gateway.channel_writer import JSONLWriter
from livepeer_gateway.http import post_json
from livepeer_gateway.trickle_publisher import TricklePublisher
from livepeer_swarm.bmt import chunk_address, pyramid
from livepeer_swarm.live import MAX_UPLOAD_BYTES

log = logging.getLogger(__name__)


class PublisherTransport:
    def __init__(self, session, state):
        self.session, self.state = session, state
        self.waiters, self.allowed = {}, {}
        self.sign_queue = asyncio.Queue(maxsize=8)
        self.tasks = []
        self.closed = False
        self.error = None
        self.writers = []

    async def __aenter__(self):
        try:
            ch = await post_json(
                self.session.app_url.rstrip("/") + "/start",
                {
                    "owner": self.state.signer.address,
                    "batch_id": self.state.batch.hex(),
                },
            )
            self.sid = ch["session"]
            self.control, self.stamps = (
                JSONLWriter(ch["control"]),
                JSONLWriter(ch["stamps"]),
            )
            self.media = TricklePublisher(ch["in"], "application/octet-stream")
            self.writers = [self.media, self.control, self.stamps]
            self.tasks = [
                asyncio.create_task(self.events(ch["events"])),
                asyncio.create_task(self.sign()),
            ]
            for task in self.tasks:
                task.add_done_callback(self.failed)
            await self.command({"op": "ready", "request_id": uuid.uuid4().hex})
            return self
        except BaseException:
            await self.close()
            raise

    def failed(self, task):
        if self.closed or task.cancelled():
            return
        self.error = task.exception() or RuntimeError("Trickle event stream ended")
        for future in self.waiters.values():
            if not future.done():
                future.set_exception(self.error)

    async def events(self, url):
        async for message in JSONLReader(url)(start_seq=0):
            if message.get("session") != self.sid:
                raise ValueError("event session mismatch")
            kind = message.get("type")
            if kind == "stamp-request":
                self.sign_queue.put_nowait(message)
            elif kind == "error":
                raise RuntimeError(message.get("detail", "runner failed"))
            else:
                key = (
                    message.get("upload_id")
                    if kind == "uploaded"
                    else message.get("request_id")
                )
                future = self.waiters.get(key)
                if future and not future.done():
                    future.set_result(message)

    async def sign(self):
        while True:
            message = await self.sign_queue.get()
            allowed = self.allowed.get(message["upload_id"])
            if allowed is None or message["addresses"] != allowed:
                raise ValueError(
                    "runner requested stamps for bytes the publisher did not submit"
                )
            stamps = await asyncio.to_thread(
                self.state.stamps, [bytes.fromhex(a) for a in allowed]
            )
            await self.stamps.write(
                {
                    "session": self.sid,
                    "request_id": message["request_id"],
                    "stamps": stamps,
                }
            )

    def future(self, key):
        if self.error:
            raise self.error
        future = asyncio.get_running_loop().create_future()
        self.waiters[key] = future
        return future

    async def command(self, message):
        key = message["request_id"]
        future = self.future(key)
        try:
            await self.control.write({**message, "session": self.sid})
            return await asyncio.wait_for(future, 30)
        finally:
            self.waiters.pop(key, None)

    async def upload(self, payload, metadata):
        if not payload or len(payload) > MAX_UPLOAD_BYTES:
            raise ValueError("upload exceeds media size limit")
        uid = uuid.uuid4().hex
        self.allowed[uid] = await asyncio.to_thread(
            lambda: [chunk_address(span, body).hex() for span, body in pyramid(payload)]
        )
        future = self.future(uid)
        try:
            await self.control.write(
                {**metadata, "op": "meta", "session": self.sid, "upload_id": uid}
            )
            async with await self.media.next() as segment:
                await segment.write(uid.encode("ascii") + payload)
            result = await asyncio.wait_for(future, 30)
            if result["ref"] != self.allowed[uid][-1]:
                raise ValueError("runner acknowledged a different media reference")
            log.info(
                "upload=%s kind=%s signing_s=%.3f publish_s=%.3f",
                uid,
                metadata.get("kind"),
                result["signing_s"],
                result["publish_s"],
            )
            return result
        finally:
            self.allowed.pop(uid, None)
            self.waiters.pop(uid, None)

    async def feed(self, command):
        started = time.monotonic()
        result = await self.command(command)
        if result.get("type") != "feed" or result.get("ref") != command["address"]:
            raise ValueError("feed acknowledgment mismatch")
        await asyncio.to_thread(self.state.ack_feed, command["request_id"])
        log.info(
            "feed=%s index=%s publication_s=%.3f",
            command["topic"],
            command["index"],
            time.monotonic() - started,
        )
        return result

    async def close(self):
        if self.closed:
            return
        self.closed = True
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for writer in self.writers:
            with suppress(Exception):
                await writer.close()

    async def __aexit__(self, *_):
        if not self.error:
            with suppress(Exception):
                await asyncio.wait_for(
                    self.command({"op": "stop", "request_id": uuid.uuid4().hex}), 2
                )
        await self.close()
