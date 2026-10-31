"""Real go-livepeer integration. Set SWARM_TEST_ORCHESTRATOR to run."""

import asyncio
import os

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from eth_account import Account
from livepeer_gateway.live_runner import register_runner
from livepeer_gateway.selection import reserve_session
from livepeer_swarm.stub import build_app

from livepeer_swarm_radio.state import PublisherState


@pytest.mark.asyncio
async def test_four_trickle_channels_publish_and_ack_without_rotation(tmp_path):
    from livepeer_swarm.service import LiveService

    from livepeer_swarm_radio.transport import PublisherTransport

    url = os.environ.get("SWARM_TEST_ORCHESTRATOR")
    if not url:
        pytest.skip("requires a dedicated offchain orchestrator")
    account = Account.from_key(bytes([1]) * 32)
    batch = bytes([2]) * 32
    app_id = "livepeer-test/swarm"
    async with TestServer(build_app()) as bee:
        service = LiveService(str(bee.make_url("")).rstrip("/"))
        app = web.Application()
        service.attach(app)
        async with TestServer(app) as runner:
            registration = await register_runner(
                url,
                secret="swarm-test-only",
                runner_url=str(runner.make_url("")).rstrip("/"),
                app=app_id,
                auto_detect_gpu=False,
                on_session_release=service.released,
            )
            app["registration"] = registration
            try:
                with PublisherState(
                    tmp_path / "db", account, batch, depth=20, new_batch=True
                ) as state:
                    async with await reserve_session(
                        orchestrators=[url], app=app_id
                    ) as session:
                        async with PublisherTransport(session, state) as transport:
                            # Writer rotation is ten seconds: this must finish before it.
                            result = await asyncio.wait_for(
                                transport.upload(
                                    b"abc" * 2000, {"kind": "media", "duration": 2.0}
                                ),
                                8,
                            )
                            assert len(result["ref"]) == 64
                            command = state.prepare_feed("test-run", b"#EXTM3U\n")
                            await transport.feed(command)
                            assert state.pending_feeds() == []
            finally:
                await registration.close()


@pytest.mark.asyncio
async def test_gateway_capture_to_radio_http_aac(tmp_path):
    import io

    import aiohttp
    import av
    from livepeer_swarm.service import LiveService

    from livepeer_swarm_radio.radio import build_radio_app

    url = os.environ.get("SWARM_TEST_ORCHESTRATOR")
    if not url:
        pytest.skip("requires a dedicated offchain orchestrator")
    from livepeer_swarm_radio import client

    source = tmp_path / "tone.wav"
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-v",
        "error",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:sample_rate=48000",
        "-t",
        "8",
        str(source),
    )
    assert await proc.wait() == 0
    key = tmp_path / "test.key"
    key.write_text("01" * 32)
    account = Account.from_key(bytes([1]) * 32)
    app_id = "livepeer-test/swarm-radio"
    async with TestServer(build_app()) as bee:
        bee_url = str(bee.make_url("")).rstrip("/")
        service = LiveService(bee_url)
        app = web.Application()
        service.attach(app)
        async with TestServer(app) as runner:
            registration = await register_runner(
                url,
                secret="swarm-test-only",
                runner_url=str(runner.make_url("")).rstrip("/"),
                app=app_id,
                auto_detect_gpu=False,
                on_session_release=service.released,
            )
            app["registration"] = registration
            task = asyncio.create_task(
                client.run(
                    {
                        "publisher_key": str(key),
                        "state_path": str(tmp_path / "db"),
                        "batch": {
                            "id": "02" * 32,
                            "depth": 20,
                            "immutable": False,
                            "unused": True,
                        },
                        "station": "smoke",
                        "new_station": True,
                        "orchestrator": url,
                        "app": app_id,
                        "offchain": True,
                        "input": str(source),
                        "read_gateway": bee_url,
                    }
                )
            )
            try:
                async with TestServer(
                    build_radio_app(bee_url, account.address, "smoke")
                ) as radio:
                    async with aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=25)
                    ) as http:
                        async with http.get(radio.make_url("/stream.aac")) as response:
                            assert response.status == 200
                            assert response.headers["Content-Type"] == "audio/aac"
                            audio = await response.read()
                        with av.open(io.BytesIO(audio), format="aac") as decoded:
                            frames = list(decoded.decode(audio=0))
                            assert sum(f.samples for f in frames) / 48000 >= 6
                    await asyncio.wait_for(task, 10)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await registration.close()


@pytest.mark.asyncio
async def test_reconnect_replays_uncertain_feed_without_reusing_index(tmp_path):
    from livepeer_swarm.bee import BeeClient
    from livepeer_swarm.feeds import feed_address
    from livepeer_swarm.service import LiveService

    from livepeer_swarm_radio.transport import PublisherTransport

    url = os.environ.get("SWARM_TEST_ORCHESTRATOR")
    if not url:
        pytest.skip("requires a dedicated offchain orchestrator")
    account = Account.from_key(bytes([1]) * 32)
    batch = bytes([2]) * 32
    app_id = "livepeer-test/swarm-reconnect"
    async with TestServer(build_app()) as bee:
        bee_url = str(bee.make_url("")).rstrip("/")
        service = LiveService(bee_url)
        app = web.Application()
        service.attach(app)
        async with TestServer(app) as runner:
            registration = await register_runner(
                url,
                secret="swarm-test-only",
                runner_url=str(runner.make_url("")).rstrip("/"),
                app=app_id,
                auto_detect_gpu=False,
                on_session_release=service.released,
            )
            app["registration"] = registration
            try:
                with PublisherState(
                    tmp_path / "db", account, batch, depth=20, new_batch=True
                ) as state:
                    command = state.prepare_feed("reconnect", b"first")
                    async with await reserve_session(
                        orchestrators=[url], app=app_id
                    ) as session:
                        async with PublisherTransport(session, state) as transport:
                            await transport.command(
                                command
                            )  # Bee accepted it; client has not committed the ack.
                    async with BeeClient(bee_url) as reader:
                        address = feed_address(account.address, "reconnect", 0)
                        before = await reader.read("/chunks/" + address.hex())
                    # Close the database too: this is an actual process-state recovery.
                with PublisherState(
                    tmp_path / "db", account, batch, depth=20
                ) as recovered:
                    assert recovered.pending_feeds() == [command]
                    async with await reserve_session(
                        orchestrators=[url], app=app_id
                    ) as session:
                        async with PublisherTransport(session, recovered) as transport:
                            await transport.feed(recovered.pending_feeds()[0])
                    async with BeeClient(bee_url) as reader:
                        assert await reader.read("/chunks/" + address.hex()) == before
                    assert recovered.prepare_feed("reconnect", b"second")["index"] == 1
            finally:
                await registration.close()
