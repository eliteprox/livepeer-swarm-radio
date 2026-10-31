import json
from pathlib import Path

import pytest
from eth_account import Account


def test_soc_matches_bee_oracle():
    from livepeer_swarm.feeds import sign_feed, verify_soc

    v = json.loads((Path(__file__).parent / "fixtures/swarm_vectors.json").read_text())
    f = v["feed"]
    signed = sign_feed(
        f["topic_name"],
        f["index"],
        b"#EXTM3U\n#EXT-X-VERSION:7\n",
        Account.from_key(v["key"]),
    )
    assert signed.address.hex() == f["soc_address"].removeprefix("0x")
    assert signed.data.hex() == f["soc_data"].removeprefix("0x")
    assert verify_soc(signed.data, signed.address) == b"#EXTM3U\n#EXT-X-VERSION:7\n"


def test_stamp_allocation_survives_restart(tmp_path):
    from livepeer_swarm_radio.state import PublisherState

    account = Account.from_key(bytes([1]) * 32)
    batch = bytes([2]) * 32
    address = bytes([0]) * 32
    other = bytes(31) + b"\x01"
    path = tmp_path / "state.sqlite"
    with PublisherState(path, account, batch, depth=17, new_batch=True) as state:
        first = state.stamps([address])[0]
    with PublisherState(path, account, batch, depth=17) as state:
        assert state.stamps([address])[0] == first
        second = state.stamps([other])[0]
        assert int.from_bytes(bytes.fromhex(second)[36:40], "big") == 1


def test_pending_feed_is_identical_after_restart_until_ack(tmp_path):
    import pytest

    from livepeer_swarm_radio.state import PublisherState

    account = Account.from_key(bytes([1]) * 32)
    batch = bytes([2]) * 32
    path = tmp_path / "state.sqlite"
    with PublisherState(path, account, batch, depth=20, new_batch=True) as state:
        first = state.prepare_feed("station/run#source", b"playlist one")
    with PublisherState(path, account, batch, depth=20) as state:
        assert state.pending_feeds() == [first]
        with pytest.raises(ValueError, match="pending"):
            state.prepare_feed("station/run#source", b"playlist two")
        state.ack_feed(first["request_id"])
        second = state.prepare_feed("station/run#source", b"playlist two")
        assert second["index"] == first["index"] + 1


@pytest.mark.asyncio
async def test_bee_upload_and_verified_feed_read(tmp_path):
    from aiohttp.test_utils import TestServer
    from livepeer_swarm.bee import BeeClient
    from livepeer_swarm.stub import build_app

    from livepeer_swarm_radio.state import PublisherState

    account = Account.from_key(bytes([1]) * 32)
    with PublisherState(
        tmp_path / "db", account, bytes([2]) * 32, depth=20, new_batch=True
    ) as state:
        command = state.prepare_feed("radio", b'{"topic":"radio/run"}')
        async with TestServer(build_app()) as server:
            async with BeeClient(str(server.make_url("")).rstrip("/")) as bee:
                await bee.put_soc(command)
                latest = await bee.latest_feed(account.address, "radio")
                assert latest == (0, b'{"topic":"radio/run"}')


@pytest.mark.asyncio
async def test_runner_only_acknowledges_retrievable_media(tmp_path):
    from aiohttp.test_utils import TestServer
    from livepeer_swarm.bee import BeeClient
    from livepeer_swarm.live import UploadPipeline
    from livepeer_swarm.stub import build_app

    from livepeer_swarm_radio.state import PublisherState

    account = Account.from_key(bytes([1]) * 32)
    batch = bytes([2]) * 32
    events = []
    with PublisherState(
        tmp_path / "db", account, batch, depth=20, new_batch=True
    ) as state:
        async with (
            TestServer(build_app()) as server,
            BeeClient(str(server.make_url("")).rstrip("/")) as bee,
        ):

            async def event(message):
                events.append(message)
                if message["type"] == "stamp-request":
                    pipeline.answer(
                        {
                            "request_id": message["request_id"],
                            "stamps": state.stamps(
                                [bytes.fromhex(a) for a in message["addresses"]]
                            ),
                        }
                    )

            pipeline = UploadPipeline(bee, account.address, batch, event)
            payload = b"audio" * 2000
            result = await pipeline.publish("segment-1", payload)
            assert await bee.read("/bytes/" + result["ref"]) == payload
            assert result["type"] == "uploaded"
            assert await pipeline.publish("segment-1", payload) == result
            assert len([e for e in events if e["type"] == "stamp-request"]) == 1


@pytest.mark.asyncio
async def test_audio_capture_is_decodable_as_continuous_adts(tmp_path):
    import asyncio
    import io

    import av

    from livepeer_swarm_radio.audio import capture, remux_adts

    source = tmp_path / "tone.wav"
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-v",
        "error",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:sample_rate=48000",
        "-t",
        "4.2",
        str(source),
    )
    assert await process.wait() == 0
    segments = [s async for s in capture(str(source), paced=False)]
    assert len(segments) >= 2
    assert 4.1 < sum(s.duration for s in segments) < 4.3
    encoded = b"".join(remux_adts(s.init, s.data) for s in segments)
    with av.open(io.BytesIO(encoded), format="aac") as media:
        frames = list(media.decode(audio=0))
        assert sum(f.samples for f in frames) / frames[0].sample_rate > 4.1


def test_playlist_window_preserves_absolute_timeline():
    from livepeer_swarm_radio.hls import media_playlist, parse_media

    entries = [
        {
            "sequence": i,
            "duration": 2.0,
            "ref": f"{i:064x}",
            "captured_at": 1700000000 + 2 * i,
            "discontinuity": i == 1,
            "discontinuity_sequence": int(i >= 1),
        }
        for i in range(4)
    ]
    text = media_playlist(entries, "ab" * 32, window=2)
    assert "#EXT-X-MEDIA-SEQUENCE:2" in text
    assert "#EXT-X-DISCONTINUITY-SEQUENCE:1" in text
    init, parsed, ended = parse_media(text)
    assert init == "ab" * 32
    assert [s["sequence"] for s in parsed] == [2, 3]
    assert parsed[0]["captured_at"] == 1700000004
    assert not ended


@pytest.mark.asyncio
async def test_radio_reads_swarm_feeds_and_emits_aac(tmp_path):
    import asyncio
    import io

    import av
    from aiohttp.test_utils import TestServer
    from livepeer_swarm.bee import BeeClient
    from livepeer_swarm.stamp import chunk_and_stamp
    from livepeer_swarm.stub import build_app

    from livepeer_swarm_radio.audio import capture
    from livepeer_swarm_radio.hls import master_playlist, media_playlist
    from livepeer_swarm_radio.radio import Radio
    from livepeer_swarm_radio.state import PublisherState

    source = tmp_path / "tone.wav"
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-v",
        "error",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=880:sample_rate=48000",
        "-t",
        "2.2",
        str(source),
    )
    assert await proc.wait() == 0
    segments = [s async for s in capture(str(source), paced=False)]
    sample = segments[0]
    account = Account.from_key(bytes([1]) * 32)
    batch = bytes([2]) * 32
    with PublisherState(
        tmp_path / "db", account, batch, depth=20, new_batch=True
    ) as state:
        async with (
            TestServer(build_app()) as server,
            BeeClient(str(server.make_url("")).rstrip("/")) as bee,
        ):

            async def put(data):
                chunks = chunk_and_stamp(data, account, batch)
                stamps = state.stamps([c.address for c in chunks])
                for c, stamp in zip(chunks, stamps):
                    await bee.put_chunk(c.chunk, stamp)
                return chunks[-1].address.hex()

            init = await put(sample.init)
            ref = await put(sample.data)
            playlist = media_playlist(
                [
                    {
                        "sequence": 0,
                        "duration": sample.duration,
                        "ref": ref,
                        "captured_at": sample.captured_at,
                        "discontinuity": False,
                        "discontinuity_sequence": 0,
                    }
                ],
                init,
            )
            for topic, content in [
                ("radio/run#source", playlist),
                ("radio/run", master_playlist()),
                ("radio", '{"topic":"radio/run"}'),
            ]:
                command = state.prepare_feed(topic, content.encode())
                await bee.put_soc(command)
                state.ack_feed(command["request_id"])
            radio = Radio(bee, account.address, "radio")
            queue = radio.subscribe()
            await radio.poll()
            audio = queue.get_nowait()
            with av.open(io.BytesIO(audio), format="aac") as decoded:
                assert sum(frame.samples for frame in decoded.decode(audio=0)) > 48000
            await radio.poll()
            assert queue.empty(), "duplicate playlists must not replay audio"


@pytest.mark.asyncio
async def test_broadcast_advertises_only_after_media_and_master(tmp_path):
    from livepeer_swarm_radio.audio import AudioSegment
    from livepeer_swarm_radio.broadcast import Broadcast
    from livepeer_swarm_radio.state import PublisherState

    account = Account.from_key(bytes([1]) * 32)
    calls = []
    with PublisherState(
        tmp_path / "db", account, bytes([2]) * 32, depth=20, new_batch=True
    ) as state:

        class Transport:
            async def upload(self, body, metadata):
                calls.append(metadata["kind"])
                return {"ref": ("ab" if metadata["kind"] == "init" else "cd") * 32}

            async def feed(self, command):
                calls.append(command["topic"])
                state.ack_feed(command["request_id"])

        broadcast = Broadcast(state, "station", first_index=0)
        await broadcast.append(
            Transport(), AudioSegment(0, 2.0, 1700000000, b"init", b"media")
        )
        assert calls == [
            "init",
            "media",
            broadcast.topic + "#source",
            broadcast.topic,
            "station",
        ]
        assert state.get("broadcast:station")["entries"][0]["sequence"] == 0


@pytest.mark.parametrize("immutable", [True, False])
def test_batch_capacity_and_slot_eviction(tmp_path, immutable):
    from livepeer_swarm.stamp import BucketFull

    from livepeer_swarm_radio.state import PublisherState

    account = Account.from_key(bytes([1]) * 32)
    addresses = [bytes(31) + bytes([i]) for i in range(3)]
    with PublisherState(
        tmp_path / "db",
        account,
        bytes([2]) * 32,
        depth=17,
        immutable=immutable,
        new_batch=True,
    ) as state:
        original = state.stamps(addresses[:2])
        if immutable:
            with pytest.raises(BucketFull):
                state.stamps([addresses[2]])
            assert state.stamps(addresses[:2]) == original
        else:
            third = bytes.fromhex(state.stamps([addresses[2]])[0])
            assert int.from_bytes(third[36:40], "big") == 0
            reissued = bytes.fromhex(state.stamps([addresses[0]])[0])
            assert int.from_bytes(reissued[36:40], "big") == 1


def test_import_counters_and_exclusive_batch_lock(tmp_path):
    from livepeer_swarm_radio.state import PublisherState

    account = Account.from_key(bytes([1]) * 32)
    batch = bytes([2]) * 32
    counters = tmp_path / "counters.json"
    counters.write_text(json.dumps({"buckets": {"0": 5}, "slots": {}}))
    with PublisherState(
        tmp_path / "db", account, batch, depth=20, import_counters=counters
    ) as state:
        with pytest.raises(ValueError, match="active publisher"):
            PublisherState(
                tmp_path / "other-db", account, batch, depth=20, new_batch=True
            )
        stamp = bytes.fromhex(state.stamps([bytes(32)])[0])
        assert int.from_bytes(stamp[36:40], "big") == 5
    with pytest.raises(ValueError, match="counter import"):
        PublisherState(tmp_path / "missing-db", account, batch, depth=20)


def test_soc_tampering_is_rejected():
    from livepeer_swarm.feeds import sign_feed, verify_soc

    signed = sign_feed("radio", 0, b"valid payload", Account.from_key(bytes([1]) * 32))
    with pytest.raises(ValueError):
        verify_soc(signed.data[:-1] + b"X", signed.address)


@pytest.mark.asyncio
async def test_feed_frontier_hint_recovers_when_old_chunks_are_gone(tmp_path):
    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from livepeer_swarm.bee import BeeClient
    from livepeer_swarm.stub import build_app

    from livepeer_swarm_radio.state import PublisherState

    account = Account.from_key(bytes([1]) * 32)
    with PublisherState(
        tmp_path / "db", account, bytes([2]) * 32, depth=20, new_batch=True
    ) as state:
        app = build_app()

        async def hint(request):
            return web.Response(headers={"Swarm-Feed-Index": "0000000000000005"})

        app.router.add_get("/feeds/{owner}/{topic}", hint)
        async with (
            TestServer(app) as server,
            BeeClient(str(server.make_url("")).rstrip("/")) as bee,
        ):
            command = state.prepare_feed("old-radio", b"newest", first_index=5)
            await bee.put_soc(command)
            assert await bee.latest_feed(account.address, "old-radio") == (5, b"newest")


@pytest.mark.asyncio
async def test_media_reference_is_verified_before_playback():
    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from livepeer_swarm.bee import BeeClient

    app = web.Application()

    async def corrupted(request):
        return web.Response(body=b"corrupted audio")

    app.router.add_get("/bytes/{ref}", corrupted)
    async with (
        TestServer(app) as server,
        BeeClient(str(server.make_url("")).rstrip("/")) as bee,
    ):
        with pytest.raises(ValueError, match="reference"):
            await bee.read_bytes("ab" * 32)


@pytest.mark.asyncio
async def test_paced_capture_never_emits_future_audio(tmp_path):
    import asyncio
    import time

    from livepeer_swarm_radio.audio import capture

    source = tmp_path / "paced.wav"
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-v",
        "error",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:sample_rate=48000",
        "-t",
        "4.1",
        str(source),
    )
    assert await process.wait() == 0
    count = 0
    async for segment in capture(str(source)):
        assert segment.captured_at + segment.duration <= time.time(), (
            "audio has not yet elapsed"
        )
        count += 1
    assert count >= 3


@pytest.mark.asyncio
async def test_frontier_timeout_preserves_authenticated_payload(tmp_path):
    import asyncio

    from aiohttp import web
    from aiohttp.test_utils import TestServer
    from livepeer_swarm.bee import BeeClient
    from livepeer_swarm.feeds import sign_feed

    signed = sign_feed(
        "slow-frontier", 0, b"available", Account.from_key(bytes([1]) * 32)
    )
    reads = []

    async def chunks(request):
        reads.append(request.match_info["address"])
        if request.match_info["address"] == signed.address.hex():
            return web.Response(body=signed.data)
        await asyncio.sleep(0.2)
        return web.Response(status=404)

    app = web.Application()
    app.router.add_get("/chunks/{address}", chunks)
    async with (
        TestServer(app) as server,
        BeeClient(str(server.make_url("")).rstrip("/"), timeout=0.03) as bee,
    ):
        assert await bee.latest_feed(signed.owner, "slow-frontier") == (0, b"available")
        assert await bee.latest_feed(signed.owner, "slow-frontier") == (0, b"available")
        assert reads.count(signed.address.hex()) == 1


def test_media_uris_resolve_outside_external_radio_feed_namespace():
    from urllib.parse import urljoin

    from livepeer_swarm_radio.hls import uri

    resolved = urljoin("https://feed.invalid/source.m3u8", uri("ab" * 32))
    assert not resolved.startswith("https://feed.invalid/"), (
        "external radio reserves feed.invalid for playlists"
    )


def test_committed_stamp_survives_process_kill(tmp_path):
    import subprocess
    import sys

    from livepeer_swarm_radio.state import PublisherState

    script = """
import sys,time
from eth_account import Account
from livepeer_swarm_radio.state import PublisherState
state=PublisherState(sys.argv[1],Account.from_key(bytes([1])*32),bytes([2])*32,depth=20,new_batch=True)
print(state.stamps([bytes(32)])[0],flush=True)
time.sleep(60)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(tmp_path / "db")],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        committed = process.stdout.readline().strip()
        assert len(committed) == 226
        process.kill()
        process.wait(timeout=5)
        account = Account.from_key(bytes([1]) * 32)
        with PublisherState(
            tmp_path / "db", account, bytes([2]) * 32, depth=20
        ) as recovered:
            assert recovered.stamps([bytes(32)])[0] == committed
            next_stamp = bytes.fromhex(recovered.stamps([bytes(31) + b"\x01"])[0])
            assert int.from_bytes(next_stamp[36:40], "big") == 1
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdout.close()


def test_unused_flag_cannot_reset_missing_batch_database(tmp_path):
    from livepeer_swarm_radio.state import PublisherState

    account = Account.from_key(bytes([1]) * 32)
    batch = bytes([2]) * 32
    path = tmp_path / "db"
    with PublisherState(path, account, batch, depth=20, new_batch=True) as state:
        state.stamps([bytes(32)])
    path.unlink()
    with pytest.raises(ValueError, match="already initialized"):
        PublisherState(path, account, batch, depth=20, new_batch=True)


@pytest.mark.asyncio
async def test_station_pointer_refresh_does_not_delay_every_audio_segment(monkeypatch):
    import livepeer_swarm_radio.radio as radio_module
    from livepeer_swarm_radio.hls import master_playlist, media_playlist
    from livepeer_swarm_radio.radio import Radio

    now = [100.0]
    monkeypatch.setattr(radio_module.time, "monotonic", lambda: now[0])
    calls = []
    playlist = media_playlist(
        [
            {
                "sequence": 0,
                "duration": 2,
                "ref": "ab" * 32,
                "captured_at": 0,
                "discontinuity": False,
                "discontinuity_sequence": 0,
            }
        ],
        "cd" * 32,
    )

    class Reader:
        async def latest_feed(self, owner, topic, **kwargs):
            calls.append(topic)
            return (
                0,
                {
                    "station": b'{"topic":"broadcast"}',
                    "broadcast": master_playlist().encode(),
                    "broadcast#source": playlist.encode(),
                }[topic],
            )

        async def read_bytes(self, *args, **kwargs):
            return None

    radio = Radio(Reader(), "owner", "station")
    await radio.poll()
    now[0] += 2
    await radio.poll()
    assert calls.count("station") == 1
    assert calls.count("broadcast#source") == 2
    now[0] += 4
    await radio.poll()
    assert calls.count("station") == 2


@pytest.mark.asyncio
async def test_http_source_burst_is_paced_to_capture_clock(tmp_path):
    import asyncio
    import time

    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from livepeer_swarm_radio.audio import capture

    path = tmp_path / "burst.wav"
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-v",
        "error",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:sample_rate=48000",
        "-t",
        "4.1",
        str(path),
    )
    assert await process.wait() == 0

    async def audio(request):
        return web.Response(body=path.read_bytes(), content_type="audio/wav")

    app = web.Application()
    app.router.add_get("/audio", audio)
    async with TestServer(app) as source:
        count = 0
        async for segment in capture(str(source.make_url("/audio"))):
            assert segment.captured_at + segment.duration <= time.time()
            count += 1
        assert count >= 3
