"""FFmpeg capture and packet-only AAC remuxing. No media work on the event loop."""

import asyncio
import configparser
import io
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp
import av


@dataclass(frozen=True)
class AudioSegment:
    sequence: int
    duration: float
    captured_at: float
    init: bytes
    data: bytes


async def resolve_input(source):
    if urlsplit(source).path.lower().endswith(".pls"):
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15)
        ) as http:
            async with http.get(source) as response:
                response.raise_for_status()
                body = await response.content.read(65537)
                if len(body) > 65536:
                    raise ValueError("PLS exceeds size limit")
        config = configparser.ConfigParser()
        config.read_string(body.decode("utf-8-sig"))
        source = config["playlist"]["file1"]
        if urlsplit(source).scheme not in ("http", "https"):
            raise ValueError("PLS must resolve to an HTTP audio stream")
    return source


async def capture(source, *, paced=True, segment_seconds=2):
    source = await resolve_input(source)
    paced_input = paced and not source.startswith(("pulse:", "alsa:"))
    with tempfile.TemporaryDirectory(prefix="swarm-capture-") as directory:
        root = Path(directory)
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin"]
        if source.startswith(("pulse:", "alsa:")):
            driver, device = source.split(":", 1)
            cmd += ["-f", driver, "-i", device]
        else:
            if paced_input:
                cmd += ["-re"]
            if urlsplit(source).scheme in ("http", "https"):
                cmd += ["-rw_timeout", "15000000"]
            cmd += ["-i", source]
        cmd += [
            "-vn",
            "-c:a",
            "aac",
            "-profile:a",
            "aac_low",
            "-b:a",
            "64k",
            "-ar",
            "48000",
            "-ac",
            "2",
            "-f",
            "hls",
            "-hls_time",
            str(segment_seconds),
            "-hls_segment_type",
            "fmp4",
            "-hls_fmp4_init_filename",
            "init.mp4",
            "-hls_list_size",
            "6",
            "-hls_flags",
            "temp_file+delete_segments+independent_segments",
            "-hls_segment_filename",
            str(root / "seg_%09d.m4s"),
            str(root / "live.m3u8"),
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd, stderr=asyncio.subprocess.PIPE
        )
        # Drain stderr continuously but retain only its tail.
        errors = bytearray()

        async def drain():
            while part := await process.stderr.read(4096):
                errors.extend(part)
                del errors[:-8192]

        drain_task = asyncio.create_task(drain())
        clock, elapsed, last = time.time(), 0.0, -1
        try:
            while True:
                finished = process.returncode is not None
                playlist = root / "live.m3u8"
                if playlist.exists():
                    lines = playlist.read_text().splitlines()
                    duration = None
                    init = (root / "init.mp4").read_bytes()
                    for line in lines:
                        if line.startswith("#EXTINF:"):
                            duration = float(line.split(":", 1)[1].rstrip(","))
                        elif line.startswith("seg_") and duration is not None:
                            seq = int(line[4:].split(".")[0])
                            if seq > last:
                                path = root / line
                                if path.exists():
                                    data = path.read_bytes()
                                    # FFmpeg's read-rate pacing may read ahead or
                                    # flush a final padded AAC frame early. Release
                                    # file/URL audio only when its full timeline has elapsed.
                                    if paced_input:
                                        await asyncio.sleep(
                                            max(
                                                0,
                                                clock
                                                + elapsed
                                                + duration
                                                - time.time(),
                                            )
                                        )
                                    yield AudioSegment(
                                        seq, duration, clock + elapsed, init, data
                                    )
                                    elapsed += duration
                                    last = seq
                            duration = None
                if finished:
                    if process.returncode:
                        raise RuntimeError(
                            "FFmpeg capture failed: "
                            + errors.decode(errors="replace")[-1500:]
                        )
                    break
                await asyncio.sleep(0.05)
        finally:
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 3)
                except TimeoutError:
                    process.kill()
                    await process.wait()
            await drain_task


def remux_adts(init, fragment):
    target = io.BytesIO()
    with av.open(io.BytesIO(init + fragment), format="mp4") as source:
        with av.open(target, mode="w", format="adts") as output:
            stream = source.streams.audio[0]
            if stream.codec_context.name != "aac":
                raise ValueError("radio requires AAC audio")
            out = output.add_stream_from_template(stream)
            for packet in source.demux(stream):
                if packet.size:
                    packet.stream = out
                    output.mux(packet)
    return target.getvalue()
