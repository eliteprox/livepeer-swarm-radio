"""Bounded single-rendition Donkey-push playlists, with absolute media positions."""

import math
import re
from datetime import UTC, datetime


def uri(reference, gateway="https://api.gateway.ethswarm.org"):
    if not re.fullmatch("[0-9a-f]{64}", reference):
        raise ValueError("invalid Swarm reference")
    return f"{gateway.rstrip('/')}/bytes/{reference}?x=.mp4"


def media_playlist(
    entries,
    init_ref,
    *,
    window=12,
    end=False,
    gateway="https://api.gateway.ethswarm.org",
):
    entries = entries[-window:]
    if not entries:
        raise ValueError("playlist requires a published segment")
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:7",
        f"#EXT-X-TARGETDURATION:{math.ceil(max(e['duration'] for e in entries))}",
        f"#EXT-X-MEDIA-SEQUENCE:{entries[0]['sequence']}",
        f"#EXT-X-DISCONTINUITY-SEQUENCE:{entries[0]['discontinuity_sequence']}",
        f'#EXT-X-MAP:URI="{uri(init_ref, gateway)}"',
    ]
    for i, entry in enumerate(entries):
        if i and entry["discontinuity"]:
            lines.append("#EXT-X-DISCONTINUITY")
        timestamp = (
            datetime.fromtimestamp(entry["captured_at"], UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        lines.extend(
            [
                f"#EXT-X-PROGRAM-DATE-TIME:{timestamp}",
                f"#EXTINF:{entry['duration']:.6f},",
                uri(entry["ref"], gateway),
            ]
        )
    if end:
        lines.append("#EXT-X-ENDLIST")
    result = "\n".join(lines) + "\n"
    if len(result.encode()) > 4096:
        raise ValueError("playlist exceeds inline SOC payload")
    return result


def master_playlist():
    return '#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-STREAM-INF:BANDWIDTH=80000,CODECS="mp4a.40.2",NAME="source"\nsource.m3u8\n'


def reference(uri_text):
    found = re.search(r"(?:^|/)([a-fA-F0-9]{64})(?:[?/#]|$)", uri_text)
    if not found:
        raise ValueError("playlist URI is not a Swarm bytes reference")
    return found[1].lower()


def parse_media(text):
    if not text.startswith("#EXTM3U") or len(text.encode()) > 4096:
        raise ValueError("invalid inline media playlist")
    seq, disc, duration, timestamp, init, ended = 0, 0, None, None, None, False
    entries = []
    for line in text.splitlines():
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            seq = int(line.split(":", 1)[1])
        elif line.startswith("#EXT-X-DISCONTINUITY-SEQUENCE:"):
            disc = int(line.split(":", 1)[1])
        elif line == "#EXT-X-DISCONTINUITY":
            disc += 1
        elif line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            timestamp = datetime.fromisoformat(
                line.split(":", 1)[1].replace("Z", "+00:00")
            ).timestamp()
        elif line.startswith("#EXT-X-MAP:"):
            match = re.search(r'URI="([^"]+)"', line)
            init = reference(match[1]) if match else None
        elif line.startswith("#EXTINF:"):
            duration = float(line.split(":", 1)[1].rstrip(","))
            if not math.isfinite(duration) or not 0 < duration <= 30:
                raise ValueError("invalid segment duration")
        elif line == "#EXT-X-ENDLIST":
            ended = True
        elif line and not line.startswith("#"):
            if duration is None:
                raise ValueError("segment has no duration")
            entries.append(
                {
                    "sequence": seq,
                    "ref": reference(line),
                    "duration": duration,
                    "captured_at": timestamp,
                    "discontinuity_sequence": disc,
                }
            )
            seq += 1
            if timestamp is not None:
                timestamp += duration
            duration = None
    if not init:
        raise ValueError("AAC fragmented MP4 playlist requires EXT-X-MAP")
    return init, entries, ended
