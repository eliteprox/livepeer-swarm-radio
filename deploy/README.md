# Swarm AAC radio

A standalone local player for the raw SOC/HLS station convention used by Donkey-push. Reads exclusively from a configured Bee gateway and exposes continuous ADTS AAC; no Livepeer session or signing key is needed.

```sh
uv run --locked swarm-radio --bee https://api.gateway.ethswarm.org \
  --owner 0xYOUR_PUBLISHER_ADDRESS --station elite-radio
ffplay -nodisp http://127.0.0.1:8787/stream.aac
curl http://127.0.0.1:8787/status
```

Or set `BEE_READ_URL`, `PUBLISHER_OWNER`, and `STATION`, then run `docker compose -f deploy/compose.yml up --build`. The published port defaults to loopback.

`/stream.aac` returns `audio/aac`. `/healthz` reports 200 while audio flows, otherwise 503. `/status` includes sequence, feed index, retrieval failures, gaps, slow-listener disconnects, and capture-to-emission latency. One bounded pipeline serves all listeners. The player verifies SOC ownership and media references, remuxes AAC without re-encoding, and disconnects stalled listeners.

Supported media is AAC in fragmented MP4 with EXT-X-MAP. The station pointer contains `{"topic":"<broadcast>"}`; master and `#source` rendition feeds contain inline HLS text. This is specifically the Donkey-push convention, not a generic Bee timestamp/reference feed reader. Use an independent Bee gateway for the network delivery test. Existing public radios may choose larger playback buffers.

See [operations](../docs/operations.md) for publisher setup and tests. The publisher runner and Bee are deployed separately from [runner-app-examples](https://github.com/eliteprox/runner-app-examples/tree/feat/swarm-live-publisher/swarm-publish-demo/deploy).

For the local user service, install this package under `~/repos/livepeer-swarm-radio` with `uv sync --locked`. Create `~/.config/swarm-radio/radio.env` with `BEE_READ_URL`, `PUBLISHER_OWNER`, and `STATION`. Copy `swarm-radio.service` to `~/.config/systemd/user/`, run `systemctl --user daemon-reload`, then `systemctl --user enable --now swarm-radio`. Stop it with `systemctl --user stop swarm-radio`. Adjust the unit path if your checkout lives elsewhere.

## Existing web-radio compatibility

The deployed Chrisradio `weeb-3` build successfully discovers this publisher and decodes its AAC/fMP4 rendition. Its long-run test also exposed a player-side limitation: it derives its polling interval from the master playlist, whose valid HLS format has no target duration, and retains a six-second default. It consequently polls one new feed index about every three seconds while this publisher creates one every two seconds, causing growing delay and stalls. It should derive the interval from the rendition playlist (and catch up to its frontier after a stall). This example keeps valid master/rendition playlists and does not inject a media-only target-duration tag into the master. Our standalone AAC service follows the rendition directly.
