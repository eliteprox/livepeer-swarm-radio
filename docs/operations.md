# Live audio → Livepeer → Swarm → AAC radio

This package owns the local client and radio. The keyless runner is supplied by [runner-app-examples](https://github.com/eliteprox/runner-app-examples/tree/feat/swarm-live-publisher/swarm-publish-demo); the shared Swarm protocol helpers are a pinned dependency.

There are three independent identities: **Livepeer payer**, **publisher**, and **Bee operator**. The payer signs tickets through a remote signer. The publisher signs postage and station/feed updates locally. Only the postage signer must own the postage batch. The gateway needs no BZZ, and the runner receives neither publisher nor payer keys.

## Install

From the standalone repository, run `uv sync --locked`. Python 3.12+, Git, uv, and the FFmpeg executable are required. The SDK and shared protocol package are installed from pinned Git revisions; no sibling checkout is required. The player uses PyAV to remux AAC without re-encoding.

## Publisher configuration

Copy `publisher.example.toml` outside the repository and fill in the existing batch's ID, depth, bucket depth, immutability and counter export. Use the publisher's encrypted geth keystore and a readable password file. Raw key files are also accepted, primarily for isolated offchain tests. No key or password is logged.

`batch.counters_file` imports the Android/Python signer format:

```json
{"buckets":{"123":5},"slots":{"<64-character chunk address>":4}}
```

The import happens only when creating a database. Subsequent runs reuse SQLite state and ignore the import file. Use `batch.unused=true` **only for a genuinely unused batch**. After the first successful run, set `batch.unused=false`; the database retains the allocations. A used batch cannot be reconstructed safely from its ID alone. A Bee LevelDB stamper store is not this JSON format; export its issuer counters before migration. The original signing process must stop before the new client takes ownership. Stop the publisher before backing up the entire state directory, including the per-batch `.initialized` marker. Never restore an older snapshot after additional stamps have been signed; recover the latest counters or use a fresh batch. The marker prevents an unchanged `unused=true` setting from silently resetting allocations if the database disappears. Keep all databases for a batch in one state directory; the local batch lock cannot coordinate other hosts.

The password path `/var/lib/bee/password` is normally owned by the Bee service account. Run the client under an account with deliberate access to that file, or create a private readable copy; do not make it world-readable.

```sh
uv run --locked swarm-publish --config /path/to/publisher.toml
uv run --locked swarm-publish --config /path/to/publisher.toml --input audio.wav
uv run --locked swarm-publish --config /path/to/publisher.toml --input 'https://streams.alexjones.media/listen.pls?sid=7'
```

Inputs include `pulse:default`, `alsa:default`, files, HTTP audio, and PLS playlists. Files and URL inputs are paced in real time, including an initial burst from a radio server. The supplied PLS resolves to `http://streams.alexjones.media:86/infowarslive`; its upstream must be reachable from the capture machine. The equivalent HTTPS endpoint `https://streams.alexjones.media/infowarslive` was reachable during validation and can be passed directly with `--input`; the resolver deliberately does not rewrite arbitrary playlist ports.

Capture is AAC-LC, 48 kHz stereo, 64 kbps, approximately two seconds per fragmented-MP4 segment. Actual frame-aligned durations are used. A queue holds at most three unpublished segments; a source-sequence gap becomes an HLS discontinuity. Each process invocation creates a fresh broadcast topic; transport reconnects retain that process's topic and timeline. Previous pending signed feeds are reconciled before publishing new media, while stamp allocations and feed indexes survive process restarts.

Station names are stable. `new_station=true` is an explicit assertion that a new feed can start at index zero; existing indexes in the state database still take precedence. For an existing station on a new database, use `new_station=false` and an independent read gateway to discover and authenticate its frontier. Never reuse an existing station name with empty state and `new_station=true`.

## Livepeer payer

`deploy/compose.signer.yml` starts a separate go-livepeer remote signer bound to `127.0.0.1:17936`. Set `ARBITRUM_RPC_URL`, `PAYER_ADDRESS`, `PAYER_KEYSTORE_DIR`, `PAYER_PASSWORD_FILE`, and `MAX_PRICE_PER_UNIT` in a private environment file. The cap is per second for metered sessions. For example `0.00005556USD` is approximately USD 0.20/hour.

```sh
docker compose --env-file /path/to/payer.env -f deploy/compose.signer.yml up -d
```

An existing builder-core signer or hosted PymtHouse signer can be reused by setting `signer_url`. If it requires authentication, set `signer_headers_file` to a private JSON file containing the request headers (for example an Authorization header); do not embed credentials in repository files. Set `max_hourly_usd` explicitly: the client checks the advertised rate before each reservation, including the SDK's 1.2% padding. On-chain discovery converts the USD rate to wei/second; set `max_wei_per_second` explicitly for that case and re-evaluate it when ETH/USD changes. The SDK includes that advertised rate in its signer request; configure a matching hard limit at the signer for protection against price changes between discovery and reservation. `max_duration_seconds` bounds a paid invocation and releases its session when reached.

The payer needs an existing funded Livepeer deposit/reserve. This example does not deposit funds, purchase postage, or transfer tokens. `offchain=true` is an explicit testing option, not an automatic fallback when payment fails. Stop the signer with the same Compose command using `down`.

## Listen

From this repository, using the same virtual environment:

```sh
uv run --locked swarm-radio \
  --bee https://api.gateway.ethswarm.org --owner 0xYOUR_PUBLISHER_ADDRESS --station elite-radio
ffplay -nodisp http://127.0.0.1:8787/stream.aac
curl http://127.0.0.1:8787/status
```

Use a Bee **read gateway independent of the uploader** for the public-network acceptance test. The local HTTP response is continuous ADTS AAC (`audio/aac`). The service authenticates raw SOC feeds, follows the stable station pointer and `#source` rendition, and uses a bounded queue per listener. Slow listeners are disconnected; delayed chunks are retried, then a recorded gap lets playback recover toward the live edge. `/healthz` is 200 only while audio is flowing; `/status` reports retrieval failures, sequence, gaps, listener buffer depth, and capture-to-emission latency.

The feed format follows `pub`/Donkey-push: topic = keccak(UTF-8 name), sequence index = big-endian uint64, inline SOC payloads, station payload `{"topic":"<broadcast>"}`, a master feed at that topic, and a media feed at `<broadcast>#source`. It is not Bee's timestamp-plus-reference feed envelope. External radio compatibility needs a live test against the chosen player build; not every program called a “Swarm radio” implements this convention. Existing players choose their own buffering.

## Test

Run the package's unit/HTTP tests:

```sh
uv run --locked pytest tests/test_live.py tests/test_config.py
```

For actual Trickle integration, start a dedicated offchain go-livepeer instance, using a DNS name rather than an IP so its generated certificate matches the hostname:

```sh
docker run -d --name swarm-audio-test-orch --network host livepeer/go-livepeer:v0.9.3 \
  -orchestrator -useLiveRunners -network=offchain \
  -httpAddr=127.0.0.1:18935 -serviceAddr=https://localhost:18935 \
  -liveRunnerAddr=https://localhost:18935 -cliAddr=127.0.0.1:17935 \
  -orchSecret=swarm-test-only -monitor=false
docker cp swarm-audio-test-orch:/root/.lpData/offchain/cert.pem /tmp/swarm-audio-test-cert.pem
SSL_CERT_FILE=/tmp/swarm-audio-test-cert.pem SWARM_TEST_ORCHESTRATOR=https://localhost:18935 \
  uv run --locked pytest tests
# Remove only this dedicated test instance afterward:
docker rm -f swarm-audio-test-orch
```

Wait until the container has generated `cert.pem` before copying it; refresh the trusted certificate after every orchestrator restart. TLS verification remains enabled. The integration tests use a stub Bee, deterministic test keys, and a real orchestrator. They test signing before JSONL rotation and actual capture → Trickle → feeds → HTTP AAC decoding. The stub does not prove postage validity, propagation, retention, or independent-network retrieval.

The live acceptance run is 30 minutes with real postage, a separately funded Livepeer payer, and an independent Bee gateway. Record signing time, publication time, retrieval time, and capture-to-player latency. Target 6–10 seconds; treat observed propagation delays and upstream buffering as measured limits, not a guaranteed SLA. Also open the station in the existing Donkey-push radio and confirm playback. Graceful file EOF publishes ENDLIST; interruption releases the Livepeer session, and the radio eventually reports a stalled stream.

Measured deployment results and remaining playback limitations are recorded in [VALIDATION.md](VALIDATION.md).

### Playback prefetch and pacing

The radio runs playlist discovery, audio retrieval/remuxing, and playback on
independent tasks. Two remuxed AAC segments can wait in the prefetch queue, with
one retrieval in flight. Playback primes one segment ahead (waiting at most two
seconds), then emits according to each segment's actual duration. A delayed feed
probe therefore does not stop delivery of audio already retrieved.

Live startup waits for a playlist whose newest capture timestamp is at most eight
seconds old, then starts with its newest segment. This assumes capture and playback
host clocks are synchronized. Finished broadcasts can still play their last two
segments. Short backlogs are played in sequence. The reader only rejoins the live edge when
its outstanding playlist duration exceeds 12 seconds, or content has fallen out
of the available playlist. Missing media is retried six times before advancing.
This favors continuity over immediate catch-up; sustained network outages can
still cause gaps. The three-segment per-listener queue remains bounded, and slow
listeners are disconnected. AAC, two-second publication segments, and station/feed
formats are unchanged.

`/status` additionally exposes `prefetched_segments`, `playback_underruns`, and
`skipped_segments`. `capture_to_emit_seconds` is measured when audio is actually
emitted, not when it finishes downloading; VLC can add its own buffering.
