# Livepeer Swarm Radio

A local publisher and independent AAC radio for live audio delivered through the Livepeer orchestrator network to Swarm.

```text
Microphone / file / URL → gateway SDK → orchestrator → keyless Swarm runner
                                                        ↓
Local AAC player ← independent Bee read gateway ← Swarm network
```

This is an application built on **livepeer-gateway**, the Python distribution from [livepeer-python-gateway](https://github.com/livepeer/livepeer-python-gateway). It imports the SDK without modifying it. The supply-side publisher runner and shared Swarm wire/crypto helpers live in [runner-app-examples](https://github.com/eliteprox/runner-app-examples/tree/feat/swarm-live-publisher/swarm-publish-demo).

## Install and run

Python 3.12+, FFmpeg, Git, and [uv](https://docs.astral.sh/uv/) are required. Dependency revisions are pinned in `uv.lock`; neither a sibling checkout nor an SDK development environment is needed.

```sh
git clone https://github.com/eliteprox/livepeer-swarm-radio.git
cd livepeer-swarm-radio
uv sync --locked
cp publisher.example.toml /private/path/publisher.toml
# Fill in publisher key/password paths, existing batch/counters, signer and price caps.
uv run --locked swarm-publish --config /private/path/publisher.toml
```

In another terminal, use the same publisher address and station name as the publisher configuration:

```sh
uv run --locked swarm-radio --bee https://api.gateway.ethswarm.org \
  --owner 0xYOUR_PUBLISHER_ADDRESS --station your-station
ffplay -nodisp http://127.0.0.1:8787/stream.aac
```

`swarm-publish --input` overrides the configured source with a paced file, HTTP/PLS URL, `pulse:default`, or `alsa:default`. Audio is AAC-LC, 48 kHz stereo, 64 kbps in two-second fragmented-MP4 segments. The player remuxes these to continuous ADTS AAC without re-encoding. `/status` and `/healthz` expose playback health.

## Identities and state

- **Publisher:** signs postage and feed updates locally; its postage signatures must match the batch owner.
- **Livepeer payer:** uses an independently configured remote signer for payment tickets.
- **Bee operator:** currently has its own funded node identity and chequebook.

[Postage pays for storage](https://docs.ethswarm.org/docs/concepts/incentives/overview/); the current Bee setup also funds its separate network-bandwidth settlement mechanism. Reusing or sponsoring that operator funding with a gateway user's wallet is future work, not implemented by this example. Publishing does not buy postage or transfer BZZ. The runner receives no publisher private key.

Publisher counters, pending signed feeds, confirmed indexes, and broadcast state are committed to local SQLite. Keep exclusive ownership of a batch's signing state. Never restart a used batch with empty counters, and never restore a stale state snapshot after signing more stamps. Private credentials and runtime state belong outside this repository.

## Tests and operation

```sh
uv run --locked pytest
```

Real Trickle integration tests additionally need a dedicated offchain orchestrator; see [operations](docs/operations.md). They run against the installed application and SDK, not another source checkout.

- [Configuration, inputs, state recovery and payment setup](docs/operations.md)
- [Measured results and known playback limitations](docs/VALIDATION.md)
- [Local service and container deployment](deploy/README.md)
- [Runner and Bee deployment](https://github.com/eliteprox/runner-app-examples/tree/feat/swarm-live-publisher/swarm-publish-demo/deploy)

The experimental public-network run measured about 7.7 seconds median capture-to-AAC latency and 9.0 seconds at the 95th percentile, with catch-up skips and occasional slower samples. This is not a latency or lossless-playback guarantee. Existing web radios choose their own polling and buffering; one tested build decodes the format but falls behind two-second feed updates.
