# Live deployment validation — 2026-10-31

The capability is deployed on `https://ai1.eliteencoder.net:8936` as `livepeer-example/swarm-publish-demo`, with capacity one and a USD 0.10/hour price. Capture and signing run locally. The standalone AAC service reads through `https://api.gateway.ethswarm.org`, independently of the private upload Bee on eliteprd1.

The publisher, Livepeer payer, and Bee operator are three different addresses, recorded in [the measured results](validation-2026-10-31.json). Neither the runner nor the player receives publisher credentials. The existing hosted PymtHouse signer supplies Livepeer tickets.

## Paid run

The deterministic source produced **892 confirmed segments**, about **29 minutes 44 seconds** of audio, with **zero publisher errors**, **zero capture-queue drops**, and **no pending feed writes** at the checkpoint. The user approved ending the endurance test early. The command process exited with SIGTERM before the planned file EOF; this is **not a completed 1800-second EOF test**. File EOF/ENDLIST is separately covered by the real-orchestrator offchain integration test.

| Measurement | Median | 95th percentile |
|---|---:|---:|
| Batched postage-signing round trip | 57 ms | 64 ms |
| Runner upload, including signing | 374 ms | 746 ms |
| Capture → confirmed media and playlist | 2.813 s | 3.343 s |
| Final player: media retrieval/remux | 408 ms | 616 ms |
| Final player: capture → AAC emission | 7.728 s | 9.047 s |

The final player version was observed for approximately the last 15 minutes alongside the original player. It recorded six catch-up events, skipping twelve segments, and a maximum latency of 11.229 seconds. The earlier player recorded sixteen catch-up events. These are explicit playback skips to remain near live; the result is not lossless playback or a latency guarantee. Initial discovery and listener sound-device buffering are outside the steady-state latency figures. A separate waveform-frequency check measured approximately 8.2 seconds, with about one second of quantization uncertainty.

FFmpeg decoded the AAC output. FFprobe identified AAC-LC, 48 kHz, stereo. Two listeners exercised fan-out from one retrieval pipeline. The development and deployment checks passed: 324 gateway SDK tests and 33 unique Swarm/example tests, including real four-channel Trickle transport, incremental signing before channel rotation, reconnect/feed replay, forced-process-kill recovery, batch exhaustion, missing-database protection, HTTP burst pacing, and SIGTERM cleanup. The final HTTP pacing regression was run after the endurance run when switching to the real source.

## Compatibility and limitations

The unmodified Chrisradio web player discovered the station and decoded its AAC/fMP4 rendition. Its long-run playback stalled: that build derives its polling interval from the master rather than the rendition and defaults to polling one update every three seconds. This is slower than this publisher's two-second updates. See [the radio compatibility notes](../deploy/README.md). The standalone AAC service follows the rendition itself.

The supplied PLS resolves to a port-86 endpoint that timed out. Its equivalent HTTPS source, `https://streams.alexjones.media/infowarslive`, was subsequently published through eliteprd1 and decoded successfully from the local AAC service. File and URL inputs are paced, including an initial server burst. Public Bee responses can be delayed or fail transiently; the player authenticates content, retries, and reports catch-up gaps.

## Operation

- Local player: `systemctl --user status swarm-radio`; AAC at `http://127.0.0.1:8787/stream.aac` and status at `http://127.0.0.1:8787/status`.
- Current bounded live-source publisher: `systemctl --user status livepeer-swarm-publisher-demo`; stop with `systemctl --user stop livepeer-swarm-publisher-demo`.
- Publisher configuration and durable state: `/home/elite/.local/state/livepeer-swarm/`. The live invocation is capped at 1900 seconds, with explicit price caps. The transient publisher service is not an indefinitely running paid job.
- Runner/Bee deployment: `/home/elite/livepeer-swarm` on eliteprd1. Bee's write API is loopback-only and storage persists across image updates.
- Logs, waveform measurement, transaction receipts, and the external-radio screenshot are retained in the local state directory. No credentials are committed to either repository.

Setup allocated **1.3937175649386496 xBZZ** and **0.01 xDAI** to the batch/operator, plus **0.00059353 xDAI** in publisher transaction gas. This is within the approved setup budget. The batch was purchased for approximately 26 hours at creation; subsequent use requires checking its remaining TTL and funding it outside the streaming workflow.

The original experiment was subsequently extracted into this standalone package. The gateway SDK checkout is unchanged; current commands are `swarm-publish` and `swarm-radio`. Historical metrics above describe the experiment before repository extraction.

## Standalone-package extraction

The new package passed 29 tests in a fresh environment with the SDK installed from its pinned public Git revision, including all three real-orchestrator Trickle tests. The supply-side package passed nine tests independently. Its Docker image builds from this repository alone. No gateway SDK source or public API was changed.
