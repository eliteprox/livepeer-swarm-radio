#!/usr/bin/env python3
"""Capture audio locally; pay Livepeer and sign Swarm under independent identities."""

import argparse
import asyncio
import json
import logging
import math
import signal
import time
import tomllib
from pathlib import Path

from eth_account import Account
from livepeer_gateway.discovery import discover_orchestrator_runners
from livepeer_gateway.selection import reserve_session
from livepeer_swarm.bee import BeeClient

from livepeer_swarm_radio.audio import capture
from livepeer_swarm_radio.broadcast import Broadcast
from livepeer_swarm_radio.state import PublisherState
from livepeer_swarm_radio.transport import PublisherTransport

log = logging.getLogger("swarm-client")


def validate_price(entries, cap, *, wei_cap=None):
    """Fail closed before reservation; the signer also receives the advertised price."""
    if not math.isfinite(float(cap)) or cap <= 0:
        raise ValueError("max_hourly_usd must be positive and finite")
    for entry in entries:
        for runner in entry.get("runners", []):
            price = runner.get("price_info") or {}
            amount = float(price.get("price", "nan"))
            rate = (price.get("currency"), price.get("unit"))
            limit = (
                cap
                if rate == ("usd", "hour")
                else wei_cap
                if rate == ("wei", "seconds")
                else None
            )
            if (
                limit is None
                or not math.isfinite(float(limit))
                or not math.isfinite(amount)
                or not 0 <= amount * 1.012 <= limit
            ):
                raise ValueError(
                    "runner price is unknown or exceeds configured spending cap"
                )


def load_account(config):
    key = Path(config["publisher_key"]).expanduser().read_text().strip()
    try:
        keystore = json.loads(key)
    except ValueError:
        return Account.from_key(key)
    password = (
        Path(config["publisher_password_file"]).expanduser().read_text().rstrip("\r\n")
    )
    return Account.from_key(Account.decrypt(keystore, password))


async def run(config):
    if config.get("signer_headers_file"):
        config["signer_headers"] = json.loads(
            Path(config["signer_headers_file"]).expanduser().read_text()
        )
    account = await asyncio.to_thread(load_account, config)
    batch = config["batch"]
    log.info(
        "publisher=%s; Livepeer payment signer=%s",
        account.address,
        config.get("signer_url", "offchain"),
    )
    if not config.get("signer_url") and not config.get("offchain", False):
        raise ValueError(
            "configure signer_url, or explicitly set offchain=true for testing"
        )
    with PublisherState(
        Path(config["state_path"]).expanduser(),
        account,
        bytes.fromhex(batch["id"].removeprefix("0x")),
        depth=batch["depth"],
        bucket_depth=batch.get("bucket_depth", 16),
        immutable=batch["immutable"],
        new_batch=batch.get("unused", False),
        import_counters=batch.get("counters_file"),
    ) as state:
        station = config["station"]
        # An explicit new-station assertion avoids treating a failed lookup as an empty feed.
        if config.get("new_station", False):
            first_index = 0
        else:
            async with BeeClient(config["read_gateway"]) as reader:
                latest = await reader.latest_feed(account.address, station)
            if latest is None:
                raise ValueError(
                    "station frontier unavailable; use new_station=true only for a new station name"
                )
            first_index = latest[0] + 1
        broadcast = Broadcast(
            state, station, first_index=first_index, gateway=config["read_gateway"]
        )
        queue = asyncio.Queue(maxsize=3)

        async def produce():
            try:
                async for segment in capture(config["input"]):
                    if queue.full():
                        queue.get_nowait()
                        log.warning(
                            "dropping stale unpublished audio; next playlist will mark discontinuity"
                        )
                    queue.put_nowait(segment)
                await queue.put(None)
            except Exception as exc:
                await queue.put(exc)

        producer = asyncio.create_task(produce())
        pending = None
        failures = 0
        try:
            while True:
                if pending is None:
                    pending = await queue.get()
                if isinstance(pending, Exception):
                    raise pending
                if pending is None:
                    return
                try:
                    if config.get("signer_url"):
                        entries = await discover_orchestrator_runners(
                            [config["orchestrator"]],
                            app=config.get(
                                "app", "livepeer-example/swarm-publish-demo"
                            ),
                        )
                        validate_price(
                            entries,
                            config["max_hourly_usd"],
                            wei_cap=config.get("max_wei_per_second"),
                        )
                    async with await reserve_session(
                        orchestrators=[config["orchestrator"]],
                        app=config.get("app", "livepeer-example/swarm-publish-demo"),
                        signer_url=config.get("signer_url") or None,
                        signer_headers=config.get("signer_headers"),
                    ) as session:
                        async with PublisherTransport(session, state) as transport:
                            await broadcast.reconcile(transport)
                            while True:
                                if isinstance(pending, Exception):
                                    raise pending
                                if pending is None:
                                    await broadcast.finish(transport)
                                    return
                                # Catch up after a disconnected session instead of growing latency.
                                while (
                                    time.time() - pending.captured_at > 6
                                    and not queue.empty()
                                ):
                                    newer = queue.get_nowait()
                                    if newer is None or isinstance(newer, Exception):
                                        queue.put_nowait(newer)
                                        break
                                    pending = newer
                                await broadcast.append(transport, pending)
                                log.info(
                                    "published sequence=%s capture_to_publish_s=%.3f topic=%s",
                                    broadcast.next_sequence - 1,
                                    time.time() - pending.captured_at,
                                    broadcast.topic,
                                )
                                failures = 0
                                pending = await queue.get()
                except (ValueError, PermissionError):
                    raise
                except Exception:
                    failures += 1
                    if failures > 5:
                        raise
                    log.exception(
                        "session failed; reconnecting with retained publisher state"
                    )
                    await asyncio.sleep(min(0.5 * 2**failures, 8))
        finally:
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        required=True,
        help="Local publisher TOML; keep secrets out of the repository.",
    )
    parser.add_argument(
        "--input", help="Override file, HTTP/PLS URL, pulse:default, or alsa:default."
    )
    args = parser.parse_args()
    config = tomllib.loads(Path(args.config).read_text())
    if args.input:
        config["input"] = args.input
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        asyncio.run(serve(config))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass


async def serve(config):
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        await run_bounded(config)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


async def run_bounded(config):
    limit = config.get("max_duration_seconds")
    if config.get("signer_url") and (
        not isinstance(limit, (int, float)) or not math.isfinite(limit) or limit <= 0
    ):
        raise ValueError("paid capture requires a positive max_duration_seconds")
    deadline = asyncio.timeout(limit)
    try:
        async with deadline:
            await run(config)
    except TimeoutError:
        if not deadline.expired():
            raise
        log.info("configured broadcast duration reached; session released")


if __name__ == "__main__":
    main()
