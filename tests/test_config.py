import pytest

from livepeer_swarm_radio import client


def test_paid_price_requires_known_usd_hour_rate_within_cap():
    valid = [
        {
            "runners": [
                {"price_info": {"price": 0.10, "currency": "usd", "unit": "hour"}}
            ]
        }
    ]
    client.validate_price(valid, 0.11)
    with pytest.raises(ValueError):
        client.validate_price(valid, 0.09)
    for info in [
        {},
        {"price": 0.1, "currency": "eth", "unit": "hour"},
        {"price": "NaN", "currency": "usd", "unit": "hour"},
    ]:
        with pytest.raises(ValueError):
            client.validate_price([{"runners": [{"price_info": info}]}], 1)


def test_price_cap_accepts_orchestrator_converted_wei_rate():
    entries = [
        {
            "runners": [
                {
                    "price_info": {
                        "price": 12384931256,
                        "currency": "wei",
                        "unit": "seconds",
                    }
                }
            ]
        }
    ]
    client.validate_price(entries, 0.11, wei_cap=14000000000)
    with pytest.raises(ValueError):
        client.validate_price(entries, 0.11, wei_cap=12000000000)


@pytest.mark.asyncio
async def test_broadcast_deadline_does_not_hide_transport_timeout(monkeypatch):
    async def failed(config):
        raise TimeoutError("transport timed out")

    monkeypatch.setattr(client, "run", failed)
    with pytest.raises(TimeoutError, match="transport"):
        await client.run_bounded({"max_duration_seconds": 10})


def test_sigterm_runs_client_cleanup(tmp_path):
    import signal
    import subprocess
    import sys

    config = tmp_path / "config.toml"
    config.write_text("offchain=true\n")
    script = """
import asyncio,importlib.util,sys
spec=importlib.util.spec_from_file_location('client',sys.argv[1]); client=importlib.util.module_from_spec(spec);spec.loader.exec_module(client)
async def run(config):
    try:
        print('ready',flush=True)
        await asyncio.Event().wait()
    finally:
        print('released',flush=True)
client.run=run
sys.argv=['client','--config',sys.argv[2]]
client.main()
"""
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            script,
            client.__file__,
            str(config),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "ready"
        child.send_signal(signal.SIGTERM)
        out, err = child.communicate(timeout=5)
        assert child.returncode == 0, err
        assert "released" in out
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
