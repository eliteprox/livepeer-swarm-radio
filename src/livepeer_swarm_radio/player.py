#!/usr/bin/env python3
"""Read a Swarm station through Bee and serve a continuous AAC radio stream."""

import argparse
import logging

from aiohttp import web
from eth_utils import is_address

from livepeer_swarm_radio.radio import build_radio_app


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bee", required=True, help="Independent read gateway URL")
    parser.add_argument(
        "--owner", required=True, help="Publisher feed owner; not the Livepeer payer"
    )
    parser.add_argument("--station", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()
    if not is_address(args.owner):
        parser.error("--owner must be an Ethereum address")
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    web.run_app(
        build_radio_app(args.bee, args.owner, args.station),
        host=args.host,
        port=args.port,
    )


if __name__ == "__main__":
    main()
