#!/usr/bin/env python3
"""Subscribe to the live Streaming Interface over WebSocket.

This uses the optional `websockets` dependency (`pip install websockets`).
Topics are *prefix* subscriptions, so
`market.bar.` matches every Bar and `market.bar.1m.binance:` matches every
instrument at 1m.

The Docker/demo fake feed is one-shot: it publishes its fixed Bars once when
the collector starts. For a live WebSocket demo, start the demo stack and
restart the collector, or use a real Feed Handler.

Examples:
    python examples/ws_live.py --base-url http://127.0.0.1:8080 \\
        --topic market.bar. --count 5
    python examples/ws_live.py --ws-url ws://127.0.0.1:8080/ws \\
        --topic market.bar.1m.binance:BTC/USDT

Press Ctrl-C to exit.
"""

from __future__ import annotations

import argparse
import sys

from mdgw_client import ClientError, GatewayClient, LiveSubscription

DEFAULT_TOPICS = ["market.bar."]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ws_live.py",
        description=(
            "Subscribe to live Bar topics over the gateway WebSocket endpoint."
        ),
    )
    endpoint = parser.add_mutually_exclusive_group(required=True)
    endpoint.add_argument(
        "--base-url",
        metavar="URL",
        help="gateway base URL; the WebSocket URL is derived as ws://host:port/ws",
    )
    endpoint.add_argument(
        "--ws-url",
        metavar="WS_URL",
        help="explicit WebSocket URL, e.g. ws://127.0.0.1:8080/ws",
    )
    parser.add_argument(
        "--topic",
        action="append",
        default=None,
        metavar="TOPIC",
        help=(
            "topic or prefix to subscribe to (repeatable; default: market.bar.)"
        ),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="exit after N Bar events; 0 runs until Ctrl-C (default: 0)",
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=10.0,
        help="WebSocket connect timeout in seconds (default: 10)",
    )
    return parser


def print_event(event) -> None:
    if event.kind == "lag_notice":
        notice = event.lag_notice or {}
        print(
            f"lag_notice: dropped={notice.get('dropped')} "
            f"since={notice.get('since')}"
        )
        return
    rows = [] if event.table is None else event.table.to_pylist()
    print(f"topic={event.topic} rows={len(rows)}")
    for row in rows:
        print(f"  {row}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.count < 0:
        parser.error("--count must be non-negative")
    topics = args.topic or DEFAULT_TOPICS

    client = None
    subscription = None
    try:
        if args.base_url:
            client = GatewayClient(args.base_url, timeout_s=args.timeout_s)
            subscription = client.subscribe(topics, transport="ws")
        else:
            subscription = LiveSubscription(
                transport="ws",
                topics=topics,
                ws_url=args.ws_url,
                timeout_s=args.timeout_s,
            )
        print(
            f"subscribed to {topics!r}; waiting for events "
            "(Ctrl-C to exit) ..."
        )

        seen = 0
        for event in subscription:
            print_event(event)
            if event.kind == "lag_notice":
                continue
            seen += 1
            if args.count and seen >= args.count:
                break
    except KeyboardInterrupt:
        print("\ninterrupted")
    except ClientError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        if subscription is not None:
            subscription.close()
        if client is not None:
            client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
