#!/usr/bin/env python3
"""Run a Replay Session with `mdgw_client.ReplayClient`.

The Replay Service Role exposes two ZMQ endpoints (printed by
`mdgw run --service replay` as `control_endpoint` and `publish_endpoint`).
This example creates one session, prints every Bar event in the protocol's
deterministic `(close_ts, instrument_id, seq)` order, then prints the End of
Session message.

Examples (endpoints from the demo config):
    python examples/replay.py \\
        --control-endpoint tcp://127.0.0.1:5557 \\
        --publish-endpoint tcp://127.0.0.1:5558 \\
        --instrument binance:BTC/USDT --interval 1m --mode asap
    python examples/replay.py ... --mode step --steps 5

`asap` is the default and is what the demo fake data is best suited for.
`realtime` paces Bars by `--speed`; use a large speed for a short demo.
"""

from __future__ import annotations

import argparse
import sys

from mdgw_client import ClientError, ReplayClient, StreamEvent

EPOCH_START = "1970-01-01T00:00:00.000000000Z"
FAR_FUTURE = "2100-01-01T00:00:00.000000000Z"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="replay.py",
        description="Create and consume one Replay Session via mdgw_client.",
    )
    parser.add_argument(
        "--control-endpoint",
        required=True,
        metavar="ZMQ_ENDPOINT",
        help="Replay control REQ/REP endpoint, e.g. tcp://127.0.0.1:5557",
    )
    parser.add_argument(
        "--publish-endpoint",
        required=True,
        metavar="ZMQ_ENDPOINT",
        help="Replay market PUB endpoint, e.g. tcp://127.0.0.1:5558",
    )
    parser.add_argument(
        "--instrument",
        default="binance:BTC/USDT",
        metavar="INSTRUMENT_ID",
        help="Instrument to replay (default: binance:BTC/USDT)",
    )
    parser.add_argument(
        "--interval",
        default="1m",
        metavar="INTERVAL",
        help="Bar Interval to replay (default: 1m)",
    )
    parser.add_argument(
        "--start",
        default=EPOCH_START,
        metavar="ISO8601",
        help="inclusive session start on close_ts (default: epoch)",
    )
    parser.add_argument(
        "--end",
        default=FAR_FUTURE,
        metavar="ISO8601",
        help="exclusive session end on close_ts (default: 2100)",
    )
    parser.add_argument(
        "--mode",
        choices=("asap", "realtime", "step"),
        default="asap",
        help="Replay Mode (default: asap)",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="realtime pacing multiplier (default: 1.0)",
    )
    parser.add_argument(
        "--steps",
        type=int,
        default=5,
        help="Step Credits granted per step() call in step mode (default: 5)",
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=10.0,
        help=(
            "step-mode wait for the next event before granting more credits "
            "(default: 10)"
        ),
    )
    return parser


def print_event(event: StreamEvent) -> None:
    rows = [] if event.table is None else event.table.to_pylist()
    print(f"topic={event.topic} rows={len(rows)}")
    for row in rows:
        print(f"  {row}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.steps <= 0:
        parser.error("--steps must be positive")
    if args.speed <= 0:
        parser.error("--speed must be positive")

    session = None
    try:
        with ReplayClient(
            publish_endpoint=args.publish_endpoint,
            control_endpoint=args.control_endpoint,
            timeout_s=args.timeout_s,
        ) as client:
            session = client.create_session(
                subscriptions=[
                    {
                        "data_type": "bar",
                        "instrument_id": args.instrument,
                        "interval": args.interval,
                    }
                ],
                start_ts=args.start,
                end_ts=args.end,
                replay_mode=args.mode,
                speed=args.speed,
            )
            print(
                f"session {session.session_id} mode={args.mode} "
                f"publish_endpoint={session.publish_endpoint}"
            )

            if args.mode == "step":
                # `step` sessions start paused; grant the first batch of credits.
                session.step(args.steps)

            while True:
                timeout = args.timeout_s if args.mode == "step" else None
                try:
                    event = session.next_event(timeout=timeout)
                except StopIteration:
                    break
                except ClientError as exc:
                    # In step mode running out of credits looks like a timeout:
                    # grant another batch and continue.
                    if args.mode == "step" and exc.code == "timeout":
                        session.step(args.steps)
                        continue
                    raise
                print_event(event)

            end = session.end_of_session or {}
            print(
                "End of Session: "
                f"reason={end.get('reason')} bars_sent={end.get('bars_sent')}"
            )
    except KeyboardInterrupt:
        print("\ninterrupted")
    except ClientError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        if session is not None:
            session.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
