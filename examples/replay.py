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

from typing import Optional

import click
import typer

from mdgw_client import ClientError, ReplayClient, StreamEvent

EPOCH_START = "1970-01-01T00:00:00.000000000Z"
FAR_FUTURE = "2100-01-01T00:00:00.000000000Z"

app = typer.Typer(
    name="replay.py",
    add_completion=False,
    help="Create and consume one Replay Session via mdgw_client.",
)


@app.command()
def replay(
    control_endpoint: str = typer.Option(
        ...,
        "--control-endpoint",
        metavar="ZMQ_ENDPOINT",
        help="Replay control REQ/REP endpoint, e.g. tcp://127.0.0.1:5557",
    ),
    publish_endpoint: str = typer.Option(
        ...,
        "--publish-endpoint",
        metavar="ZMQ_ENDPOINT",
        help="Replay market PUB endpoint, e.g. tcp://127.0.0.1:5558",
    ),
    instrument: str = typer.Option(
        "binance:BTC/USDT",
        "--instrument",
        metavar="INSTRUMENT_ID",
        help="Instrument to replay (default: binance:BTC/USDT)",
    ),
    interval: str = typer.Option(
        "1m",
        "--interval",
        metavar="INTERVAL",
        help="Bar Interval to replay (default: 1m)",
    ),
    start: str = typer.Option(
        EPOCH_START,
        "--start",
        metavar="ISO8601",
        help="inclusive session start on close_ts (default: epoch)",
    ),
    end: str = typer.Option(
        FAR_FUTURE,
        "--end",
        metavar="ISO8601",
        help="exclusive session end on close_ts (default: 2100)",
    ),
    mode: str = typer.Option(
        "asap",
        "--mode",
        click_type=click.Choice(("asap", "realtime", "step")),
        help="Replay Mode (default: asap)",
    ),
    speed: float = typer.Option(
        1.0,
        "--speed",
        help="realtime pacing multiplier (default: 1.0)",
    ),
    steps: int = typer.Option(
        5,
        "--steps",
        help="Step Credits granted per step() call in step mode (default: 5)",
    ),
    timeout_s: float = typer.Option(
        10.0,
        "--timeout-s",
        help=(
            "step-mode wait for the next event before granting more credits "
            "(default: 10)"
        ),
    ),
) -> int:
    if steps <= 0:
        raise typer.BadParameter("--steps must be positive", param_hint="--steps")
    if speed <= 0:
        raise typer.BadParameter("--speed must be positive", param_hint="--speed")

    session = None
    try:
        with ReplayClient(
            publish_endpoint=publish_endpoint,
            control_endpoint=control_endpoint,
            timeout_s=timeout_s,
        ) as client:
            session = client.create_session(
                subscriptions=[
                    {
                        "data_type": "bar",
                        "instrument_id": instrument,
                        "interval": interval,
                    }
                ],
                start_ts=start,
                end_ts=end,
                replay_mode=mode,
                speed=speed,
            )
            typer.echo(
                f"session {session.session_id} mode={mode} "
                f"publish_endpoint={session.publish_endpoint}"
            )

            if mode == "step":
                # `step` sessions start paused; grant the first batch of credits.
                session.step(steps)

            while True:
                timeout = timeout_s if mode == "step" else None
                try:
                    event = session.next_event(timeout=timeout)
                except StopIteration:
                    break
                except ClientError as exc:
                    # In step mode running out of credits looks like a timeout:
                    # grant another batch and continue.
                    if mode == "step" and exc.code == "timeout":
                        session.step(steps)
                        continue
                    raise
                print_event(event)

            end_of_session = session.end_of_session or {}
            typer.echo(
                "End of Session: "
                f"reason={end_of_session.get('reason')} "
                f"bars_sent={end_of_session.get('bars_sent')}"
            )
    except KeyboardInterrupt:
        typer.echo("\ninterrupted")
    except ClientError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1)
    finally:
        if session is not None:
            session.close()
    return 0


def print_event(event: StreamEvent) -> None:
    rows = [] if event.table is None else event.table.to_pylist()
    typer.echo(f"topic={event.topic} rows={len(rows)}")
    for row in rows:
        typer.echo(f"  {row}")


def main(argv: Optional[list[str]] = None) -> int:
    try:
        result = app(args=argv, standalone_mode=False)
    except click.exceptions.ClickException as exc:
        exc.show()
        raise SystemExit(exc.exit_code) from None
    except click.exceptions.Abort:
        click.echo("Aborted!", err=True)
        return 1
    return 0 if result is None else int(result)


if __name__ == "__main__":
    raise SystemExit(main())
