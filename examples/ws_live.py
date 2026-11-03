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

from typing import Optional

import click
import typer

from mdgw_client import ClientError, GatewayClient, LiveSubscription

DEFAULT_TOPICS = ["market.bar."]

app = typer.Typer(
    name="ws_live.py",
    add_completion=False,
    help="Subscribe to live Bar topics over the gateway WebSocket endpoint.",
)


@app.command()
def live(
    base_url: Optional[str] = typer.Option(
        None,
        "--base-url",
        metavar="URL",
        help="gateway base URL; the WebSocket URL is derived as ws://host:port/ws",
    ),
    ws_url: Optional[str] = typer.Option(
        None,
        "--ws-url",
        metavar="WS_URL",
        help="explicit WebSocket URL, e.g. ws://127.0.0.1:8080/ws",
    ),
    topic: Optional[list[str]] = typer.Option(
        None,
        "--topic",
        metavar="TOPIC",
        help=(
            "topic or prefix to subscribe to (repeatable; default: market.bar.)"
        ),
    ),
    count: int = typer.Option(
        0,
        "--count",
        help="exit after N Bar events; 0 runs until Ctrl-C (default: 0)",
    ),
    timeout_s: float = typer.Option(
        10.0,
        "--timeout-s",
        help="WebSocket connect timeout in seconds (default: 10)",
    ),
) -> int:
    if base_url is None and ws_url is None:
        raise typer.BadParameter(
            "one of --base-url or --ws-url is required", param_hint="--base-url"
        )
    if base_url is not None and ws_url is not None:
        raise typer.BadParameter(
            "--base-url and --ws-url are mutually exclusive",
            param_hint="--ws-url",
        )
    if count < 0:
        raise typer.BadParameter("--count must be non-negative", param_hint="--count")
    topics = topic or DEFAULT_TOPICS

    client = None
    subscription = None
    try:
        if base_url:
            client = GatewayClient(base_url, timeout_s=timeout_s)
            subscription = client.subscribe(topics, transport="ws")
        else:
            subscription = LiveSubscription(
                transport="ws",
                topics=topics,
                ws_url=ws_url,
                timeout_s=timeout_s,
            )
        typer.echo(
            f"subscribed to {topics!r}; waiting for events "
            "(Ctrl-C to exit) ..."
        )

        seen = 0
        for event in subscription:
            print_event(event)
            if event.kind == "lag_notice":
                continue
            seen += 1
            if count and seen >= count:
                break
    except KeyboardInterrupt:
        typer.echo("\ninterrupted")
    except ClientError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1)
    finally:
        if subscription is not None:
            subscription.close()
        if client is not None:
            client.close()
    return 0


def print_event(event) -> None:
    if event.kind == "lag_notice":
        notice = event.lag_notice or {}
        typer.echo(
            f"lag_notice: dropped={notice.get('dropped')} "
            f"since={notice.get('since')}"
        )
        return
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
