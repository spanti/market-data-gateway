#!/usr/bin/env python3
"""Query Bar history through the REST API with `mdgw_client.GatewayClient`.

The gateway answers `GET /api/v1/bars?format=arrow` with an Arrow IPC stream;
`GatewayClient.history` decodes it and preserves `decimal128(38, 18)` and
`timestamp[ns, tz=UTC]` exactly. No price ever passes through a float.

Examples:
    python examples/rest_history.py --base-url http://127.0.0.1:8080 \\
        --instrument binance:BTC/USDT --interval 1m
    python examples/rest_history.py --base-url http://127.0.0.1:8080 \\
        --interval 1m --pandas
"""

from __future__ import annotations

from typing import Optional

import click
import typer

from mdgw_client import ClientError, GatewayClient

app = typer.Typer(
    name="rest_history.py",
    add_completion=False,
    help="Fetch Bar history over REST via mdgw_client.GatewayClient.",
)


@app.command()
def history(
    base_url: str = typer.Option(
        ...,
        "--base-url",
        metavar="URL",
        help="gateway base URL, e.g. http://127.0.0.1:8080",
    ),
    instrument: Optional[str] = typer.Option(
        None,
        "--instrument",
        metavar="INSTRUMENT_ID",
        help="filter one Instrument, e.g. binance:BTC/USDT",
    ),
    interval: str = typer.Option(
        "1m",
        "--interval",
        metavar="INTERVAL",
        help="Bar Interval (default: 1m)",
    ),
    start: Optional[str] = typer.Option(
        None,
        "--start",
        metavar="ISO8601",
        help="inclusive open_ts lower bound",
    ),
    end: Optional[str] = typer.Option(
        None,
        "--end",
        metavar="ISO8601",
        help="exclusive open_ts upper bound",
    ),
    limit: int = typer.Option(
        20,
        "--limit",
        help="maximum rows to request/print; 0 requests all rows (default: 20)",
    ),
    pandas: bool = typer.Option(
        False,
        "--pandas",
        help="render with Table.to_pandas() instead of Arrow rows",
    ),
    timeout_s: float = typer.Option(
        10.0,
        "--timeout-s",
        help="HTTP timeout in seconds (default: 10)",
    ),
) -> int:
    if limit < 0:
        raise typer.BadParameter("--limit must be non-negative", param_hint="--limit")

    row_limit = None if limit == 0 else limit
    try:
        with GatewayClient(base_url, timeout_s=timeout_s) as client:
            table = client.history(
                interval,
                instrument=instrument,
                start=start,
                end=end,
                limit=row_limit,
            )
    except ClientError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Rows: {table.num_rows}")
    typer.echo("Schema (Arrow):")
    typer.echo(table.schema)

    decimal_note = (
        "decimal128 columns keep exact values; do not call astype(float)."
    )
    if pandas:
        frame = table.to_pandas()
        typer.echo("\nDataFrame:")
        typer.echo(frame.to_string(index=False))
        typer.echo(
            "\nNote: decimal columns are pandas object dtype holding "
            "decimal.Decimal. pandas 3 may display timestamps at microsecond "
            "resolution even though the wire value is nanoseconds. "
            + decimal_note
        )
    else:
        typer.echo("\nRows:")
        for row in table.to_pylist():
            typer.echo(row)
        typer.echo(
            "\nNote: Arrow decimals decode to decimal.Decimal and timestamps "
            "to datetime. " + decimal_note
        )
    return 0


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
