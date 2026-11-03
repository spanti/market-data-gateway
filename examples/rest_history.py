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

import argparse
import sys

from mdgw_client import ClientError, GatewayClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rest_history.py",
        description="Fetch Bar history over REST via mdgw_client.GatewayClient.",
    )
    parser.add_argument(
        "--base-url",
        required=True,
        metavar="URL",
        help="gateway base URL, e.g. http://127.0.0.1:8080",
    )
    parser.add_argument(
        "--instrument",
        metavar="INSTRUMENT_ID",
        help="filter one Instrument, e.g. binance:BTC/USDT",
    )
    parser.add_argument(
        "--interval",
        default="1m",
        metavar="INTERVAL",
        help="Bar Interval (default: 1m)",
    )
    parser.add_argument(
        "--start",
        metavar="ISO8601",
        help="inclusive open_ts lower bound",
    )
    parser.add_argument(
        "--end",
        metavar="ISO8601",
        help="exclusive open_ts upper bound",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="maximum rows to request/print; 0 requests all rows (default: 20)",
    )
    parser.add_argument(
        "--pandas",
        action="store_true",
        help="render with Table.to_pandas() instead of Arrow rows",
    )
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=10.0,
        help="HTTP timeout in seconds (default: 10)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.limit < 0:
        parser.error("--limit must be non-negative")

    limit = None if args.limit == 0 else args.limit
    try:
        with GatewayClient(args.base_url, timeout_s=args.timeout_s) as client:
            table = client.history(
                args.interval,
                instrument=args.instrument,
                start=args.start,
                end=args.end,
                limit=limit,
            )
    except ClientError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"Rows: {table.num_rows}")
    print("Schema (Arrow):")
    print(table.schema)

    decimal_note = (
        "decimal128 columns keep exact values; do not call astype(float)."
    )
    if args.pandas:
        frame = table.to_pandas()
        print("\nDataFrame:")
        print(frame.to_string(index=False))
        print(
            "\nNote: decimal columns are pandas object dtype holding "
            "decimal.Decimal. pandas 3 may display timestamps at microsecond "
            "resolution even though the wire value is nanoseconds. "
            + decimal_note
        )
    else:
        print("\nRows:")
        for row in table.to_pylist():
            print(row)
        print(
            "\nNote: Arrow decimals decode to decimal.Decimal and timestamps "
            "to datetime. " + decimal_note
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
