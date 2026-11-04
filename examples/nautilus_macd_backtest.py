#!/usr/bin/env python3
"""Backtest a MACD crossover on Bars fetched from a running gateway.

The script is a consumer example:

1. ``mdgw_client.GatewayClient`` queries ``GET /api/v1/bars?format=arrow``
   from the api Service Role (the Docker stack exposes it on
   ``http://127.0.0.1:8080``).
2. The Arrow table is deduplicated and converted to a pandas frame, then
   NautilusTrader's ``BarDataWrangler`` turns that frame into ``Bar`` objects.
   Decimal prices and volumes stay exact; timestamps keep their nanosecond
   values.
3. A NautilusTrader ``BacktestEngine`` runs a MACD crossover strategy and
   prints fills, positions and the account summary.

NautilusTrader is an optional consumer dependency (the gateway itself never
imports it). Install it in the same environment as ``mdgw_client``:

    pip install -U --pre nautilus_trader numpy pandas

Start the Docker stack first:

    docker compose up -d

Then, from the repository root:

    python examples/nautilus_macd_backtest.py \
        --base-url http://127.0.0.1:8080 \
        --instrument binance:BTC/USDT --interval 1m

The Compose demo feed emits a flat price series, so MACD produces no signals.
Point ``--base-url`` at a gateway with real history (or replay/backfilled data)
to see crossings and fills.

Bar timestamp convention: the gateway's ``open_ts`` becomes the Nautilus
``ts_event`` and its ``close_ts`` becomes ``ts_init``. Nautilus uses the event
timestamp as the bar interval start for OHLC execution, so this is the
convention that makes bar-based fills work.
"""

from __future__ import annotations

import sys
from decimal import ROUND_HALF_EVEN, Decimal

import pandas as pd
import pyarrow as pa
import typer

from mdgw_client import ClientError, GatewayClient

try:  # ruff: ignore[too-many-statements-in-try-clause] - optional dependency probe
    from nautilus_trader.backtest import BacktestEngine
    from nautilus_trader.common import LogLevel
    from nautilus_trader.config import (
        BacktestEngineConfig,
        LoggerConfig,
        StrategyConfig,
    )
    from nautilus_trader.execution import MakerTakerFeeModel
    from nautilus_trader.indicators import (
        MovingAverageConvergenceDivergence,
    )
    from nautilus_trader.model import (
        AccountType,
        AggregationSource,
        Bar,
        BarAggregation,
        BarSpecification,
        BarType,
        BookType,
        Currency,
        CurrencyPair,
        InstrumentId,
        Money,
        OmsType,
        OrderSide,
        Price,
        PriceType,
        Quantity,
        Symbol,
        Venue,
    )
    from nautilus_trader.persistence import BarDataWrangler
    from nautilus_trader.trading import Strategy
except ModuleNotFoundError as exc:  # pragma: no cover - environment dependent
    sys.stderr.write(
        "nautilus_trader is required for this example. Install it with:\n"
        "  pip install -U --pre nautilus_trader numpy pandas\n"
        f"(missing module: {exc.name})\n"
    )
    raise SystemExit(1) from exc

app = typer.Typer(
    name="nautilus_macd_backtest.py",
    add_completion=False,
    help="Fetch gateway Bars with mdgw_client and backtest a MACD strategy.",
)

MAX_FIXED_PRECISION = 16
DEFAULT_TRADE_SIZE = "1"
DEFAULT_STARTING_BALANCE = "100000"

_INTERVAL_SPECS: dict[str, tuple[int, BarAggregation]] = {
    "1m": (1, BarAggregation.MINUTE),
    "5m": (5, BarAggregation.MINUTE),
    "15m": (15, BarAggregation.MINUTE),
    "1h": (1, BarAggregation.HOUR),
    "1d": (1, BarAggregation.DAY),
}


# -- Strategy ---------------------------------------------------------------


class MacdConfig(StrategyConfig):
    """Parameters for the MACD crossover strategy."""

    def __init__(  # ruff: ignore[too-many-arguments] - strategy parameters
        self,
        *,
        instrument_id: InstrumentId,
        bar_type: BarType,
        trade_size: Decimal,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        **_kwargs: object,
    ) -> None:
        super().__init__(**_kwargs)
        self.instrument_id = instrument_id
        self.bar_type = bar_type
        self.trade_size = trade_size
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period


class MacdCrossStrategy(Strategy):
    """Go long on a bullish MACD cross and short on a bearish one."""

    def __init__(self, config: MacdConfig) -> None:
        super().__init__(config)
        self.macd = MovingAverageConvergenceDivergence(
            config.fast_period,
            config.slow_period,
            config.signal_period,
        )
        self._previous_value: float | None = None
        self._previous_signal: float | None = None
        self.crosses = 0
        self.submitted = 0

    def on_start(self) -> None:
        # Registering the indicator makes the engine feed it each Bar; the
        # explicit subscribe delivers the Bar to `on_bar`.
        self.register_indicator_for_bars(self.config.bar_type, self.macd)
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, _bar: Bar) -> None:
        if not self.macd.initialized:
            return

        value = self.macd.value
        signal = self.macd.signal
        if self._previous_value is not None and self._previous_signal is not None:
            bullish = (
                self._previous_value <= self._previous_signal and value > signal
            )
            bearish = (
                self._previous_value >= self._previous_signal and value < signal
            )
            if bullish:
                self.crosses += 1
                self._enter(OrderSide.BUY)
            elif bearish:
                self.crosses += 1
                self._enter(OrderSide.SELL)

        self._previous_value = value
        self._previous_signal = signal

    def _enter(self, side: OrderSide) -> None:
        instrument_id = self.config.instrument_id
        instrument = self.cache.instrument(instrument_id)
        if instrument is None:  # pragma: no cover - engine always caches it
            return

        reversing = (
            (side is OrderSide.BUY and self.portfolio.is_net_short(instrument_id))
            or (
                side is OrderSide.SELL
                and self.portfolio.is_net_long(instrument_id)
            )
        )
        if reversing:
            self.close_all_positions(instrument_id)
        elif not self.portfolio.is_net_flat(instrument_id):
            return

        order = self.order_factory.market(
            instrument_id,
            side,
            instrument.make_qty(self.config.trade_size),
        )
        self.submit_order(order)
        self.submitted += 1

    def on_stop(self) -> None:
        self.close_all_positions(self.config.instrument_id)


# -- Gateway -> Nautilus conversion ----------------------------------------


def _decimal_places(value: Decimal) -> int:
    # Arrow decimals keep the contract's fixed scale (18), so normalize first
    # to avoid inflating every instrument to the maximum precision.
    exponent = value.normalize().as_tuple().exponent
    return -exponent if isinstance(exponent, int) and exponent < 0 else 0


def _precision(values: list[Decimal]) -> int:
    observed = max((_decimal_places(value) for value in values), default=0)
    return min(observed, MAX_FIXED_PRECISION)


def _round_to(value: Decimal, precision: int) -> Decimal:
    quantum = Decimal(1).scaleb(-precision)
    return value.quantize(quantum, rounding=ROUND_HALF_EVEN)


def _parse_spot_instrument(
    value: str,
) -> tuple[Venue, Symbol, Currency, Currency]:
    venue_text, separator, symbol_text = value.partition(":")
    if not separator or not venue_text or not symbol_text:
        msg = "instrument must be <venue>:<symbol>, e.g. binance:BTC/USDT"
        raise ValueError(msg)

    base_text, pair_separator, quote_text = symbol_text.partition("/")
    if not pair_separator or not base_text or not quote_text:
        msg = (
            "this example supports spot pairs named <BASE>/<QUOTE>, "
            f"got {symbol_text!r}"
        )
        raise ValueError(msg)

    return (
        Venue(venue_text.upper()),
        Symbol(symbol_text),
        Currency.from_str(base_text),
        Currency.from_str(quote_text),
    )


def _bar_type_for(instrument_id: InstrumentId, interval: str) -> BarType:
    try:
        step, aggregation = _INTERVAL_SPECS[interval]
    except KeyError as exc:
        supported = ", ".join(sorted(_INTERVAL_SPECS))
        msg = f"unsupported interval {interval!r} (supported: {supported})"
        raise ValueError(msg) from exc
    return BarType(
        instrument_id,
        BarSpecification(step, aggregation, PriceType.LAST),
        AggregationSource.EXTERNAL,
    )


def _latest_row_indexes(table: pa.Table) -> list[int]:
    """Dedupe revisions, returning table indexes in ascending `open_ts` order."""
    open_ns = table.column("open_ts").cast(pa.int64()).to_pylist()
    revisions = table.column("revision").to_pylist()
    latest: dict[int, int] = {}
    for index, timestamp in enumerate(open_ns):
        current = latest.get(timestamp)
        if current is None or revisions[index] > revisions[current]:
            latest[timestamp] = index
    return [latest[timestamp] for timestamp in sorted(latest)]


def _table_to_frame(table: pa.Table) -> pd.DataFrame:
    """Convert gateway Arrow Bars to a pandas frame, latest revision only.

    The frame index is the bar open time. ``ts_event`` is the open, ``ts_init``
    is the close, and the OHLCV columns stay as exact ``Decimal`` values.
    """
    if table.num_rows == 0:
        return pd.DataFrame(
            columns=[
                "ts_event",
                "ts_init",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ]
        )

    subset = table.take(pa.array(_latest_row_indexes(table)))
    frame = pd.DataFrame(
        {
            "ts_event": subset.column("open_ts").cast(pa.int64()).to_pylist(),
            "ts_init": subset.column("close_ts").cast(pa.int64()).to_pylist(),
            "open": subset.column("open").to_pylist(),
            "high": subset.column("high").to_pylist(),
            "low": subset.column("low").to_pylist(),
            "close": subset.column("close").to_pylist(),
            "volume": subset.column("volume").to_pylist(),
        }
    )
    # A malformed bar (close not after open) cannot be represented in the
    # backtest engine's interval model.
    frame = frame[frame["ts_init"] > frame["ts_event"]]
    frame.index = pd.to_datetime(frame["ts_event"], unit="ns", utc=True)
    frame.index.name = "timestamp"
    return frame


def _frame_precisions(frame: pd.DataFrame) -> tuple[int, int]:
    prices = [
        value
        for column in ("open", "high", "low", "close")
        for value in frame[column].tolist()
    ]
    return _precision(prices), _precision(frame["volume"].tolist())


def _quantize_frame(
    frame: pd.DataFrame,
    *,
    price_precision: int,
    size_precision: int,
) -> pd.DataFrame:
    quantized = frame.copy()
    for column in ("open", "high", "low", "close"):
        quantized[column] = quantized[column].map(
            lambda value: _round_to(value, price_precision)
        )
    quantized["volume"] = quantized["volume"].map(
        lambda value: _round_to(value, size_precision)
    )
    return quantized


def _instrument_from_precisions(  # ruff: ignore[too-many-arguments] - explicit fields
    *,
    instrument_id: InstrumentId,
    symbol: Symbol,
    base: Currency,
    quote: Currency,
    price_precision: int,
    size_precision: int,
) -> CurrencyPair:
    price_increment = Price(
        Decimal(1).scaleb(-price_precision), price_precision
    )
    size_increment = Quantity(
        Decimal(1).scaleb(-size_precision), size_precision
    )
    return CurrencyPair(
        instrument_id=instrument_id,
        raw_symbol=symbol,
        base_currency=base,
        quote_currency=quote,
        price_precision=price_precision,
        size_precision=size_precision,
        price_increment=price_increment,
        size_increment=size_increment,
        ts_event=0,
        ts_init=0,
        multiplier=Quantity.from_int(1),
        lot_size=size_increment,
        margin_init=Decimal("0.01"),
        margin_maint=Decimal("0.01"),
    )


def _frame_to_bars(
    frame: pd.DataFrame,
    *,
    bar_type: BarType,
    price_precision: int,
    size_precision: int,
) -> list[Bar]:
    """Convert a pandas Bar frame with NautilusTrader's `BarDataWrangler`.

    The 2.x PyO3 wrangler accepts a schema-compatible Arrow IPC stream rather
    than a DataFrame directly, so the frame is encoded with the Bar schema
    reported by `Bar.get_fields()` and passed to
    `process_record_batch_bytes`. Prices and volumes remain exact Decimals.
    """
    if frame.empty:
        return []

    decimal_type = pa.decimal128(38, 16)
    timestamp_type = pa.timestamp("ns", tz="UTC")
    schema = pa.schema(
        [
            *(
                pa.field(column, decimal_type, False)
                for column in ("open", "high", "low", "close", "volume")
            ),
            pa.field("ts_event", timestamp_type, False),
            pa.field("ts_init", timestamp_type, False),
        ],
        metadata=Bar.get_metadata(bar_type, price_precision, size_precision),
    )
    arrays = [
        *(
            pa.array(frame[column].tolist(), type=decimal_type)
            for column in ("open", "high", "low", "close", "volume")
        ),
        pa.array(frame["ts_event"].tolist(), type=timestamp_type),
        pa.array(frame["ts_init"].tolist(), type=timestamp_type),
    ]
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batch)

    wrangler = BarDataWrangler(
        str(bar_type),
        price_precision,
        size_precision,
    )
    return wrangler.process_record_batch_bytes(sink.getvalue().to_pybytes())


# -- CLI --------------------------------------------------------------------


@app.command()
def backtest(  # ruff: ignore[too-many-arguments,too-many-positional-arguments,too-many-locals,too-many-branches,too-many-statements] - CLI surface
    base_url: str = typer.Option(
        "http://127.0.0.1:8080",
        "--base-url",
        metavar="URL",
        help="gateway api base URL (default: http://127.0.0.1:8080)",
    ),
    instrument: str = typer.Option(
        "binance:BTC/USDT",
        "--instrument",
        metavar="INSTRUMENT_ID",
        help="spot Instrument to backtest, e.g. binance:BTC/USDT",
    ),
    interval: str = typer.Option(
        "1m",
        "--interval",
        metavar="INTERVAL",
        help="Bar Interval: 1m / 5m / 15m / 1h / 1d (default: 1m)",
    ),
    start: str | None = typer.Option(
        None,
        "--start",
        metavar="ISO8601",
        help="inclusive open_ts lower bound",
    ),
    end: str | None = typer.Option(
        None,
        "--end",
        metavar="ISO8601",
        help="exclusive open_ts upper bound",
    ),
    limit: int = typer.Option(
        5000,
        "--limit",
        help="maximum Bars to fetch (default: 5000)",
    ),
    fast_period: int = typer.Option(12, "--fast-period"),
    slow_period: int = typer.Option(26, "--slow-period"),
    signal_period: int = typer.Option(9, "--signal-period"),
    trade_size: str = typer.Option(
        DEFAULT_TRADE_SIZE,
        "--trade-size",
        help="base-asset quantity per entry (default: 1)",
    ),
    starting_balance: str = typer.Option(
        DEFAULT_STARTING_BALANCE,
        "--starting-balance",
        help="starting quote-currency balance (default: 100000)",
    ),
    timeout_s: float = typer.Option(10.0, "--timeout-s"),
    reports: bool = typer.Option(
        False,
        "--reports",
        help="print the full fills/positions/account DataFrames",
    ),
    verbose_logs: bool = typer.Option(
        False,
        "--verbose-logs",
        help="show NautilusTrader INFO logs",
    ),
) -> int:
    if limit <= 0:
        message = "--limit must be positive"
        raise typer.BadParameter(message, param_hint="--limit")
    if fast_period <= 0 or slow_period <= 0 or signal_period <= 0:
        message = "MACD periods must be positive"
        raise typer.BadParameter(message, param_hint="--fast-period")
    if fast_period >= slow_period:
        message = "--fast-period must be smaller than --slow-period"
        raise typer.BadParameter(message, param_hint="--fast-period")

    try:
        venue, symbol, base, quote = _parse_spot_instrument(instrument)
        instrument_id = InstrumentId(symbol, venue)
        bar_type = _bar_type_for(instrument_id, interval)
        trade_size_decimal = Decimal(trade_size)
        starting_balance_decimal = Decimal(starting_balance)
    except (ValueError, ArithmeticError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    typer.echo(f"Fetching up to {limit} Bars from {base_url} ...")
    try:
        with GatewayClient(base_url, timeout_s=timeout_s) as client:
            table = client.history(
                interval,
                instrument=instrument,
                start=start,
                end=end,
                limit=limit,
            )
    except ClientError as exc:
        typer.echo(
            "error: cannot fetch Bar history. Is the gateway running?\n"
            f"  docker compose up -d\n  {exc}",
            err=True,
        )
        raise typer.Exit(code=1) from exc

    if table.num_rows == 0:
        typer.echo(
            "no Bars returned. Check --instrument/--interval, widen "
            "--start/--end, or wait for the collector to flush.",
            err=True,
        )
        raise typer.Exit(code=1)

    frame = _table_to_frame(table)
    price_precision, size_precision = _frame_precisions(frame)
    frame = _quantize_frame(
        frame,
        price_precision=price_precision,
        size_precision=size_precision,
    )
    instrument_obj = _instrument_from_precisions(
        instrument_id=instrument_id,
        symbol=symbol,
        base=base,
        quote=quote,
        price_precision=price_precision,
        size_precision=size_precision,
    )
    bars = _frame_to_bars(
        frame,
        bar_type=bar_type,
        price_precision=price_precision,
        size_precision=size_precision,
    )
    if not bars:
        typer.echo("no usable Bars after revision deduplication.", err=True)
        raise typer.Exit(code=1)

    closes = {bar.close.as_decimal() for bar in bars}
    if len(closes) == 1:
        typer.echo(
            "warning: every close price is identical. The Compose demo feed is "
            "flat, so MACD cannot cross. Use a gateway with real history to "
            "see signals.",
            err=True,
        )
    warmup = slow_period + signal_period
    if len(bars) < warmup:
        typer.echo(
            f"warning: only {len(bars)} Bars; MACD({fast_period},"
            f"{slow_period},{signal_period}) needs about {warmup} to initialize.",
            err=True,
        )

    strategy = MacdCrossStrategy(
        MacdConfig(
            instrument_id=instrument_id,
            bar_type=bar_type,
            trade_size=trade_size_decimal,
            fast_period=fast_period,
            slow_period=slow_period,
            signal_period=signal_period,
        )
    )
    engine = BacktestEngine(
        config=BacktestEngineConfig(
            logging=LoggerConfig(
                stdout_level=LogLevel.INFO if verbose_logs else LogLevel.ERROR
            )
        )
    )
    try:
        engine.add_venue(
            venue=venue,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            starting_balances=[Money(starting_balance_decimal, quote)],
            base_currency=quote,
            default_leverage=Decimal(1),
            fee_model=MakerTakerFeeModel(
                maker_rate=Decimal(0),
                taker_rate=Decimal(0),
            ),
            book_type=BookType.L1_MBP,
            bar_execution=True,
        )
        engine.add_instrument(instrument_obj)
        engine.add_data(bars)
        engine.add_strategy(strategy)
        engine.run()
    finally:
        # Reports must be read before dispose().
        fills = engine.generate_order_fills_report()
        positions = engine.generate_positions_report()
        account = engine.generate_account_report(venue=venue)
        engine.dispose()

    typer.echo(
        f"\nBars: {len(bars)}  MACD crosses: {strategy.crosses}  "
        f"entries submitted: {strategy.submitted}  fills: {len(fills)}"
    )
    if positions.empty:
        typer.echo("Positions: none (no round-trip trades).")
    else:
        typer.echo(f"Positions: {len(positions)}")
    if not account.empty:
        typer.echo(f"Final account total: {account['total'].iloc[-1]} {quote}")

    if reports:
        typer.echo("\nFills report:")
        typer.echo(fills.to_string() if not fills.empty else "(empty)")
        typer.echo("\nPositions report:")
        typer.echo(positions.to_string() if not positions.empty else "(empty)")
        typer.echo("\nAccount report:")
        typer.echo(account.to_string() if not account.empty else "(empty)")
    return 0


def main() -> None:
    app()


if __name__ == "__main__":
    main()
