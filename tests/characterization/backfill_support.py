# ruff: file-ignore[no-self-use]
"""Real-Dataset fixtures shared by the Backfill characterization tests.

Every helper drives an existing public entry point over real Parquet Data Files
and a real High-Water Mark database under ``tmp_path``. Nothing here adds a
production seam or changes runtime behaviour; the Backfill refactor is
characterized with these fixtures before the implementation moves.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from mdgw.config import load_config
from mdgw.core.bar import RawBar
from mdgw.core.time import datetime_to_ns, ns_to_iso
from mdgw.ingest.fake import BarInput
from mdgw.state.high_water_mark import HighWaterMarkStore
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from collections.abc import Generator, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INSTRUMENT = "binance:BTC/USDT"
OTHER_INSTRUMENT = "binance:ETH/USDT"
FEED = "characterization"
BASE_INTERVALS = ("1m", "5m", "15m", "1h", "1d")
TERMINAL_STATES = frozenset({"succeeded", "partially_succeeded", "failed", "cancelled"})


def config(*, backfill_hours: float = 24):
    """The checked-in config with an ephemeral HTTP port."""
    loaded = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = loaded.server.model_copy(update={"http_port": 0})
    collector = loaded.collector.model_copy(update={"backfill_hours": backfill_hours})
    return loaded.model_copy(update={"server": server, "collector": collector})


def at(minute: int = 0, *, hour: int = 0, day: int = 1) -> datetime:
    """A minute-aligned UTC instant shared by every characterization scenario."""
    return datetime(2024, 1, day, hour, minute, tzinfo=UTC)


def iso_at(minute: int = 0, *, hour: int = 0, day: int = 1) -> str:
    return ns_to_iso(datetime_to_ns(at(minute, hour=hour, day=day)))


def bar_input(  # ruff: ignore[too-many-arguments] - explicit fixture fields
    minute: int = 0,
    *,
    hour: int = 0,
    day: int = 1,
    close: str = "100",
    high: str = "101",
    low: str = "99",
    revision: int = 0,
) -> BarInput:
    return BarInput(
        open_ts=at(minute, hour=hour, day=day),
        open="100",
        high=high,
        low=low,
        close=close,
        volume="1",
        trade_count=1,
        revision=revision,
    )


def raw_bar(  # ruff: ignore[too-many-arguments] - explicit fixture fields
    open_ts: datetime,
    *,
    instrument_id: str = INSTRUMENT,
    interval: str = "1m",
    open_: object = "100",
    high: object = "101",
    low: object = "99",
    close: object = "100",
    volume: object = "1",
    trade_count: int | None = 1,
    revision: int = 0,
) -> RawBar:
    """One RawBar with exact Decimals; pass a float to force a rejection."""
    return RawBar(
        instrument_id=instrument_id,
        interval=interval,
        open_ts=open_ts,
        open=_decimal(open_),
        high=_decimal(high),
        low=_decimal(low),
        close=_decimal(close),
        volume=_decimal(volume),
        trade_count=trade_count,
        revision=revision,
    )


def _decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (str, int)):
        return Decimal(value)
    # Deliberately invalid (a float) so Normalization rejects this row.
    return value  # type: ignore[return-value]


def _history_map(
    history: Mapping[str, Sequence[RawBar]] | Sequence[RawBar],
    instrument_ids: Sequence[str],
) -> dict[str, tuple[RawBar, ...]]:
    source = history if isinstance(history, Mapping) else {instrument_ids[0]: history}
    return {
        instrument: tuple(source.get(instrument, ())) for instrument in instrument_ids
    }


class StaticHistoryFeed:
    """History-only Feed Handler over fixed RawBars per Instrument.

    ``filter_by_range=False`` returns rows the automatic filter must drop. A
    real Feed Handler would not normally do that, but ``BackfillReport``
    defines ``fetched_bars`` before filtering, so the public seam accepts it.
    """

    def __init__(
        self,
        *,
        history: Mapping[str, Sequence[RawBar]] | Sequence[RawBar] = (),
        instrument_ids: Sequence[str] = (INSTRUMENT,),
        name: str = FEED,
        supports_history: bool = True,
        filter_by_range: bool = True,
    ) -> None:
        self.name = name
        self.instrument_ids = tuple(instrument_ids)
        self.supports_history = supports_history
        self.filter_by_range = filter_by_range
        self._history = _history_map(history, self.instrument_ids)
        self.requests: list[tuple[str, str, int, int]] = []

    def iter_bars(self):
        return iter(())

    def feed_status(self):
        return ()

    def set_history(
        self,
        history: Mapping[str, Sequence[RawBar]] | Sequence[RawBar],
    ) -> None:
        self._history = _history_map(history, self.instrument_ids)

    def fetch_history(
        self,
        instrument_id: str,
        interval: str,
        start: datetime,
        end: datetime,
    ):
        start_ns = datetime_to_ns(start)
        end_ns = datetime_to_ns(end)
        self.requests.append((instrument_id, interval, start_ns, end_ns))
        raws = self._history.get(instrument_id, ())
        if not self.filter_by_range:
            return raws
        return tuple(
            bar for bar in raws if start_ns <= datetime_to_ns(bar.open_ts) < end_ns
        )


@contextmanager
def open_dataset(
    root: Path,
) -> Generator[tuple[ParquetBarStore, HighWaterMarkStore]]:
    """A real temporary Bars Dataset and its HWM, closed in the safe order."""
    store = ParquetBarStore(root / "bars")
    hwm = HighWaterMarkStore(root / "bars" / "state.db")
    try:
        yield store, hwm
    finally:
        hwm.close()
        store.close()


def count_rows(
    store: ParquetBarStore,
    interval: str,
    *,
    instrument_id: str = INSTRUMENT,
) -> int:
    return store.read_bars(instrument_id=instrument_id, interval=interval).num_rows


def rows_by_open(
    store: ParquetBarStore,
    interval: str,
    *,
    instrument_id: str = INSTRUMENT,
) -> dict[int, list[dict]]:
    grouped: dict[int, list[dict]] = {}
    for row in store.read_bars(
        instrument_id=instrument_id, interval=interval
    ).to_pylist():
        grouped.setdefault(datetime_to_ns(row["open_ts"]), []).append(row)
    return grouped


def _row_content(row: dict) -> tuple:
    """Every logical Bar field except the deliberately unfixed `ingest_ts`."""
    return (
        int(row["schema_version"]),
        str(row["data_type"]),
        str(row["instrument_id"]),
        str(row["interval"]),
        datetime_to_ns(row["open_ts"]),
        datetime_to_ns(row["close_ts"]),
        datetime_to_ns(row["event_ts"]),
        str(row["feed"]),
        row["seq"],
        str(row["aggregation_source"]),
        row["open"],
        row["high"],
        row["low"],
        row["close"],
        row["volume"],
        row["trade_count"],
        int(row["revision"]),
    )


def logical_bars(
    store: ParquetBarStore,
    *,
    instrument_id: str = INSTRUMENT,
    intervals: Sequence[str] = BASE_INTERVALS,
) -> dict[tuple[str, int], tuple[tuple, ...]]:
    """All revisions per logical key, sorted, with `ingest_ts` excluded.

    Data File names, UUIDs and physical file counts never enter this view.
    """
    grouped: dict[tuple[str, int], list[tuple]] = {}
    for interval in intervals:
        rows = store.read_bars(
            instrument_id=instrument_id, interval=interval
        ).to_pylist()
        for row in rows:
            key = (interval, datetime_to_ns(row["open_ts"]))
            grouped.setdefault(key, []).append(_row_content(row))
    return {
        key: tuple(sorted(values, key=lambda value: (value[-1], value)))
        for key, values in sorted(grouped.items())
    }
