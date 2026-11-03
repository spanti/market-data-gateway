"""Live ingest uses the same identity/revision rules as Backfill.

A repeated closed Bar must not append or publish twice; a corrected Bar at an
existing `open_ts` must append `revision+1` and republish its parent too.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mdgw.config import load_config
from mdgw.core.bar import RawBar
from mdgw.core.time import datetime_to_ns
from mdgw.ingest.fake import BarInput
from mdgw.runtime import GatewayRuntime

PROJECT_ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc
INSTRUMENT = "binance:BTC/USDT"
START = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def _config():
    config = load_config(PROJECT_ROOT / "config" / "gateway.toml")
    server = config.server.model_copy(update={"http_port": 0})
    return config.model_copy(update={"server": server})


class BatchFeedHandler:
    """Emit one scripted batch per `iter_bars()` call, then nothing."""

    name = "fake"
    supports_history = False
    interval = "1m"

    def __init__(self, batches: Sequence[Sequence[BarInput]]) -> None:
        self.instrument_id = INSTRUMENT
        self._batches = [tuple(batch) for batch in batches]
        self._calls = 0

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        return (self.instrument_id,)

    def iter_bars(self) -> Iterator[RawBar]:
        if self._calls >= len(self._batches):
            return
        batch = self._batches[self._calls]
        self._calls += 1
        for bar in batch:
            yield RawBar(
                instrument_id=self.instrument_id,
                interval="1m",
                open_ts=bar.open_ts,
                open=Decimal(str(bar.open)),
                high=Decimal(str(bar.high)),
                low=Decimal(str(bar.low)),
                close=Decimal(str(bar.close)),
                volume=Decimal(str(bar.volume)),
                trade_count=bar.trade_count,
                revision=bar.revision,
            )


def _bar(minute: int, *, close: str = "100", high: str = "101") -> BarInput:
    return BarInput(
        open_ts=START + timedelta(minutes=minute),
        open="100",
        high=high,
        low="99",
        close=close,
        volume="1",
        trade_count=1,
    )


def _bars(*minutes: int) -> list[BarInput]:
    return [_bar(minute) for minute in minutes]


def _rows(runtime: GatewayRuntime, interval: str) -> list[dict]:
    return runtime.store.read_bars(
        instrument_id=INSTRUMENT, interval=interval
    ).to_pylist()


def test_repeating_the_same_live_bars_writes_and_publishes_once(
    tmp_path: Path,
) -> None:
    feed = BatchFeedHandler([_bars(0, 1, 2, 3, 4), _bars(0, 1, 2, 3, 4)])
    with GatewayRuntime(feed_handler=feed, config=_config(), data_root=tmp_path) as runtime:
        runtime.start()
        assert runtime.ingest() == 5
        published = runtime.received_bar_count

        # The second identical ingest drains the Feed Handler but changes
        # nothing: no Store write, no ZMQ publish.
        assert runtime.ingest() == 5
        assert runtime.received_bar_count == published

        one_minute = _rows(runtime, "1m")
        assert len(one_minute) == 5
        assert sorted(row["revision"] for row in one_minute) == [0] * 5
        assert len(_rows(runtime, "5m")) == 1


def test_live_correction_appends_a_revision_and_republishes_the_parent(
    tmp_path: Path,
) -> None:
    corrected = _bars(0, 1, 2, 3, 4)
    corrected[-1] = _bar(4, close="200", high="200")
    feed = BatchFeedHandler([_bars(0, 1, 2, 3, 4), corrected])

    with GatewayRuntime(feed_handler=feed, config=_config(), data_root=tmp_path) as runtime:
        runtime.start()
        runtime.ingest()
        published = runtime.received_bar_count

        runtime.ingest()

        # The corrected 1m Bar and its 5m parent are both published.
        assert runtime.received_bar_count == published + 2

        one_minute = [
            row for row in _rows(runtime, "1m") if row["open_ts"] == START + timedelta(minutes=4)
        ]
        assert sorted(row["revision"] for row in one_minute) == [0, 1]
        latest = max(one_minute, key=lambda row: row["revision"])
        assert latest["close"] == Decimal("200")
        assert latest["high"] == Decimal("200")

        five_minute = [
            row for row in _rows(runtime, "5m") if row["open_ts"] == START
        ]
        assert sorted(row["revision"] for row in five_minute) == [0, 1]
        assert max(five_minute, key=lambda row: row["revision"])["close"] == Decimal("200")

        assert runtime.high_water_mark(INSTRUMENT, "1m") == datetime_to_ns(
            START + timedelta(minutes=4)
        )
