"""Behavior tests for realtime/step Replay Modes and session controls.

Seams under test:
- `ReplayService` with a real Parquet Dataset: Replay Clock pacing, Step
  Credits, `pause`/`resume`/`seek`/`set_speed`/`step`, the legal-operation
  matrix, and per-session isolation.
- `ReplayServer` over real ZMQ REQ/REP + PUB: every new control op and the
  resulting event stream.

Timing assertions use injected-style bounds (`Event.wait`, polling on observed
state) rather than assuming a fixed wall-clock schedule.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import pytest
import zmq

from mdgw.bus.zmq_bus import READY_TOPIC, ipc_to_table
from mdgw.core.bar import Bar
from mdgw.core.errors import (
    INVALID_PARAMETER,
    INVALID_RANGE,
    InvalidParameterError,
    InvalidRangeError,
)
from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns, iso_to_ns, ns_to_iso
from mdgw.replay.server import ReplayServer
from mdgw.replay.service import ReplayService
from mdgw.replay.session import ReplaySessionSpec
from mdgw.store.parquet_dataset import ParquetBarStore

UTC = timezone.utc
BASE = datetime(2026, 10, 30, 0, 0, tzinfo=UTC)
BTC = "binance:BTC/USDT"
ETH = "binance:ETH/USDT"

START = "2026-10-30T00:00:00Z"
END = "2026-10-30T00:10:00Z"
START_ISO = ns_to_iso(iso_to_ns(START))


def _bar(
    *,
    instrument_id: str = BTC,
    interval: str = "1m",
    open_minute: int = 0,
    seq: int | None = None,
) -> Bar:
    open_ts = BASE + timedelta(minutes=open_minute)
    close_ts = open_ts + INTERVALS[interval]
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval=interval,
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=seq,
        aggregation_source="external",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


def _populate_minutes(store: ParquetBarStore, minutes: range) -> None:
    store.write_bars(
        [_bar(open_minute=minute) for minute in minutes],
        asset_class="crypto",
    )


def _populate_ordering(store: ParquetBarStore) -> None:
    store.write_bars(
        [
            _bar(instrument_id=BTC, interval="1m", open_minute=0, seq=2),
            _bar(instrument_id=ETH, interval="1m", open_minute=0, seq=1),
            _bar(instrument_id=BTC, interval="5m", open_minute=0),
            _bar(instrument_id=BTC, interval="1m", open_minute=4),
        ],
        asset_class="crypto",
    )


def _create_request(
    *,
    mode: str = "asap",
    speed: float | None = None,
    subscriptions: list[dict[str, str]] | None = None,
    start_ts: str = START,
    end_ts: str = END,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "op": "create_session",
        "subscriptions": subscriptions
        or [{"data_type": "bar", "instrument_id": BTC, "interval": "1m"}],
        "start_ts": start_ts,
        "end_ts": end_ts,
        "replay_mode": mode,
    }
    if speed is not None:
        payload["speed"] = speed
    return payload


def _spec(**kwargs: Any) -> ReplaySessionSpec:
    return ReplaySessionSpec.from_mapping(_create_request(**kwargs))


def _ns(minutes: float) -> int:
    return datetime_to_ns(BASE) + int(minutes * 60 * 1_000_000_000)


def _is_market(topic: str, session_id: str) -> bool:
    return topic.startswith(f"replay.{session_id}.market.")


def _control_topic(session_id: str) -> str:
    return f"replay.{session_id}.control"


class _Collector:
    """Thread-safe publish sink with condition-based waits."""

    def __init__(self) -> None:
        self._messages: list[tuple[str, bytes]] = []
        self._cond = threading.Condition()

    def __call__(self, topic: str, payload: bytes) -> None:
        with self._cond:
            self._messages.append((topic, payload))
            self._cond.notify_all()

    def snapshot(self) -> list[tuple[str, bytes]]:
        with self._cond:
            return list(self._messages)

    def wait_for(
        self,
        predicate: Callable[[list[tuple[str, bytes]]], bool],
        *,
        timeout: float = 10.0,
        description: str = "condition",
    ) -> list[tuple[str, bytes]]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while not predicate(self._messages):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(
                        f"timed out waiting for {description}: {self._messages!r}"
                    )
                self._cond.wait(remaining)
            return list(self._messages)

    def wait_market(
        self, session_id: str, count: int, *, timeout: float = 10.0
    ) -> list[tuple[str, bytes]]:
        return self.wait_for(
            lambda messages: len(
                [1 for topic, _ in messages if _is_market(topic, session_id)]
            )
            >= count,
            timeout=timeout,
            description=f"{count} market message(s) for {session_id}",
        )

    def wait_control(
        self, session_id: str, *, timeout: float = 10.0
    ) -> list[tuple[str, bytes]]:
        return self.wait_for(
            lambda messages: any(
                topic == _control_topic(session_id) for topic, _ in messages
            ),
            timeout=timeout,
            description=f"End of Session for {session_id}",
        )


def _market(messages: list[tuple[str, bytes]], session_id: str) -> list[bytes]:
    return [
        payload
        for topic, payload in messages
        if _is_market(topic, session_id)
    ]


def _end(messages: list[tuple[str, bytes]], session_id: str) -> dict[str, Any]:
    for topic, payload in reversed(messages):
        if topic == _control_topic(session_id):
            return json.loads(payload.decode("utf-8"))
    raise AssertionError(f"no End of Session for {session_id!r}")


def _close_minutes(payloads: list[bytes]) -> list[float]:
    result: list[float] = []
    for payload in payloads:
        close_ts = ipc_to_table(payload).column("close_ts").to_pylist()[0]
        result.append((close_ts - BASE).total_seconds() / 60)
    return result


def _wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 5.0,
    description: str = "state",
) -> None:
    """Bounded condition polling (not a timing assertion)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {description}")


class _BlockingReadStore:
    """Proxy whose Parquet read waits behind a test-controlled gate."""

    def __init__(self, inner: ParquetBarStore, gate: threading.Event) -> None:
        self._inner = inner
        self._gate = gate

    @property
    def dataset_root(self) -> Path:
        return self._inner.dataset_root

    def iter_batches(self, **kwargs: Any):
        assert self._gate.wait(5), "test never released the read gate"
        yield from self._inner.iter_batches(**kwargs)


class _GatePublisher:
    """Blocks the first publish until released, then records every message."""

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()
        self.messages: list[tuple[str, bytes]] = []
        self._lock = threading.Lock()
        self._first = True

    def __call__(self, topic: str, payload: bytes) -> None:
        with self._lock:
            if self._first:
                self._first = False
                block = True
            else:
                block = False
        if block:
            self.entered.set()
            assert self.release.wait(5), "test never released the first publish"
        with self._lock:
            self.messages.append((topic, payload))


# -- realtime --------------------------------------------------------------


def test_realtime_streams_ordered_bars_then_end_of_session(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_ordering(store)
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-rt"
    )

    spec = _spec(
        mode="realtime",
        speed=100_000.0,
        subscriptions=[
            {"data_type": "bar", "instrument_id": BTC, "interval": "1m"},
            {"data_type": "bar", "instrument_id": ETH, "interval": "1m"},
            {"data_type": "bar", "instrument_id": BTC, "interval": "5m"},
        ],
    )
    session_id = service.create_session(spec)
    messages = collector.wait_control(session_id)
    assert service.wait_session(session_id, timeout=10)

    market = _market(messages, session_id)
    assert [ipc_to_table(payload).column("interval").to_pylist()[0] for payload in market] == [
        "1m",
        "1m",
        "1m",
        "5m",
    ]
    assert _close_minutes(market) == [1, 1, 5, 5]
    assert [ipc_to_table(payload).column("seq").to_pylist()[0] for payload in market] == [
        2,
        1,
        None,
        None,
    ]
    end = _end(messages, session_id)
    assert end["reason"] == "exhausted"
    assert end["bars_sent"] == 4
    assert service.session_status(session_id) == "finished"
    service.close()


def test_realtime_pause_freezes_the_clock_and_resume_advances(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(1))  # close = start_ts + 1m
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-pause"
    )
    session_id = service.create_session(_spec(mode="realtime", speed=1.0))
    try:
        # Wait until the worker has read the Bars and started the Clock.
        _wait_until(
            lambda: service.session_info(session_id)["position_ts"] != START_ISO,
            description="the Replay Clock to start",
        )

        service.pause_session(session_id)
        assert service.pause_session(session_id) is True  # idempotent
        info = service.session_info(session_id)
        assert info["paused"] is True
        frozen = info["position_ts"]

        # Deliberate short negative window: the first Bar is still 1 minute
        # away at speed 1, so a paused session must publish nothing at all.
        threading.Event().wait(0.2)
        assert service.session_info(session_id)["position_ts"] == frozen
        assert _market(collector.snapshot(), session_id) == []

        service.resume_session(session_id)
        assert service.resume_session(session_id) is True  # idempotent
        _wait_until(
            lambda: service.session_info(session_id)["position_ts"] != frozen,
            description="the Replay Clock to resume",
        )

        # Finish the minute quickly instead of waiting 60 wall seconds.
        service.set_speed_session(session_id, 1_000_000.0)
        messages = collector.wait_control(session_id, timeout=10)
        assert len(_market(messages, session_id)) == 1
        assert _end(messages, session_id)["bars_sent"] == 1
    finally:
        service.stop_session(session_id)
        service.wait_session(session_id, timeout=10)
        service.close()


def test_set_speed_while_paused_is_applied_on_resume(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(1))
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-speed"
    )
    session_id = service.create_session(_spec(mode="realtime", speed=1.0))
    try:
        # The Session may still be reading; pause is legal either way.
        service.pause_session(session_id)
        service.set_speed_session(session_id, 1_000_000.0)
        info = service.session_info(session_id)
        assert info["paused"] is True
        assert info["speed"] == 1_000_000.0

        # Deliberate short negative window while paused.
        threading.Event().wait(0.1)
        assert _market(collector.snapshot(), session_id) == []

        service.resume_session(session_id)
        messages = collector.wait_control(session_id, timeout=10)
        assert len(_market(messages, session_id)) == 1
        assert _end(messages, session_id)["bars_sent"] == 1
    finally:
        service.stop_session(session_id)
        service.wait_session(session_id, timeout=10)
        service.close()


def test_stop_while_paused_sends_stopped_eos(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(2))
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-stop-paused"
    )
    session_id = service.create_session(_spec(mode="realtime", speed=1.0))
    try:
        service.pause_session(session_id)
        assert service.stop_session(session_id) is True

        messages = collector.wait_control(session_id, timeout=10)
        assert _market(messages, session_id) == []
        assert _end(messages, session_id) == {
            "op": "end_of_session",
            "session_id": session_id,
            "reason": "stopped",
            "bars_sent": 0,
        }
        assert service.wait_session(session_id, timeout=10)
        assert service.session_status(session_id) == "stopped"
    finally:
        service.close()


# -- step ------------------------------------------------------------------


def test_step_sends_exactly_the_credited_bars(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(5))
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-step"
    )
    session_id = service.create_session(_spec(mode="step"))
    try:
        service.step_session(session_id, 2)
        messages = collector.wait_market(session_id, 2)
        assert len(_market(messages, session_id)) == 2
        assert not any(
            topic == _control_topic(session_id) for topic, _ in messages
        )

        # Deliberate short negative window: a second Bar is not due until
        # another credit arrives, not merely because time passes.
        threading.Event().wait(0.2)
        assert len(_market(collector.snapshot(), session_id)) == 2
        assert service.session_info(session_id)["credits"] == 0

        service.step_session(session_id, 1)
        assert len(_market(collector.wait_market(session_id, 3), session_id)) == 3

        service.step_session(session_id, 2)
        messages = collector.wait_control(session_id, timeout=10)  # last Bar -> EOS
        assert len(_market(messages, session_id)) == 5
        assert _end(messages, session_id) == {
            "op": "end_of_session",
            "session_id": session_id,
            "reason": "exhausted",
            "bars_sent": 5,
        }
    finally:
        service.close()


def test_step_credits_accumulate(tmp_path: Path) -> None:
    inner = ParquetBarStore(tmp_path / "data")
    _populate_minutes(inner, range(4))
    gate = threading.Event()
    store = _BlockingReadStore(inner, gate)
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-credit"
    )
    session_id = service.create_session(_spec(mode="step"))
    try:
        service.step_session(session_id, 2)
        service.step_session(session_id, 2)
        # The worker is still blocked in the Parquet read, so no credit has
        # been consumed yet: both grants must be additive.
        assert service.session_info(session_id)["credits"] == 4

        gate.set()
        messages = collector.wait_control(session_id, timeout=10)
        assert len(_market(messages, session_id)) == 4
        assert _end(messages, session_id)["bars_sent"] == 4
    finally:
        gate.set()
        service.stop_session(session_id)
        service.wait_session(session_id, timeout=10)
        service.close()


def test_step_last_bar_sends_eos_without_another_credit(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(1))
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-last"
    )
    session_id = service.create_session(_spec(mode="step"))
    try:
        service.step_session(session_id, 1)
        messages = collector.wait_control(session_id, timeout=10)
        assert len(_market(messages, session_id)) == 1
        assert _end(messages, session_id)["reason"] == "exhausted"
        assert _end(messages, session_id)["bars_sent"] == 1
        assert service.wait_session(session_id, timeout=10)
        assert service.session_status(session_id) == "finished"
    finally:
        service.close()


def test_step_empty_session_sends_eos_immediately(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-empty"
    )
    session_id = service.create_session(_spec(mode="step"))

    messages = collector.wait_control(session_id, timeout=5)
    assert _market(messages, session_id) == []
    assert _end(messages, session_id) == {
        "op": "end_of_session",
        "session_id": session_id,
        "reason": "exhausted",
        "bars_sent": 0,
    }
    service.close()


def test_step_control_is_not_blocked_by_a_slow_publish(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(2))
    publisher = _GatePublisher()
    service = ReplayService(
        store, publish=publisher, session_id_factory=lambda: "rpl-step-block"
    )
    session_id = service.create_session(_spec(mode="step"))
    try:
        service.step_session(session_id, 2)
        assert publisher.entered.wait(5), "the worker never published"

        returned = threading.Event()

        def grant_more() -> None:
            service.step_session(session_id, 1)
            returned.set()

        thread = threading.Thread(target=grant_more, daemon=True)
        thread.start()
        # If `step` waited on the worker (blocked in publish), this would hang.
        assert returned.wait(2), "step blocked behind an in-flight publish"

        publisher.release.set()
        assert service.wait_session(session_id, timeout=10)
        end = _end(publisher.messages, session_id)
        assert end["bars_sent"] == 2
        assert end["reason"] == "exhausted"
    finally:
        publisher.release.set()
        service.stop_session(session_id)
        service.wait_session(session_id, timeout=10)
        service.close()


# -- seek ------------------------------------------------------------------


def test_seek_while_running_is_rejected(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(3))
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-seek-run"
    )
    session_id = service.create_session(_spec(mode="realtime", speed=1.0))
    try:
        _wait_until(
            lambda: service.session_info(session_id)["position_ts"] != START_ISO,
            description="the Replay Clock to start",
        )

        with pytest.raises(InvalidParameterError) as excinfo:
            service.seek_session(session_id, _ns(1))
        assert excinfo.value.code == INVALID_PARAMETER
        assert "paused" in excinfo.value.message
    finally:
        service.stop_session(session_id)
        service.wait_session(session_id, timeout=10)
        service.close()


def test_seek_forward_skips_bars_while_paused(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(4))  # close 1, 2, 3, 4 minutes
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-seek-skip"
    )
    session_id = service.create_session(_spec(mode="step"))
    try:
        service.pause_session(session_id)
        # 1.5 minutes: the first Bar with close_ts >= 1.5m is the 2m Bar.
        service.seek_session(session_id, _ns(1.5))

        service.step_session(session_id, 1)
        messages = collector.wait_market(session_id, 1)
        assert _close_minutes(_market(messages, session_id)) == [2]

        service.step_session(session_id, 10)
        messages = collector.wait_control(session_id, timeout=10)
        assert _close_minutes(_market(messages, session_id)) == [2, 3, 4]
        assert _end(messages, session_id)["bars_sent"] == 3
    finally:
        service.close()


def test_seek_backward_resends_already_sent_bars(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(3))
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-seek-back"
    )
    session_id = service.create_session(_spec(mode="step"))
    try:
        service.step_session(session_id, 2)
        messages = collector.wait_market(session_id, 2)
        assert _close_minutes(_market(messages, session_id)) == [1, 2]

        service.seek_session(session_id, _ns(1))  # exactly the 1m Bar's close
        service.step_session(session_id, 1)
        messages = collector.wait_market(session_id, 3)
        assert _close_minutes(_market(messages, session_id)) == [1, 2, 1]

        service.step_session(session_id, 10)
        messages = collector.wait_control(session_id, timeout=10)
        assert _close_minutes(_market(messages, session_id)) == [1, 2, 1, 2, 3]
        assert _end(messages, session_id)["bars_sent"] == 5
    finally:
        service.close()


def test_seek_to_the_end_sends_eos(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(2))
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-seek-end"
    )
    session_id = service.create_session(_spec(mode="step"))
    try:
        service.seek_session(session_id, iso_to_ns(END))

        messages = collector.wait_control(session_id, timeout=5)
        assert _market(messages, session_id) == []
        assert _end(messages, session_id) == {
            "op": "end_of_session",
            "session_id": session_id,
            "reason": "exhausted",
            "bars_sent": 0,
        }
        assert service.wait_session(session_id, timeout=10)
        assert service.session_status(session_id) == "finished"
    finally:
        service.close()


def test_realtime_seek_to_the_end_while_paused_sends_eos(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(2))
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-rt-seek-end"
    )
    session_id = service.create_session(_spec(mode="realtime", speed=1.0))
    try:
        service.pause_session(session_id)
        service.seek_session(session_id, iso_to_ns(END))

        messages = collector.wait_control(session_id, timeout=5)
        assert _market(messages, session_id) == []
        assert _end(messages, session_id)["reason"] == "exhausted"
        assert _end(messages, session_id)["bars_sent"] == 0
        assert service.wait_session(session_id, timeout=10)
        assert service.session_status(session_id) == "finished"
    finally:
        service.stop_session(session_id)
        service.close()


def test_seek_outside_the_session_range_is_invalid_range(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(2))
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-seek-range"
    )
    session_id = service.create_session(_spec(mode="step"))
    try:
        for ts_ns in (iso_to_ns(START) - 1, iso_to_ns(END) + 1):
            with pytest.raises(InvalidRangeError) as excinfo:
                service.seek_session(session_id, ts_ns)
            assert excinfo.value.code == INVALID_RANGE
    finally:
        service.close()


# -- illegal operation matrix ---------------------------------------------


def test_asap_rejects_every_control_op(tmp_path: Path) -> None:
    inner = ParquetBarStore(tmp_path / "data")
    _populate_minutes(inner, range(2))
    gate = threading.Event()
    store = _BlockingReadStore(inner, gate)
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-asap-ctl"
    )
    session_id = service.create_session(_spec(mode="asap"))
    try:
        # The read gate keeps the Session alive even though asap is fast.
        for operation in (
            lambda: service.pause_session(session_id),
            lambda: service.resume_session(session_id),
            lambda: service.seek_session(session_id, _ns(1)),
            lambda: service.set_speed_session(session_id, 2.0),
            lambda: service.step_session(session_id, 1),
        ):
            with pytest.raises(InvalidParameterError) as excinfo:
                operation()
            assert excinfo.value.code == INVALID_PARAMETER
    finally:
        gate.set()
        service.stop_session(session_id)
        service.wait_session(session_id, timeout=10)
        service.close()


def test_realtime_rejects_step(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(2))
    service = ReplayService(
        store, publish=_Collector(), session_id_factory=lambda: "rpl-rt-step"
    )
    session_id = service.create_session(_spec(mode="realtime", speed=1.0))
    try:
        with pytest.raises(InvalidParameterError):
            service.step_session(session_id, 1)
    finally:
        service.stop_session(session_id)
        service.wait_session(session_id, timeout=10)
        service.close()


def test_step_rejects_resume_and_set_speed(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(2))
    service = ReplayService(
        store, publish=_Collector(), session_id_factory=lambda: "rpl-step-ctl"
    )
    session_id = service.create_session(_spec(mode="step"))
    try:
        with pytest.raises(InvalidParameterError):
            service.resume_session(session_id)
        with pytest.raises(InvalidParameterError):
            service.set_speed_session(session_id, 2.0)
    finally:
        service.stop_session(session_id)
        service.wait_session(session_id, timeout=10)
        service.close()


def test_finished_session_rejects_controls_but_stop_is_ok(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(1))
    collector = _Collector()
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: "rpl-finished"
    )
    session_id = service.create_session(_spec(mode="asap"))
    collector.wait_control(session_id, timeout=10)
    assert service.wait_session(session_id, timeout=10)

    for operation in (
        lambda: service.pause_session(session_id),
        lambda: service.resume_session(session_id),
        lambda: service.seek_session(session_id, _ns(0)),
        lambda: service.set_speed_session(session_id, 2.0),
        lambda: service.step_session(session_id, 1),
    ):
        with pytest.raises(InvalidParameterError):
            operation()

    assert service.stop_session(session_id) is True
    assert service.session_status(session_id) == "finished"
    service.close()


# -- multi-session isolation ----------------------------------------------


def test_two_step_sessions_do_not_affect_each_other(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(3))
    collector = _Collector()
    ids = iter(["rpl-iso-a", "rpl-iso-b"])
    service = ReplayService(
        store, publish=collector, session_id_factory=lambda: next(ids)
    )
    first = service.create_session(_spec(mode="step"))
    second = service.create_session(_spec(mode="step"))
    try:
        service.step_session(first, 1)
        collector.wait_market(first, 1)
        # Deliberate short negative window: crediting one Session must never
        # advance the other.
        threading.Event().wait(0.2)
        assert _market(collector.snapshot(), second) == []
        assert service.session_info(first)["credits"] == 0
        assert service.session_info(second)["credits"] == 0

        service.step_session(second, 2)
        collector.wait_market(second, 2)
        assert len(_market(collector.snapshot(), first)) == 1
        assert len(_market(collector.snapshot(), second)) == 2

        service.step_session(first, 10)
        first_messages = collector.wait_control(first, timeout=10)
        assert _end(first_messages, first)["bars_sent"] == 3
        assert not any(
            topic == _control_topic(second)
            for topic, _ in collector.snapshot()
        )

        service.step_session(second, 10)
        second_messages = collector.wait_control(second, timeout=10)
        assert _end(second_messages, second)["bars_sent"] == 3
    finally:
        service.close()


# -- ZMQ control plane -----------------------------------------------------


def _wait_ready(subscriber: zmq.Socket, *, timeout_s: float = 5.0) -> None:
    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        remaining_ms = max(1, int((deadline - time.monotonic()) * 1000))
        if dict(poller.poll(min(100, remaining_ms))).get(subscriber) != zmq.POLLIN:
            continue
        topic, *_ = subscriber.recv_multipart()
        if topic.decode("utf-8") == READY_TOPIC:
            return
    raise AssertionError("Replay PUB never sent the ready handshake")


def _request(socket: zmq.Socket, payload: dict[str, Any]) -> dict[str, Any]:
    socket.send_string(json.dumps(payload))
    return json.loads(socket.recv().decode("utf-8"))


def _drain_session(
    subscriber: zmq.Socket,
    session_id: str,
    *,
    count: int | None = None,
    until_control: bool = False,
    buffer: list[tuple[str, bytes]] | None = None,
    timeout_s: float = 10.0,
) -> list[tuple[str, bytes]]:
    prefix = f"replay.{session_id}."
    control = _control_topic(session_id)
    received: list[tuple[str, bytes]] = list(buffer or [])

    def satisfied() -> bool:
        if until_control and any(topic == control for topic, _ in received):
            return True
        if count is not None:
            return len([1 for topic, _ in received if _is_market(topic, session_id)]) >= count
        return False

    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)
    deadline = time.monotonic() + timeout_s
    while not satisfied():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(
                f"timed out waiting for {session_id!r} events: {received!r}"
            )
        if dict(poller.poll(min(100, int(remaining * 1000) or 1))).get(
            subscriber
        ) != zmq.POLLIN:
            continue
        frames = subscriber.recv_multipart()
        topic = frames[0].decode("utf-8")
        if topic == READY_TOPIC:
            continue
        if topic.startswith(prefix):
            received.append((topic, frames[1]))
    return received


def test_zmq_control_ops_drive_realtime_and_step_sessions(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate_minutes(store, range(4))  # close 1, 2, 3, 4 minutes
    ids = iter(["rpl-zmq-rt", "rpl-zmq-step"])
    service = ReplayService(store, session_id_factory=lambda: next(ids))
    server = ReplayServer(service)
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    requester = context.socket(zmq.REQ)
    requester.setsockopt(zmq.LINGER, 0)
    requester.setsockopt(zmq.RCVTIMEO, 5000)
    try:
        server.start()
        subscriber.connect(server.publish_endpoint)
        requester.connect(server.control_endpoint)
        _wait_ready(subscriber)

        # realtime: pause -> seek past the first two Bars -> set_speed ->
        # resume. All four control ops over real REQ/REP.
        created = _request(
            requester,
            _create_request(mode="realtime", speed=0.001),
        )
        assert created["ok"] is True
        realtime_id = created["session_id"]
        assert realtime_id == "rpl-zmq-rt"

        for op, extra in (
            ("pause", {}),
            ("seek", {"ts": "2026-10-30T00:02:30Z"}),
            ("set_speed", {"speed": 1_000_000.0}),
            ("resume", {}),
        ):
            response = _request(
                requester,
                {"op": op, "session_id": realtime_id, **extra},
            )
            assert response == {"ok": True, "session_id": realtime_id}

        messages = _drain_session(subscriber, realtime_id, until_control=True)
        assert _close_minutes(_market(messages, realtime_id)) == [3, 4]
        assert _end(messages, realtime_id)["bars_sent"] == 2

        # step: the new `step` op grants credits over REQ/REP.
        created = _request(requester, _create_request(mode="step"))
        assert created["ok"] is True
        step_id = created["session_id"]
        assert step_id == "rpl-zmq-step"

        response = _request(
            requester, {"op": "step", "session_id": step_id, "credits": 1}
        )
        assert response == {"ok": True, "session_id": step_id}
        first_batch = _drain_session(subscriber, step_id, count=1)
        assert len(_market(first_batch, step_id)) == 1

        response = _request(
            requester, {"op": "step", "session_id": step_id, "credits": 10}
        )
        assert response == {"ok": True, "session_id": step_id}
        messages = _drain_session(
            subscriber, step_id, until_control=True, buffer=first_batch
        )
        assert _close_minutes(_market(messages, step_id)) == [1, 2, 3, 4]
        assert _end(messages, step_id)["bars_sent"] == 4

        # stop stays idempotently ok after either Session finished.
        assert _request(
            requester, {"op": "stop", "session_id": realtime_id}
        ) == {"ok": True, "session_id": realtime_id}
        assert _request(
            requester, {"op": "stop", "session_id": step_id}
        ) == {"ok": True, "session_id": step_id}
        assert service.wait_session(realtime_id, timeout=5)
        assert service.wait_session(step_id, timeout=5)
    finally:
        subscriber.close()
        requester.close()
        server.stop()
        context.term()
