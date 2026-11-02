"""Replay Session control and event consumption through the SDK.

A real ``ReplayServer`` (ZMQ PUB + REQ/REP) and Parquet store back every test,
so asap ordering, Step Credits, realtime controls and End of Session are
exercised end to end.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from mdgw.core.bar import Bar
from mdgw.core.schema import INTERVALS
from mdgw.core.time import datetime_to_ns, iso_to_ns
from mdgw.replay.server import ReplayServer
from mdgw.replay.service import ReplayService
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw_client import ClientError, GatewayClient, ReplaySession

UTC = timezone.utc
BASE = datetime(2026, 10, 30, 0, 0, tzinfo=UTC)
BTC = "binance:BTC/USDT"
START = "2026-10-30T00:00:00Z"
END = "2026-10-30T00:10:00Z"


def _bar(open_minute: int, *, instrument_id: str = BTC) -> Bar:
    open_ts = BASE + timedelta(minutes=open_minute)
    close_ts = open_ts + INTERVALS["1m"]
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=instrument_id,
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=None,
        aggregation_source="external",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


def _populate(store: ParquetBarStore, minutes: range) -> None:
    store.write_bars(
        [_bar(minute) for minute in minutes], asset_class="crypto"
    )


def _close_minutes(events) -> list[float]:
    result: list[float] = []
    for event in events:
        close_ts = event.table.column("close_ts").to_pylist()[0]
        result.append((close_ts - BASE).total_seconds() / 60)
    return result


def _next_events(
    session: ReplaySession, count: int, *, timeout_s: float = 10.0
):
    events = []
    deadline = time.monotonic() + timeout_s
    while len(events) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(
                f"only {len(events)} of {count} events arrived: {events!r}"
            )
        try:
            events.append(session.next_event(timeout=min(0.5, remaining)))
        except StopIteration:
            raise AssertionError(
                f"End of Session after {len(events)} of {count} events"
            )
        except ClientError as exc:
            if exc.code != "timeout":
                raise
    return events


def _collect_until_eos(session: ReplaySession, *, timeout_s: float = 10.0):
    events = []
    deadline = time.monotonic() + timeout_s
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(
                f"no End of Session within {timeout_s}s: {events!r}"
            )
        try:
            events.append(session.next_event(timeout=min(0.5, remaining)))
        except StopIteration:
            return events
        except ClientError as exc:
            if exc.code != "timeout":
                raise


def _client(server: ReplayServer) -> GatewayClient:
    return GatewayClient(
        "http://127.0.0.1:1",
        replay_publish_endpoint=server.publish_endpoint,
        replay_control_endpoint=server.control_endpoint,
        timeout_s=10.0,
        reconnect_backoff_s=(0.01, 0.02),
    )


def _subscriptions():
    return [
        {"data_type": "bar", "instrument_id": BTC, "interval": "1m"}
    ]


def _wait_until(predicate, *, timeout_s: float = 5.0) -> bool:
    """Poll ``predicate`` with a bounded deadline (never a blind sleep)."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_asap_streams_in_order_without_losing_the_first_event(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store, range(5))
    server = ReplayServer(ReplayService(store))
    server.start()
    client = _client(server)
    session = None
    try:
        session = client.replay.create_session(
            _subscriptions(), START, END, replay_mode="asap"
        )
        events = _collect_until_eos(session)

        assert len(events) == 5
        assert _close_minutes(events) == [1, 2, 3, 4, 5]
        assert all(event.kind == "bar" for event in events)
        assert all(event.topic.startswith(f"replay.{session.session_id}.") for event in events)
        assert session.end_of_session == {
            "op": "end_of_session",
            "session_id": session.session_id,
            "reason": "exhausted",
            "bars_sent": 5,
        }
    finally:
        if session is not None:
            session.close()
        client.close()
        server.stop()


def test_step_streams_exactly_the_credited_bars_then_eos(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store, range(3))
    server = ReplayServer(ReplayService(store))
    server.start()
    client = _client(server)
    session = None
    try:
        session = client.replay.create_session(
            _subscriptions(), START, END, replay_mode="step"
        )
        session.step(2)
        first = _next_events(session, 2)
        assert _close_minutes(first) == [1, 2]
        assert session.ended is False

        # No extra Bar without another credit.
        with pytest.raises(ClientError) as excinfo:
            session.next_event(timeout=0.3)
        assert excinfo.value.code == "timeout"

        session.step(10)
        rest = _collect_until_eos(session)
        assert _close_minutes(rest) == [3]
        assert session.end_of_session["reason"] == "exhausted"
        assert session.end_of_session["bars_sent"] == 3
    finally:
        if session is not None:
            session.close()
        client.close()
        server.stop()


def test_realtime_pause_set_speed_and_resume(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store, range(1))  # first close is 1 minute after start
    server = ReplayServer(ReplayService(store))
    server.start()
    client = _client(server)
    session = None
    try:
        session = client.replay.create_session(
            _subscriptions(),
            START,
            END,
            replay_mode="realtime",
            speed=1.0,
        )
        session.pause()
        # At speed 1.0 the first Bar is still 60 s away: nothing may arrive.
        with pytest.raises(ClientError) as excinfo:
            session.next_event(timeout=0.2)
        assert excinfo.value.code == "timeout"

        session.set_speed(1_000_000.0)
        session.resume()
        events = _collect_until_eos(session)
        assert _close_minutes(events) == [1]
        assert session.end_of_session["reason"] == "exhausted"
        assert session.end_of_session["bars_sent"] == 1
    finally:
        if session is not None:
            session.close()
        client.close()
        server.stop()


def test_seek_while_paused_and_out_of_range_error(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store, range(4))  # closes at 1, 2, 3, 4 minutes
    server = ReplayServer(ReplayService(store))
    server.start()
    client = _client(server)
    first = None
    second = None
    try:
        first = client.replay.create_session(
            _subscriptions(), START, END, replay_mode="step"
        )
        first.seek("2026-10-30T00:01:30Z")
        first.step(10)
        events = _collect_until_eos(first)
        assert _close_minutes(events) == [2, 3, 4]
        first.close()

        second = client.replay.create_session(
            _subscriptions(), START, END, replay_mode="step"
        )
        with pytest.raises(ClientError) as excinfo:
            second.seek(iso_to_ns(END) + 1)
        assert excinfo.value.code == "invalid_range"
        # Still active after the rejected seek; stop cleanly.
        second.stop()
    finally:
        if first is not None:
            first.close()
        if second is not None:
            second.close()
        client.close()
        server.stop()


def test_stop_produces_a_stopped_end_of_session(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store, range(4))
    server = ReplayServer(ReplayService(store))
    server.start()
    client = _client(server)
    session = None
    try:
        session = client.replay.create_session(
            _subscriptions(),
            START,
            END,
            replay_mode="realtime",
            speed=1.0,
        )
        session.stop()
        assert _collect_until_eos(session) == []
        assert session.end_of_session["reason"] == "stopped"
        assert session.end_of_session["bars_sent"] == 0
    finally:
        if session is not None:
            session.close()
        client.close()
        server.stop()


def test_control_errors_are_mapped_to_client_error(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store, range(2))
    server = ReplayServer(ReplayService(store))
    server.start()
    client = _client(server)
    session = None
    try:
        session = client.replay.create_session(
            _subscriptions(), START, END, replay_mode="step"
        )
        for operation in (
            lambda: session.set_speed(2.0),
            lambda: session.resume(),
        ):
            with pytest.raises(ClientError) as excinfo:
                operation()
            assert excinfo.value.code == "invalid_parameter"
    finally:
        if session is not None:
            session.close()
        client.close()
        server.stop()


def test_step_rejects_non_positive_credits_locally(tmp_path: Path) -> None:
    store = ParquetBarStore(tmp_path / "data")
    _populate(store, range(1))
    server = ReplayServer(ReplayService(store))
    server.start()
    client = _client(server)
    session = None
    try:
        session = client.replay.create_session(
            _subscriptions(), START, END, replay_mode="step"
        )
        with pytest.raises(ValueError):
            session.step(0)
        session.stop()
    finally:
        if session is not None:
            session.close()
        client.close()
        server.stop()


def test_client_close_stops_the_server_sessions(tmp_path: Path) -> None:
    """``close()`` must not leave step sessions running on the server.

    A ``step`` session never advances on its own, so it is still ``running``
    when the client is closed. The close path marks the client closed first
    (blocking new sessions) but must still send ``stop`` on the wire.
    """
    store = ParquetBarStore(tmp_path / "data")
    _populate(store, range(3))
    service = ReplayService(store)
    server = ReplayServer(service)
    server.start()
    client = _client(server)
    session = None
    try:
        replay = client.replay
        session = replay.create_session(
            _subscriptions(), START, END, replay_mode="step"
        )
        session_id = session.session_id
        assert service.session_info(session_id)["status"] == "running"
        assert session_id in service.active_session_ids()

        client.close()

        assert _wait_until(
            lambda: service.session_info(session_id)["status"] != "running"
        ), "the server session was still running after client.close()"
        assert service.session_info(session_id)["status"] == "stopped"
        assert service.active_session_ids() == []
        assert session.ended is True
    finally:
        if session is not None:
            session.close()
        client.close()
        server.stop()
