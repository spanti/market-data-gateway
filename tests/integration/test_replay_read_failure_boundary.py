"""Public Replay read-failure termination protocol.

A read failure is an explicit `reason="error"` End of Session, never an empty or
short `exhausted` replay. The event carries a stable sanitized error object and
the exact number of Bars published before the failure; `exhausted` and
`stopped` keep their existing shape (no `error` key). These tests use real
Parquet/Manifest/DuckDB failures wherever the failure mode is real, and scripted
`StoreRead` proxies for partial-publication and seek-generation races.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

from mdgw.core.bar import Bar
from mdgw.core.errors import InvalidParameterError, SchemaVersionMismatch
from mdgw.core.schema import bars_to_logical_table
from mdgw.core.time import datetime_to_ns
from mdgw.replay.service import ReplayService
from mdgw.replay.session import BarSubscription, ReplaySession, ReplaySessionSpec
from mdgw.store.duckdb_reader import DuckDBOptions
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw.store.session_locks import active_session_locks

if TYPE_CHECKING:
    from pathlib import Path

BASE = datetime(2024, 1, 1, tzinfo=UTC)
INSTRUMENT = "binance:BTC/USDT"


def _bar(minute_offset: int) -> Bar:
    open_ts = BASE + timedelta(minutes=minute_offset)
    close_ts = open_ts + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id=INSTRUMENT,
        interval="1m",
        open_ts=open_ts,
        close_ts=close_ts,
        event_ts=close_ts,
        ingest_ts=close_ts,
        feed="fake",
        seq=minute_offset,
        aggregation_source="external",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1"),
        trade_count=1,
        revision=0,
    )


def _spec(
    *, mode: str = "asap", speed: float = 1.0, end_minute: int = 10
) -> ReplaySessionSpec:
    return ReplaySessionSpec(
        subscriptions=(BarSubscription(INSTRUMENT, "1m"),),
        start_ts_ns=datetime_to_ns(BASE),
        end_ts_ns=datetime_to_ns(BASE + timedelta(minutes=end_minute)),
        replay_mode=mode,
        speed=speed,
    )


def _wait_until(predicate, *, timeout: float = 5.0, description: str = "state"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError(f"timed out waiting for {description}")


def _run_session(
    session: ReplaySession, *, mode: str = "asap", credits: int = 0
) -> None:
    session.start()
    try:
        if mode == "step" and credits:
            session.step(credits)
        assert session.wait(timeout=15), "Session did not finish"
    finally:
        session.stop()
        assert session.wait(timeout=15)


class _Recorder:
    def __init__(self) -> None:
        self._cond = threading.Condition()
        self.bars: list[Bar] = []
        self.events: list[dict[str, Any]] = []

    def publish_bar(self, bar: Bar) -> None:
        with self._cond:
            self.bars.append(bar)
            self._cond.notify_all()

    def publish_event(self, event: dict[str, Any]) -> None:
        with self._cond:
            self.events.append(dict(event))
            self._cond.notify_all()


def _corrupt_the_dataset(store: ParquetBarStore) -> None:
    files = sorted(store.dataset_root.rglob("*.parquet"))
    assert files
    for path in files:
        path.write_bytes(b"this is not a parquet file")


def _batch(bars: list[Bar]):
    return bars_to_logical_table(list(bars)).to_batches()[0]


class _Generation:
    """One scripted query generation returned by `_ScriptedRead`."""

    def __init__(
        self,
        batches: list[list[Bar]],
        *,
        ready_gate: threading.Event | None = None,
        between_gate: threading.Event | None = None,
        fail_gate: threading.Event | None = None,
        raise_after: BaseException | None = None,
    ) -> None:
        self.batches = [list(batch) for batch in batches]
        self.ready_gate = ready_gate
        self.between_gate = between_gate
        self.fail_gate = fail_gate
        self.raise_after = raise_after
        self.started = threading.Event()
        self.finished = threading.Event()
        self.cancelled = threading.Event()
        self.raised = threading.Event()
        self.yielded = 0


def _wait_or_cancelled(gate: threading.Event, cancellation: Any) -> bool:
    deadline = time.monotonic() + 10.0
    while not gate.wait(0.005):
        if cancellation is not None and cancellation.cancelled:
            return False
        if time.monotonic() > deadline:
            raise AssertionError("test never released the read gate")
    return True


class _ScriptedRead:
    """A StoreRead proxy serving one scripted generation per query call.

    Producer failures are raised after the already-yielded batches, so a test
    can prove that published Bars are never rolled back or re-sent. A
    generation honours the per-generation cancellation handle used by seek.
    """

    def __init__(self, generations: list[_Generation]) -> None:
        self._generations = list(generations)
        self._lock = threading.Lock()
        self._index = 0
        self.close_calls = 0

    def open_read(self, **_kwargs: Any) -> _ScriptedRead:
        return self

    def _next_generation(self) -> _Generation:
        with self._lock:
            generation = self._generations[self._index]
            self._index += 1
            return generation

    def iter_replay_batches(self, queries, *, batch_size=None, cancellation=None):
        generation = self._next_generation()
        generation.started.set()
        try:
            for position, bars in enumerate(generation.batches):
                gate = (
                    generation.ready_gate
                    if position == 0
                    else generation.between_gate
                )
                if gate is not None and not _wait_or_cancelled(gate, cancellation):
                    generation.cancelled.set()
                    return
                if cancellation is not None and cancellation.cancelled:
                    generation.cancelled.set()
                    return
                yield _batch(bars)
                generation.yielded += 1
            if generation.fail_gate is not None:
                generation.fail_gate.wait(10)
            if generation.raise_after is not None:
                generation.raised.set()
                raise generation.raise_after
        finally:
            generation.finished.set()

    def close(self) -> None:
        self.close_calls += 1


def test_corrupt_dataset_is_an_explicit_error_not_an_empty_exhausted_replay(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0), _bar(1)], asset_class="crypto")
    _corrupt_the_dataset(store)
    recorder = _Recorder()

    session = ReplaySession(
        "rpl-corrupt",
        _spec(),
        store,
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    _run_session(session)

    assert recorder.events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-corrupt",
            "reason": "error",
            "bars_sent": 0,
            "error": {
                "code": "internal_error",
                "message": "replay read failed",
            },
        }
    ]
    assert recorder.bars == []
    assert session.bars_sent == 0
    assert isinstance(session.failure, Exception)
    # The public event must not leak engine details, SQL or Data File paths.
    public = json.dumps(recorder.events[0]).lower()
    for forbidden in (
        "duckdb",
        "parquet",
        ".parquet",
        "select ",
        "read_parquet",
        "sql",
        str(store.dataset_root).lower(),
    ):
        assert forbidden not in public, forbidden
    # The failed Session is terminal, its lease is released and stop stays an
    # idempotent success that cannot emit a second End of Session.
    assert session.status == "finished"
    assert active_session_locks(store.dataset_root) == []
    assert session.stop() is True
    assert len(recorder.events) == 1


def test_schema_version_mismatch_uses_the_existing_stable_code(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")
    descriptor_path = store.dataset_root / "_dataset.json"
    descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    descriptor["schema_version"] = 2
    descriptor_path.write_text(json.dumps(descriptor), encoding="utf-8")
    recorder = _Recorder()

    session = ReplaySession(
        "rpl-schema",
        _spec(),
        store,
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    _run_session(session)

    assert recorder.bars == []
    assert recorder.events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-schema",
            "reason": "error",
            "bars_sent": 0,
            "error": {
                "code": "schema_version_mismatch",
                "message": "Dataset schema version is not supported",
            },
        }
    ]
    assert isinstance(session.failure, SchemaVersionMismatch)
    assert active_session_locks(store.dataset_root) == []


READ_FAILURE = OSError(
    "Cannot open file '/secret/data.parquet': No such file or directory"
)


@pytest.mark.parametrize("mode", ["asap", "realtime", "step"])
def test_initial_read_failure_is_reason_error_in_every_mode(mode: str) -> None:
    read = _ScriptedRead([_Generation([], raise_after=READ_FAILURE)])
    recorder = _Recorder()
    session = ReplaySession(
        f"rpl-initial-{mode}",
        _spec(mode=mode, speed=1e9),
        read,  # type: ignore[arg-type] - proxy implements the read seam
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    _run_session(session, mode=mode)

    assert recorder.bars == []
    assert recorder.events == [
        {
            "op": "end_of_session",
            "session_id": f"rpl-initial-{mode}",
            "reason": "error",
            "bars_sent": 0,
            "error": {
                "code": "internal_error",
                "message": "replay read failed",
            },
        }
    ]
    assert isinstance(session.failure, OSError)
    assert session.bars_sent == 0
    assert session.status == "finished"
    assert read.close_calls >= 1
    # Sanitized: the Data File path from the raw exception is not public.
    assert "/secret/data.parquet" not in json.dumps(recorder.events)


@pytest.mark.parametrize("mode", ["asap", "realtime", "step"])
def test_failure_after_partial_publication_keeps_the_exact_bars_sent(
    mode: str,
) -> None:
    published = [_bar(0), _bar(1)]
    read = _ScriptedRead(
        [_Generation([published], raise_after=READ_FAILURE)]
    )
    recorder = _Recorder()
    session = ReplaySession(
        f"rpl-partial-{mode}",
        _spec(mode=mode, speed=1e9),
        read,  # type: ignore[arg-type] - proxy implements the read seam
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    _run_session(session, mode=mode, credits=10 if mode == "step" else 0)

    # Bars published before the failure are not rolled back or re-sent.
    assert [bar.open_ts for bar in recorder.bars] == [
        BASE,
        BASE + timedelta(minutes=1),
    ]
    assert recorder.events == [
        {
            "op": "end_of_session",
            "session_id": f"rpl-partial-{mode}",
            "reason": "error",
            "bars_sent": 2,
            "error": {
                "code": "internal_error",
                "message": "replay read failed",
            },
        }
    ]
    assert session.bars_sent == 2 == len(recorder.bars)
    assert isinstance(session.failure, OSError)
    assert session.status == "finished"


def test_realtime_failure_in_a_seek_generation_ends_with_one_error_eos() -> None:
    between = threading.Event()
    old_generation = _Generation(
        [[_bar(0)], [_bar(1)]], between_gate=between
    )
    new_generation = _Generation([], raise_after=READ_FAILURE)
    read = _ScriptedRead([old_generation, new_generation])
    recorder = _Recorder()
    session = ReplaySession(
        "rpl-seek-failure-rt",
        _spec(mode="realtime", speed=1e9),
        read,  # type: ignore[arg-type] - proxy implements the read seam
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    session.start()
    try:
        _wait_until(
            lambda: len(recorder.bars) >= 1, description="the first Bar"
        )
        assert session.pause() is True
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=2.5)))
        assert session.wait(timeout=15)
    finally:
        between.set()
        session.stop()
        assert session.wait(timeout=15)

    # The old generation's second batch must never publish, and the failing
    # seek generation must end the Session exactly once.
    assert [bar.open_ts for bar in recorder.bars] == [BASE]
    assert recorder.events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-seek-failure-rt",
            "reason": "error",
            "bars_sent": 1,
            "error": {
                "code": "internal_error",
                "message": "replay read failed",
            },
        }
    ]
    assert isinstance(session.failure, OSError)
    assert session.bars_sent == 1
    assert old_generation.finished.is_set()
    assert new_generation.raised.is_set()


def test_step_failure_in_a_seek_generation_ends_with_one_error_eos() -> None:
    between = threading.Event()
    old_generation = _Generation(
        [[_bar(0)], [_bar(1)]], between_gate=between
    )
    new_generation = _Generation([], raise_after=READ_FAILURE)
    read = _ScriptedRead([old_generation, new_generation])
    recorder = _Recorder()
    session = ReplaySession(
        "rpl-seek-failure-step",
        _spec(mode="step"),
        read,  # type: ignore[arg-type] - proxy implements the read seam
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    session.start()
    try:
        session.step(1)
        _wait_until(
            lambda: len(recorder.bars) >= 1, description="the first Bar"
        )
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=2.5)))
        assert session.wait(timeout=15)
    finally:
        between.set()
        session.stop()
        assert session.wait(timeout=15)

    assert [bar.open_ts for bar in recorder.bars] == [BASE]
    assert recorder.events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-seek-failure-step",
            "reason": "error",
            "bars_sent": 1,
            "error": {
                "code": "internal_error",
                "message": "replay read failed",
            },
        }
    ]
    assert isinstance(session.failure, OSError)
    assert session.bars_sent == 1
    assert old_generation.finished.is_set()
    assert new_generation.raised.is_set()


def test_a_superseded_generation_failure_is_discarded_after_a_seek() -> None:
    fail_gate = threading.Event()
    old_generation = _Generation(
        [[_bar(0)]], fail_gate=fail_gate, raise_after=OSError("old generation")
    )
    new_generation = _Generation([[_bar(3)]])
    read = _ScriptedRead([old_generation, new_generation])
    recorder = _Recorder()
    session = ReplaySession(
        "rpl-superseded-failure",
        _spec(mode="realtime", speed=1e9),
        read,  # type: ignore[arg-type] - proxy implements the read seam
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    session.start()
    try:
        _wait_until(
            lambda: len(recorder.bars) >= 1, description="the first Bar"
        )
        assert session.pause() is True
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=2.5)))
        # The replaced generation now fails; its failure belongs to a dead
        # generation and must never become the Session's termination.
        fail_gate.set()
        assert session.resume() is True
        assert session.wait(timeout=15)
    finally:
        fail_gate.set()
        session.stop()
        assert session.wait(timeout=15)

    assert old_generation.raised.is_set()
    assert [bar.open_ts for bar in recorder.bars] == [
        BASE,
        BASE + timedelta(minutes=3),
    ]
    assert recorder.events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-superseded-failure",
            "reason": "exhausted",
            "bars_sent": 2,
        }
    ]
    assert session.failure is None


def test_resource_exhaustion_is_an_internal_error_not_an_empty_replay(
    tmp_path: Path,
) -> None:
    temp_directory = tmp_path / "duckdb-temp"
    temp_directory.mkdir()
    store = ParquetBarStore(
        tmp_path / "data",
        duckdb=DuckDBOptions(
            memory_limit="2MB",
            threads=1,
            temp_directory=temp_directory,
            max_temp_directory_size="1MB",
        ),
    )
    # A global Replay sort cannot fit the tiny budget and raises DuckDB's real
    # OutOfMemory condition; it must never become a truncated finite replay.
    store.write_bars(
        [_bar(minute) for minute in range(20_000)], asset_class="crypto"
    )
    recorder = _Recorder()
    session = ReplaySession(
        "rpl-oom",
        _spec(end_minute=20_002),
        store,
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    _run_session(session)

    assert recorder.bars == []
    assert recorder.events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-oom",
            "reason": "error",
            "bars_sent": 0,
            "error": {
                "code": "internal_error",
                "message": "replay read failed",
            },
        }
    ]
    assert session.bars_sent == 0
    assert session.failure is not None
    assert "out of memory" in str(session.failure).lower()
    # The public event never exposes the engine's resource message.
    public = json.dumps(recorder.events[0]).lower()
    assert "out of memory" not in public
    assert "duckdb" not in public
    assert session.status == "finished"
    assert active_session_locks(store.dataset_root) == []


def test_stop_racing_an_initial_read_failure_reports_stopped() -> None:
    fail_gate = threading.Event()
    generation = _Generation([], fail_gate=fail_gate, raise_after=READ_FAILURE)
    read = _ScriptedRead([generation])
    recorder = _Recorder()
    session = ReplaySession(
        "rpl-stop-race",
        _spec(),
        read,  # type: ignore[arg-type] - proxy implements the read seam
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    session.start()
    try:
        assert generation.started.wait(5)
        assert session.stop() is True
        fail_gate.set()
        assert session.wait(timeout=15)
    finally:
        fail_gate.set()
        session.stop()
        assert session.wait(timeout=15)

    # A stop that races the read failure wins: the public termination is
    # `stopped`, not an error, and the read exception stays private.
    assert recorder.events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-stop-race",
            "reason": "stopped",
            "bars_sent": 0,
        }
    ]
    assert "error" not in recorder.events[0]
    assert session.failure is None
    assert session.status == "stopped"


def test_stop_racing_a_seek_generation_failure_reports_stopped() -> None:
    between = threading.Event()
    old_generation = _Generation(
        [[_bar(0)], [_bar(1)]], between_gate=between
    )
    fail_gate = threading.Event()
    new_generation = _Generation(
        [], fail_gate=fail_gate, raise_after=READ_FAILURE
    )
    read = _ScriptedRead([old_generation, new_generation])
    recorder = _Recorder()
    session = ReplaySession(
        "rpl-stop-seek-race",
        _spec(mode="realtime", speed=1e9),
        read,  # type: ignore[arg-type] - proxy implements the read seam
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    session.start()
    try:
        _wait_until(
            lambda: len(recorder.bars) >= 1, description="the first Bar"
        )
        assert session.pause() is True
        assert session.seek(datetime_to_ns(BASE + timedelta(minutes=2.5)))
        assert new_generation.started.wait(5)
        assert session.stop() is True
        fail_gate.set()
        assert session.wait(timeout=15)
    finally:
        between.set()
        fail_gate.set()
        session.stop()
        assert session.wait(timeout=15)

    assert [bar.open_ts for bar in recorder.bars] == [BASE]
    assert recorder.events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-stop-seek-race",
            "reason": "stopped",
            "bars_sent": 1,
        }
    ]
    assert "error" not in recorder.events[0]
    assert session.status == "stopped"


def test_controls_are_rejected_after_an_error_termination(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")
    _corrupt_the_dataset(store)
    recorder = _Recorder()
    session = ReplaySession(
        "rpl-error-controls",
        _spec(),
        store,
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    _run_session(session)

    for operation in (
        session.pause,
        session.resume,
        lambda: session.seek(datetime_to_ns(BASE)),
        lambda: session.set_speed(2.0),
        lambda: session.step(1),
    ):
        with pytest.raises(InvalidParameterError):
            operation()
    # `stop` remains an idempotent terminal success and never emits twice.
    assert session.stop() is True
    assert session.wait(timeout=5)
    assert len(recorder.events) == 1
    assert recorder.events[0]["reason"] == "error"


def test_empty_dataset_and_read_failure_are_distinguishable_publicly(
    tmp_path: Path,
) -> None:
    from fastapi.testclient import TestClient

    from mdgw.api.app import create_app

    def _replay_events(store: ParquetBarStore, session_id: str) -> list[dict]:
        recorder = _Recorder()
        service = ReplayService(
            store,
            publish_bar=lambda _session_id, bar: recorder.publish_bar(bar),
            publish_event=lambda _session_id, event: recorder.publish_event(
                event
            ),
            session_id_factory=lambda: session_id,
        )
        try:
            created = service.create_session(_spec())
            assert service.wait_session(created, timeout=15)
        finally:
            service.close()
        return recorder.events

    # An empty Dataset is a normal, complete replay: REST 200 and a replay
    # End of Session without an error object.
    empty = ParquetBarStore(tmp_path / "empty")
    response = TestClient(create_app(empty)).get("/api/v1/bars?interval=1m")
    assert response.status_code == 200
    assert response.json() == {"bars": []}
    empty_events = _replay_events(empty, "rpl-contract-empty")
    assert empty_events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-contract-empty",
            "reason": "exhausted",
            "bars_sent": 0,
        }
    ]

    # A read failure stays an explicit failure on both surfaces: the REST
    # envelope is unchanged, while Replay uses `reason=error`.
    broken = ParquetBarStore(tmp_path / "broken")
    broken.write_bars([_bar(0)], asset_class="crypto")
    _corrupt_the_dataset(broken)
    response = TestClient(
        create_app(broken), raise_server_exceptions=False
    ).get("/api/v1/bars?interval=1m")
    assert response.status_code == 500
    body = response.json()
    assert set(body) == {"error"}
    assert body["error"]["code"] == "internal_error"
    assert set(body["error"]) == {"code", "message", "details", "request_id"}
    rest_text = response.text.lower()
    for forbidden in (
        "duckdb",
        "parquet",
        "read_parquet",
        str(broken.dataset_root).lower(),
    ):
        assert forbidden not in rest_text, forbidden

    failure_events = _replay_events(broken, "rpl-contract-broken")
    assert failure_events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-contract-broken",
            "reason": "error",
            "bars_sent": 0,
            "error": {
                "code": "internal_error",
                "message": "replay read failed",
            },
        }
    ]
    assert empty_events[0]["reason"] != failure_events[0]["reason"]


def test_a_failed_session_releases_resources_and_the_store_is_reusable(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0), _bar(1)], asset_class="crypto")
    files = sorted(store.dataset_root.rglob("*.parquet"))
    assert files
    valid = {path: path.read_bytes() for path in files}
    for path in files:
        path.write_bytes(b"this is not a parquet file")

    failed = _Recorder()
    session = ReplaySession(
        "rpl-reusable-failure",
        _spec(),
        store,
        failed.publish_bar,
        failed.publish_event,
        heartbeat_s=0.05,
    )
    _run_session(session)
    assert failed.events[-1]["reason"] == "error"
    assert active_session_locks(store.dataset_root) == []

    # Restoring the same immutable Data File paths makes the same Store work
    # again: the failed Session released its Reader, connection and lease.
    for path, payload in valid.items():
        path.write_bytes(payload)
    recovered = _Recorder()
    second = ReplaySession(
        "rpl-recovered",
        _spec(),
        store,
        recovered.publish_bar,
        recovered.publish_event,
        heartbeat_s=0.05,
    )
    _run_session(second)

    assert [bar.open_ts for bar in recovered.bars] == [
        BASE,
        BASE + timedelta(minutes=1),
    ]
    assert recovered.events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-recovered",
            "reason": "exhausted",
            "bars_sent": 2,
        }
    ]
    assert second.failure is None
    assert active_session_locks(store.dataset_root) == []


def test_service_read_failure_publishes_the_error_eos_and_releases_lease(
    tmp_path: Path,
) -> None:
    store = ParquetBarStore(tmp_path)
    store.write_bars([_bar(0)], asset_class="crypto")
    _corrupt_the_dataset(store)
    events: list[dict[str, Any]] = []
    bars: list[Bar] = []
    service = ReplayService(
        store,
        publish_bar=lambda _session_id, bar: bars.append(bar),
        publish_event=lambda _session_id, event: events.append(dict(event)),
        session_id_factory=lambda: "rpl-corrupt-service",
        heartbeat_s=0.05,
    )
    try:
        session_id = service.create_session(_spec())
        assert service.wait_session(session_id, timeout=10)
        assert service.session_bars_sent(session_id) == 0
        assert service.session_status(session_id) == "finished"
        assert service.active_session_ids() == []
        # A failed Session is terminal; stop stays idempotent and emits nothing.
        assert service.stop_session(session_id) is True
    finally:
        service.close()

    assert bars == []
    assert events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-corrupt-service",
            "reason": "error",
            "bars_sent": 0,
            "error": {
                "code": "internal_error",
                "message": "replay read failed",
            },
        }
    ]
    assert active_session_locks(store.dataset_root) == []


class _DeleteFilesAfterSnapshot:
    """Proxy: hand out a Snapshot, then delete its Data Files."""

    def __init__(self, inner: ParquetBarStore) -> None:
        self._inner = inner
        self.acquired = threading.Event()
        self.proceed = threading.Event()

    @property
    def dataset_root(self) -> Path:
        return self._inner.dataset_root

    def open_read(self, **kwargs: Any):
        read = self._inner.open_read(**kwargs)
        self.acquired.set()
        assert self.proceed.wait(10), "test did not release the Replay read"
        for path in self._inner.dataset_root.rglob("*.parquet"):
            path.unlink()
        return read


def test_missing_data_file_after_snapshot_is_an_explicit_error(
    tmp_path: Path,
) -> None:
    inner = ParquetBarStore(tmp_path)
    inner.write_bars([_bar(0)], asset_class="crypto")
    store = _DeleteFilesAfterSnapshot(inner)
    recorder = _Recorder()

    session = ReplaySession(
        "rpl-missing-file",
        _spec(),
        store,  # type: ignore[arg-type] - proxy implements the read seam
        recorder.publish_bar,
        recorder.publish_event,
        heartbeat_s=0.05,
    )
    try:
        session.start()
        assert store.acquired.wait(10)
        store.proceed.set()
        assert session.wait(timeout=10)
    finally:
        store.proceed.set()
        session.stop()
        assert session.wait(timeout=10)

    # A Data File deleted after the fixed Snapshot is a read failure, never a
    # silently short replay.
    assert recorder.bars == []
    assert recorder.events == [
        {
            "op": "end_of_session",
            "session_id": "rpl-missing-file",
            "reason": "error",
            "bars_sent": 0,
            "error": {
                "code": "internal_error",
                "message": "replay read failed",
            },
        }
    ]
    assert isinstance(session.failure, FileNotFoundError)
    assert session.status == "finished"
    assert active_session_locks(inner.dataset_root) == []
