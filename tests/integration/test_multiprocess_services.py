"""Multi-process Service Role integration tests.

Every test spawns real `python -m mdgw` processes with ephemeral loopback
ports, never reaches the network, and cleans up in `finally`. Readiness is
always a bounded poll rather than a fixed sleep.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
import zmq

from mdgw.bus.zmq_bus import READY_TOPIC
from mdgw.core.bar import Bar
from mdgw.reference import export_instrument_snapshot, load_instrument_reference
from mdgw.store.parquet_dataset import ParquetBarStore
from mdgw_client.replay import ReplayClient

REPO = Path(__file__).resolve().parents[2]
SRC = REPO / "src"
UTC = timezone.utc
BTC = "binance:BTC/USDT"

INSTRUMENT_YAML = """\
- instrument_id: "binance:BTC/USDT"
  venue: binance
  symbol: "BTC/USDT"
  asset_class: crypto
  instrument_type: spot
  base: BTC
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.00001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
- instrument_id: "binance:ETH/USDT"
  venue: binance
  symbol: "ETH/USDT"
  asset_class: crypto
  instrument_type: spot
  base: ETH
  quote: USDT
  tick_size: "0.01"
  lot_size: "0.0001"
  multiplier: "1"
  timezone: UTC
  listing_ts: "2017-08-17T04:00:00Z"
"""


# -- fixtures / helpers ------------------------------------------------------


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _write_instruments(directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "binance.yaml").write_text(INSTRUMENT_YAML, encoding="utf-8")
    return directory


def _write_config(
    path: Path,
    *,
    root: Path,
    instruments: Path,
    http_port: int,
    collector_port: int,
    api_pub_port: int,
    collector_health_port: int,
    replay_control_port: int = 0,
    replay_publish_port: int = 0,
    replay_health_port: int = 0,
    fake_bars: int = 0,
    startup_delay_s: float = 0.5,
    collector_required: bool = False,
) -> Path:
    feed = ""
    if fake_bars:
        feed = f"""
[[feeds]]
name = "fake_spot"
kind = "fake"
venue = "binance"
symbols = ["BTC/USDT"]
data_types = ["bar"]
fake_bars = {fake_bars}
fake_price = "100"
"""
    path.write_text(
        f"""
[server]
host = "127.0.0.1"
http_port = {http_port}

[bus]
transport = "inproc"
zmq_hwm = 10000
collector_pub_bind = "tcp://127.0.0.1:{collector_port}"
collector_pub_connect = "tcp://127.0.0.1:{collector_port}"
api_pub_bind = "tcp://127.0.0.1:{api_pub_port}"

[service]
health_host = "127.0.0.1"
collector_health_port = {collector_health_port}
replay_health_port = {replay_health_port}
log_level = "INFO"
startup_delay_s = {startup_delay_s}
collector_required = {"true" if collector_required else "false"}
collector_ready_timeout_s = 20.0

[replay]
control_bind_host = "127.0.0.1"
control_port = {replay_control_port}
publish_bind_host = "127.0.0.1"
publish_port = {replay_publish_port}

[storage]
root = "{root.as_posix()}"

[collector]
backfill_hours = 24
stale_after_s = 120
state_db_name = "state.db"

[aggregation]
base_interval = "1m"
intervals = ["1m", "5m", "15m", "1h", "1d"]
retention_parent_periods = 2

[instruments]
path = "{instruments.as_posix()}"
{feed}
""",
        encoding="utf-8",
    )
    return path


def _seed_bars(
    root: Path,
    *,
    count: int = 1,
    base: datetime = datetime(2026, 10, 30, tzinfo=UTC),
) -> None:
    store = ParquetBarStore(root)
    bars = []
    for minute in range(count):
        open_ts = base + timedelta(minutes=minute)
        close_ts = open_ts + timedelta(minutes=1)
        bars.append(
            Bar(
                schema_version=1,
                data_type="bar",
                instrument_id=BTC,
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
        )
    store.write_bars(bars, asset_class="crypto")


class _ServiceProcess:
    def __init__(
        self,
        args: list[str],
        log_path: Path,
        *,
        creationflags: int = 0,
    ) -> None:
        env = os.environ.copy()
        previous = env.get("PYTHONPATH")
        env["PYTHONPATH"] = (
            str(SRC) if not previous else str(SRC) + os.pathsep + previous
        )
        env["PYTHONUNBUFFERED"] = "1"
        self._log_path = log_path
        self._log = log_path.open("w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "mdgw", *args],
            cwd=str(REPO),
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
            text=True,
            creationflags=creationflags,
        )

    def stop(self, timeout: float = 25.0) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=timeout)
        self._log.close()

    def logs(self) -> str:
        try:
            self._log.flush()
        except ValueError:  # already closed by stop()
            pass
        return self._log_path.read_text(encoding="utf-8", errors="replace")


def _get(url: str, timeout: float = 20.0) -> bytes:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                return response.read()
        except (urllib.error.URLError, OSError) as exc:
            last = exc
            time.sleep(0.1)
    raise AssertionError(f"no response from {url}: {last}")


def _get_status_and_body(url: str, timeout: float = 20.0) -> tuple[int, bytes]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()
        except (urllib.error.URLError, OSError):
            time.sleep(0.1)
    # 0 means "not reachable yet"; callers that need a hard failure assert on
    # the body or status.
    return 0, b""


def _get_json(url: str, timeout: float = 20.0) -> dict:
    return json.loads(_get(url, timeout).decode("utf-8"))


def _wait_ready(url: str, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, body = _get_status_and_body(url, timeout=2)
        if status == 200:
            return json.loads(body.decode("utf-8"))
        time.sleep(0.1)
    raise AssertionError(f"{url} never became ready")


def _wait_bars(base_url: str, minimum: int, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            payload = _get_json(
                f"{base_url}/api/v1/bars?interval=1m&format=json", timeout=2
            )
        except AssertionError:
            time.sleep(0.1)
            continue
        if len(payload.get("bars", [])) >= minimum:
            return payload
        time.sleep(0.1)
    raise AssertionError(f"fewer than {minimum} Bars became visible")


def _wait_zmq_ready(subscriber: zmq.Socket, timeout: float = 10.0) -> None:
    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if dict(poller.poll(100)).get(subscriber) != zmq.POLLIN:
            continue
        frames = subscriber.recv_multipart()
        if frames and frames[0].decode("utf-8") == READY_TOPIC:
            return
    raise AssertionError("external ZMQ SUB never received the ready handshake")


def _collect_market_messages(
    subscriber: zmq.Socket, *, minimum: int = 1, timeout: float = 20.0
) -> list[tuple[str, bytes]]:
    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)
    messages: list[tuple[str, bytes]] = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if dict(poller.poll(100)).get(subscriber) != zmq.POLLIN:
            continue
        frames = subscriber.recv_multipart()
        topic = frames[0].decode("utf-8")
        if topic == READY_TOPIC:
            continue
        if topic.startswith("market.bar."):
            messages.append((topic, frames[1]))
            if len(messages) >= minimum:
                return messages
    raise AssertionError(f"received {messages!r} before the market timeout")


def _url_reachable(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=1):
            return True
    except (urllib.error.URLError, OSError):
        return False


# -- test 1: api with a pre-seeded Dataset ----------------------------------


def test_api_service_role_serves_preseeded_dataset(tmp_path: Path) -> None:
    root = tmp_path / "data"
    instruments = _write_instruments(tmp_path / "instruments")
    _seed_bars(root)
    reference = load_instrument_reference(instruments)
    export_instrument_snapshot(reference, root / "instruments")

    http_port = _free_port()
    config = _write_config(
        tmp_path / "gateway.toml",
        root=root,
        instruments=instruments,
        http_port=http_port,
        collector_port=_free_port(),
        api_pub_port=_free_port(),
        collector_health_port=_free_port(),
    )
    service = _ServiceProcess(
        ["run", "--service", "api", "--config", str(config), "--root", str(root)],
        tmp_path / "api.log",
    )
    base_url = f"http://127.0.0.1:{http_port}"
    try:
        health = _wait_ready(f"{base_url}/healthz")
        assert health["status"] == "ok"
        assert health["service"] == "api"

        ready = _wait_ready(f"{base_url}/readyz")
        assert ready["ready"] is True
        # No collector is running; the api still serves historical data.
        assert ready["collector_ready"] is False

        status = _get_json(f"{base_url}/status")
        assert status["dataset"] == "bars"
        assert status["service"] == "api"
        assert status["collector"]["ready"] is False

        with urllib.request.urlopen(f"{base_url}/metrics", timeout=5) as response:
            assert (
                response.headers["Content-Type"]
                == "text/plain; version=0.0.4"
            )
            text = response.read().decode("utf-8")
        assert "mdgw_up 1" in text
        assert "mdgw_ready 1" in text

        _, arrow = _get_status_and_body(
            f"{base_url}/api/v1/bars?interval=1m&format=arrow"
        )
        table = pa.ipc.open_stream(pa.BufferReader(arrow)).read_all()
        assert table.num_rows == 1
        assert table.column("instrument_id").to_pylist() == [BTC]
    finally:
        service.stop()


# -- test 2: replay Service Role --------------------------------------------


def test_replay_service_role_streams_a_session(tmp_path: Path) -> None:
    root = tmp_path / "data"
    instruments = _write_instruments(tmp_path / "instruments")
    _seed_bars(root, count=3)

    control_port = _free_port()
    publish_port = _free_port()
    health_port = _free_port()
    config = _write_config(
        tmp_path / "gateway.toml",
        root=root,
        instruments=instruments,
        http_port=_free_port(),
        collector_port=_free_port(),
        api_pub_port=_free_port(),
        collector_health_port=_free_port(),
        replay_control_port=control_port,
        replay_publish_port=publish_port,
        replay_health_port=health_port,
    )
    service = _ServiceProcess(
        [
            "run",
            "--service",
            "replay",
            "--config",
            str(config),
            "--root",
            str(root),
        ],
        tmp_path / "replay.log",
    )
    try:
        _wait_ready(f"http://127.0.0.1:{health_port}/readyz")
        _, metrics = _get_status_and_body(
            f"http://127.0.0.1:{health_port}/metrics"
        )
        assert "mdgw_replay_active_sessions 0" in metrics.decode("utf-8")

        with ReplayClient(
            publish_endpoint=f"tcp://127.0.0.1:{publish_port}",
            control_endpoint=f"tcp://127.0.0.1:{control_port}",
        ) as client:
            session = client.create_session(
                [{"data_type": "bar", "instrument_id": BTC, "interval": "1m"}],
                "2026-10-30T00:00:00Z",
                "2026-10-30T00:10:00Z",
                replay_mode="asap",
            )
            events = []
            try:
                while True:
                    events.append(session.next_event(timeout=10))
            except StopIteration:
                pass

            assert len(events) == 3
            assert all(event.topic.startswith("replay.") for event in events)
            assert all(event.table.num_rows == 1 for event in events)
            end = session.end_of_session
            assert end is not None
            assert end["reason"] == "exhausted"
            assert end["bars_sent"] == 3
    finally:
        service.stop()


# -- test 3: collector + api ------------------------------------------------


def test_collector_and_api_forward_fake_bars(tmp_path: Path) -> None:
    root = tmp_path / "data"
    instruments = _write_instruments(tmp_path / "instruments")
    reference = load_instrument_reference(instruments)
    export_instrument_snapshot(reference, root / "instruments")

    http_port = _free_port()
    collector_port = _free_port()
    api_pub_port = _free_port()
    collector_health_port = _free_port()
    config = _write_config(
        tmp_path / "gateway.toml",
        root=root,
        instruments=instruments,
        http_port=http_port,
        collector_port=collector_port,
        api_pub_port=api_pub_port,
        collector_health_port=collector_health_port,
        fake_bars=3,
        startup_delay_s=1.0,
    )
    api = _ServiceProcess(
        ["run", "--service", "api", "--config", str(config), "--root", str(root)],
        tmp_path / "api.log",
    )
    collector = None
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.RCVHWM, 10_000)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"market.bar.")
    subscriber.setsockopt(zmq.SUBSCRIBE, READY_TOPIC.encode("utf-8"))
    try:
        # The api starts first so its SUB is connected before the collector
        # binds, and the external SUB handshake proves forwarding is live.
        _wait_ready(f"http://127.0.0.1:{http_port}/readyz")
        subscriber.connect(f"tcp://127.0.0.1:{api_pub_port}")
        _wait_zmq_ready(subscriber)

        collector = _ServiceProcess(
            [
                "run",
                "--service",
                "collector",
                "--config",
                str(config),
                "--root",
                str(root),
            ],
            tmp_path / "collector.log",
        )
        _wait_ready(f"http://127.0.0.1:{collector_health_port}/readyz")

        payload = _wait_bars(f"http://127.0.0.1:{http_port}", minimum=3)
        assert len(payload["bars"]) >= 3

        messages = _collect_market_messages(subscriber, minimum=1, timeout=20)
        topic, frames = messages[0]
        assert topic == f"market.bar.1m.{BTC}"
        table = pa.ipc.open_stream(pa.BufferReader(frames)).read_all()
        assert table.num_rows >= 1
        assert set(table.column("instrument_id").to_pylist()) == {BTC}
    finally:
        if collector is not None:
            collector.stop()
        api.stop()
        subscriber.close()
        context.term()


# -- test 4: `mdgw up` smoke ------------------------------------------------


def test_up_supervisor_starts_and_stops_the_stack(tmp_path: Path) -> None:
    root = tmp_path / "data"
    instruments = _write_instruments(tmp_path / "instruments")

    http_port = _free_port()
    collector_health_port = _free_port()
    config = _write_config(
        tmp_path / "gateway.toml",
        root=root,
        instruments=instruments,
        http_port=http_port,
        collector_port=_free_port(),
        api_pub_port=_free_port(),
        collector_health_port=collector_health_port,
        fake_bars=2,
        startup_delay_s=0.2,
    )

    creationflags = (
        subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    )
    up = _ServiceProcess(
        ["up", "--config", str(config), "--root", str(root)],
        tmp_path / "up.log",
        creationflags=creationflags,
    )
    api_ready = f"http://127.0.0.1:{http_port}/readyz"
    collector_ready = f"http://127.0.0.1:{collector_health_port}/readyz"
    try:
        _wait_ready(api_ready, timeout=40)
        _wait_ready(collector_ready, timeout=20)

        # Graceful stop: SIGTERM on POSIX, CTRL_BREAK on Windows, so the
        # supervisor can terminate both children before exiting.
        if os.name == "nt":
            try:
                up.proc.send_signal(signal.CTRL_BREAK_EVENT)
            except Exception:  # pragma: no cover - platform quirk
                up.proc.terminate()
        else:
            up.proc.terminate()
        try:
            up.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            up.proc.kill()
            up.proc.wait(timeout=10)

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if not _url_reachable(api_ready) and not _url_reachable(
                collector_ready
            ):
                break
            time.sleep(0.2)
        else:
            raise AssertionError("child Service Roles survived the supervisor")

        logs = up.logs()
        assert logs.count('"event": "supervisor.child_started"') >= 2
        assert '"service": "collector"' in logs
        assert '"service": "api"' in logs
    finally:
        up.stop()
