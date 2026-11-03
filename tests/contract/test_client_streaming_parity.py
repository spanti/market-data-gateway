"""Client/server parity for the Streaming Interface wire strings.

The SDK owns an independent copy of the READY topic and topic grammar; it must
never import the gateway package. This test locks the two copies together
without creating that dependency.
"""

from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mdgw.core.bar import Bar
from mdgw.streaming import READY_TOPIC, bar_topic, bar_topic_prefix
from mdgw_client.live import READY_TOPIC as CLIENT_READY_TOPIC
from mdgw_client.replay import REPLAY_PREFIX

CLIENT_ROOT = Path(__file__).resolve().parents[2] / "src" / "mdgw_client"
UTC = timezone.utc


def _bar() -> Bar:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    close = start + timedelta(minutes=1)
    return Bar(
        schema_version=1,
        data_type="bar",
        instrument_id="binance:BTC/USDT",
        interval="1m",
        open_ts=start,
        close_ts=close,
        event_ts=close,
        ingest_ts=close,
        feed="binance_spot",
        seq=123456,
        aggregation_source="external",
        open=Decimal("65000.1"),
        high=Decimal("65010.0"),
        low=Decimal("64990.0"),
        close=Decimal("65005.5"),
        volume=Decimal("12.34"),
        trade_count=128,
        revision=0,
    )


def test_ready_topic_matches_the_client_copy() -> None:
    assert CLIENT_READY_TOPIC == READY_TOPIC == "_internal.ready"


def test_bar_topic_matches_the_topic_the_client_documents() -> None:
    # ``mdgw_client.live`` documents ``market.bar.1m.binance:`` as an
    # instrument-group prefix and ``market.bar.`` as the all-Bars prefix.
    assert bar_topic(_bar()) == "market.bar.1m.binance:BTC/USDT"
    assert bar_topic_prefix(interval="1m") == "market.bar.1m."
    assert bar_topic_prefix() == "market.bar."
    assert bar_topic(_bar()).startswith(bar_topic_prefix(interval="1m"))
    assert bar_topic(_bar()).startswith(bar_topic_prefix())


def test_namespaced_bar_topics_match_the_client_replay_grammar() -> None:
    # ``mdgw_client.replay`` owns ``replay.<session_id>.`` and
    # ``replay.<session_id>.control``.
    assert REPLAY_PREFIX == "replay."
    namespace = f"{REPLAY_PREFIX}rpl-abc"
    assert (
        bar_topic(_bar(), namespace=namespace)
        == "replay.rpl-abc.market.bar.1m.binance:BTC/USDT"
    )
    assert (
        bar_topic_prefix(namespace=namespace)
        == "replay.rpl-abc.market.bar."
    )


def test_client_package_does_not_import_the_streaming_module() -> None:
    violations: list[str] = []
    for path in sorted(CLIENT_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.ImportFrom) and node.module:
                modules.append(node.module)
            elif isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            if any(
                module == "mdgw.streaming"
                or module.startswith("mdgw.streaming.")
                for module in modules
            ):
                violations.append(f"{path.relative_to(CLIENT_ROOT)}")
    assert violations == []
