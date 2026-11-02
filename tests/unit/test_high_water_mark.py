from __future__ import annotations

from pathlib import Path

import pytest

from mdgw.state.high_water_mark import HighWaterMarkStore

INSTRUMENT = "binance:BTC/USDT"


def test_hwm_persists_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    store = HighWaterMarkStore(path)
    try:
        store.set(INSTRUMENT, "1m", 1_700_000_060_000_000_000)
        store.set(INSTRUMENT, "5m", 1_700_000_000_000_000_000)
        assert store.get(INSTRUMENT, "1m") == 1_700_000_060_000_000_000
        assert store.get(INSTRUMENT, "5m") == 1_700_000_000_000_000_000
        assert store.all() == {
            (INSTRUMENT, "1m"): 1_700_000_060_000_000_000,
            (INSTRUMENT, "5m"): 1_700_000_000_000_000_000,
        }
    finally:
        store.close()

    reopened = HighWaterMarkStore(path)
    try:
        assert reopened.get(INSTRUMENT, "1m") == 1_700_000_060_000_000_000
        assert reopened.all() == {
            (INSTRUMENT, "1m"): 1_700_000_060_000_000_000,
            (INSTRUMENT, "5m"): 1_700_000_000_000_000_000,
        }
    finally:
        reopened.close()


def test_missing_hwm_returns_none(tmp_path: Path) -> None:
    store = HighWaterMarkStore(tmp_path / "state.db")
    try:
        assert store.get(INSTRUMENT, "1m") is None
        assert store.all() == {}
    finally:
        store.close()


def test_hwm_does_not_regress(tmp_path: Path) -> None:
    store = HighWaterMarkStore(tmp_path / "state.db")
    try:
        store.set(INSTRUMENT, "1m", 200)
        store.set(INSTRUMENT, "1m", 100)
        assert store.get(INSTRUMENT, "1m") == 200
    finally:
        store.close()


def test_failed_write_does_not_advance_hwm(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    store = HighWaterMarkStore(path)
    store.set(INSTRUMENT, "1m", 100)
    store.close()

    with pytest.raises(Exception):
        store.set(INSTRUMENT, "1m", 200)

    reopened = HighWaterMarkStore(path)
    try:
        assert reopened.get(INSTRUMENT, "1m") == 100
    finally:
        reopened.close()
