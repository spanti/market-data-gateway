"""Replay Protocol request validation and stable error codes."""

from __future__ import annotations

import pytest

from mdgw.core.errors import (
    INVALID_INSTRUMENT,
    INVALID_INTERVAL,
    INVALID_PARAMETER,
    INVALID_RANGE,
    GatewayError,
)
from mdgw.replay.session import BarSubscription, ReplaySessionSpec


def _request(**overrides):
    payload = {
        "op": "create_session",
        "subscriptions": [
            {
                "data_type": "bar",
                "instrument_id": "binance:BTC/USDT",
                "interval": "1m",
            }
        ],
        "start_ts": "2026-10-30T00:00:00Z",
        "end_ts": "2026-10-31T00:00:00Z",
        "replay_mode": "asap",
    }
    payload.update(overrides)
    return payload


def _error_code(payload) -> str:
    with pytest.raises(GatewayError) as excinfo:
        ReplaySessionSpec.from_mapping(payload)
    return excinfo.value.code


def test_parses_a_valid_asap_request() -> None:
    spec = ReplaySessionSpec.from_mapping(_request())

    assert spec.replay_mode == "asap"
    assert spec.subscriptions == (
        BarSubscription(
            instrument_id="binance:BTC/USDT",
            interval="1m",
            data_type="bar",
        ),
    )
    assert spec.start_ts_ns < spec.end_ts_ns


def test_data_type_defaults_to_bar() -> None:
    payload = _request(
        subscriptions=[{"instrument_id": "binance:BTC/USDT", "interval": "1m"}]
    )

    spec = ReplaySessionSpec.from_mapping(payload)

    assert spec.subscriptions[0].data_type == "bar"


@pytest.mark.parametrize(
    "overrides",
    [
        {"subscriptions": []},
        {"subscriptions": "not-a-list"},
        {"start_ts": None},
        {"end_ts": None},
        {"start_ts": "not-a-timestamp"},
        {"replay_mode": "turbo"},
        {"replay_mode": 1},
        {"speed": 0},
        {"speed": -1.0},
        {"speed": float("inf")},
        {"speed": float("nan")},
        {"speed": True},
        {"speed": "2"},
        {"speed": None},
        {"subscriptions": [{"instrument_id": "binance:BTC/USDT"}]},
        {
            "subscriptions": [
                {
                    "data_type": "trade",
                    "instrument_id": "binance:BTC/USDT",
                    "interval": "1m",
                }
            ]
        },
    ],
)
def test_invalid_parameter_requests(overrides: dict) -> None:
    assert _error_code(_request(**overrides)) == INVALID_PARAMETER


def test_inverted_range_is_invalid_range() -> None:
    assert (
        _error_code(
            _request(
                start_ts="2026-10-31T00:00:00Z",
                end_ts="2026-10-30T00:00:00Z",
            )
        )
        == INVALID_RANGE
    )


def test_equal_range_is_invalid_range() -> None:
    assert (
        _error_code(
            _request(
                start_ts="2026-10-30T00:00:00Z",
                end_ts="2026-10-30T00:00:00Z",
            )
        )
        == INVALID_RANGE
    )


def test_unknown_interval_is_invalid_interval() -> None:
    payload = _request(
        subscriptions=[
            {"data_type": "bar", "instrument_id": "binance:BTC/USDT", "interval": "2m"}
        ]
    )

    assert _error_code(payload) == INVALID_INTERVAL


def test_unparseable_instrument_is_invalid_instrument() -> None:
    payload = _request(
        subscriptions=[
            {"data_type": "bar", "instrument_id": "BTCUSDT", "interval": "1m"}
        ]
    )

    assert _error_code(payload) == INVALID_INSTRUMENT


def test_spec_rejects_unknown_mode_directly() -> None:
    with pytest.raises(GatewayError) as excinfo:
        ReplaySessionSpec(
            subscriptions=(BarSubscription("binance:BTC/USDT", "1m"),),
            start_ts_ns=1,
            end_ts_ns=2,
            replay_mode="turbo",
        )

    assert excinfo.value.code == INVALID_PARAMETER


@pytest.mark.parametrize("mode", ["asap", "realtime", "step"])
def test_all_supported_modes_are_accepted(mode: str) -> None:
    spec = ReplaySessionSpec.from_mapping(_request(replay_mode=mode))

    assert spec.replay_mode == mode


def test_speed_defaults_to_one() -> None:
    assert ReplaySessionSpec.from_mapping(_request()).speed == 1.0


@pytest.mark.parametrize(("raw", "expected"), [(2, 2.0), (0.5, 0.5), (10.0, 10.0)])
def test_speed_is_parsed_as_a_float(raw: object, expected: float) -> None:
    spec = ReplaySessionSpec.from_mapping(_request(speed=raw))

    assert spec.speed == expected


def test_unknown_interval_is_checked_before_instrument() -> None:
    # Both fields are bad; the interval code wins because it is validated
    # before the InstrumentId parse in BarSubscription.
    payload = _request(
        subscriptions=[
            {"data_type": "bar", "instrument_id": "bad", "interval": "2m"}
        ]
    )

    assert _error_code(payload) == INVALID_INTERVAL
