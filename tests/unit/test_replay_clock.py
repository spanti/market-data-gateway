"""Replay Clock arithmetic with an injected monotonic time source.

No sleeps: every assertion advances the fake wall clock by an exact amount.
"""

from __future__ import annotations

import pytest

from mdgw.core.errors import InvalidParameterError
from mdgw.replay.clock import ReplayClock


class _Now:
    def __init__(self, start_ns: int = 1_000_000_000) -> None:
        self.value_ns = start_ns

    def __call__(self) -> int:
        return self.value_ns

    def advance(self, seconds: float) -> None:
        self.value_ns += int(seconds * 1_000_000_000)


def _clock(speed: float = 1.0) -> tuple[ReplayClock, _Now]:
    now = _Now()
    return ReplayClock(now_ns=now, speed=speed), now


def test_start_anchors_the_position_and_advances_with_wall_time() -> None:
    clock, now = _clock()

    clock.start(5_000_000_000)
    assert clock.position_ns() == 5_000_000_000

    now.advance(2)
    assert clock.position_ns() == 7_000_000_000


def test_speed_multiplies_the_elapsed_time() -> None:
    clock, now = _clock(speed=2.0)

    clock.start(0)
    now.advance(3)

    assert clock.position_ns() == 6_000_000_000


def test_pause_freezes_the_position_and_is_idempotent() -> None:
    clock, now = _clock()

    clock.start(0)
    now.advance(2)
    clock.pause()
    frozen = clock.position_ns()

    now.advance(100)
    clock.pause()
    assert clock.paused is True
    assert clock.position_ns() == frozen == 2_000_000_000


def test_resume_continues_from_the_frozen_position_and_is_idempotent() -> None:
    clock, now = _clock()

    clock.start(0)
    now.advance(2)
    clock.pause()
    now.advance(100)
    clock.resume()
    clock.resume()

    now.advance(3)
    assert clock.paused is False
    assert clock.position_ns() == 5_000_000_000


def test_pause_before_start_freezes_at_the_start_position() -> None:
    clock, now = _clock()

    clock.pause()
    clock.start(42)
    now.advance(10)

    assert clock.position_ns() == 42


def test_set_speed_while_running_settles_then_changes_rate() -> None:
    clock, now = _clock()

    clock.start(0)
    now.advance(2)
    clock.set_speed(3.0)
    # Exactly the position at the switch: 2 seconds * 1x.
    assert clock.position_ns() == 2_000_000_000

    now.advance(1)
    # 2s settled + 1 second * 3x.
    assert clock.position_ns() == 5_000_000_000


def test_set_speed_while_paused_keeps_the_position() -> None:
    clock, now = _clock()

    clock.start(0)
    now.advance(2)
    clock.pause()
    clock.set_speed(10.0)
    now.advance(5)

    assert clock.position_ns() == 2_000_000_000
    assert clock.speed == 10.0

    clock.resume()
    now.advance(1)
    assert clock.position_ns() == 12_000_000_000


def test_seek_jumps_the_position_and_continues_from_there() -> None:
    clock, now = _clock()

    clock.start(0)
    now.advance(2)
    clock.seek(10_000_000_000)
    assert clock.position_ns() == 10_000_000_000

    now.advance(1)
    assert clock.position_ns() == 11_000_000_000


def test_seek_while_paused_stays_frozen() -> None:
    clock, now = _clock()

    clock.start(0)
    clock.pause()
    clock.seek(20_000_000_000)
    now.advance(10)

    assert clock.position_ns() == 20_000_000_000


def test_seconds_until_returns_remaining_wall_seconds() -> None:
    clock, now = _clock()

    clock.start(0)
    now.advance(2)
    assert clock.seconds_until(5_000_000_000) == pytest.approx(3.0)


def test_seconds_until_divides_by_speed() -> None:
    clock, _ = _clock(speed=2.0)

    clock.start(0)
    # 10 replay seconds remain, consumed at 2 replay-seconds per wall second.
    assert clock.seconds_until(10_000_000_000) == pytest.approx(5.0)


def test_seconds_until_returns_zero_when_due() -> None:
    clock, now = _clock()

    clock.start(0)
    now.advance(5)
    assert clock.seconds_until(5_000_000_000) == 0

    now.advance(1)
    assert clock.seconds_until(5_000_000_000) == 0


def test_seconds_until_returns_none_while_paused() -> None:
    clock, now = _clock()

    clock.start(0)
    clock.pause()

    assert clock.seconds_until(5_000_000_000) is None


@pytest.mark.parametrize("speed", [0, -1, 0.0, -0.5, float("inf"), float("nan"), True, "2"])
def test_invalid_constructor_speed_is_rejected(speed: object) -> None:
    with pytest.raises(InvalidParameterError):
        ReplayClock(speed=speed)  # type: ignore[arg-type]


@pytest.mark.parametrize("speed", [0, -1, float("inf"), float("nan"), True, "2"])
def test_invalid_set_speed_is_rejected(speed: object) -> None:
    clock, _ = _clock()

    with pytest.raises(InvalidParameterError):
        clock.set_speed(speed)  # type: ignore[arg-type]
