"""Server-owned Replay Clock.

The Replay Clock turns wall time into Replay Position time:

    position = anchor_position + (now - anchor_wall) * speed

The clock's zero point is the session's `start_ts`, so a Bar closing at
`close_ts` becomes due `(close_ts - start_ts) / speed` wall seconds after the
worker starts the clock (i.e. after the Parquet read, so read latency never
leaks into the replay tempo).

`now_ns` is injectable so unit tests can verify the arithmetic exactly without
sleeping. The default is `time.monotonic_ns`: wall-clock jumps must never move
a replay position.
"""

from __future__ import annotations

import math
import time
from typing import TYPE_CHECKING

from mdgw.core.errors import InvalidParameterError

if TYPE_CHECKING:
    from collections.abc import Callable

NANOS_PER_SECOND = 1_000_000_000

_SPEED_ERROR = "'speed' must be a finite number greater than 0"


def validate_speed(speed: float) -> float:
    """Return `speed` as a finite float greater than zero.

    `bool` is rejected explicitly even though it subclasses `int`: `speed=True`
    is a client bug, not a request for 1x replay.
    """
    if isinstance(speed, bool) or not isinstance(speed, (int, float)):
        raise InvalidParameterError(_SPEED_ERROR)
    value = float(speed)
    if not math.isfinite(value) or value <= 0:
        raise InvalidParameterError(_SPEED_ERROR)
    return value


class ReplayClock:
    """A pausable, seekable replay position driven by an injectable wall clock."""

    def __init__(
        self,
        *,
        now_ns: Callable[[], int] = time.monotonic_ns,
        speed: float = 1.0,
    ) -> None:
        self._now_ns = now_ns
        self._speed = validate_speed(speed)
        self._anchor_pos_ns = 0
        self._anchor_wall_ns = 0
        self._started = False
        self._paused = False

    # -- state -------------------------------------------------------------

    @property
    def speed(self) -> float:
        return self._speed

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def started(self) -> bool:
        return self._started

    # -- position ----------------------------------------------------------

    def _position_at(self, now_ns: int) -> int:
        if self._paused:
            return self._anchor_pos_ns
        elapsed_ns = now_ns - self._anchor_wall_ns
        if elapsed_ns <= 0:
            return self._anchor_pos_ns
        return self._anchor_pos_ns + int(elapsed_ns * self._speed)

    def position_ns(self) -> int:
        """Current replay position; frozen at the anchor while paused."""
        if not self._started:
            return self._anchor_pos_ns
        return self._position_at(self._now_ns())

    def start(self, position_ns: int) -> None:
        """Anchor the position to wall time; read latency is excluded.

        A clock paused before `start` (e.g. a client paused the session while
        the worker was still reading) stays paused at `position_ns`.
        """
        self._anchor_pos_ns = int(position_ns)
        self._anchor_wall_ns = self._now_ns()
        self._started = True

    def pause(self) -> None:
        """Freeze the position. Idempotent."""
        if self._paused:
            return
        if self._started:
            now_ns = self._now_ns()
            self._anchor_pos_ns = self._position_at(now_ns)
            self._anchor_wall_ns = now_ns
        self._paused = True

    def resume(self) -> None:
        """Continue from the frozen position. Idempotent."""
        if not self._paused:
            return
        self._anchor_wall_ns = self._now_ns()
        self._paused = False

    def set_speed(self, speed: float) -> None:
        """Settle the position under the old speed, then adopt the new one.

        While paused only the speed changes; the frozen position stays put.
        """
        value = validate_speed(speed)
        if self._started and not self._paused:
            now_ns = self._now_ns()
            self._anchor_pos_ns = self._position_at(now_ns)
            self._anchor_wall_ns = now_ns
        self._speed = value

    def seek(self, position_ns: int) -> None:
        """Jump straight to `position_ns`; commonly used while paused."""
        self._anchor_pos_ns = int(position_ns)
        self._anchor_wall_ns = self._now_ns()
        self._started = True

    def seconds_until(self, target_ns: int) -> float | None:
        """Wall seconds until `target_ns`; `0.0` when due, `None` while paused.

        The Replay Clock advances `speed` replay seconds per wall second, so
        the wall wait is the remaining replay time divided by the speed.
        """
        if self._paused:
            return None
        remaining_ns = int(target_ns) - self.position_ns()
        if remaining_ns <= 0:
            return 0.0
        return remaining_ns / (NANOS_PER_SECOND * self._speed)
