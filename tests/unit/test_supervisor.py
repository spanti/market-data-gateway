from __future__ import annotations

import threading
import time

from mdgw.service.supervisor import ChildSpec, Supervisor


class _FakeProcess:
    """Minimal ChildProcess whose exit is controlled by the test."""

    _next_pid = 1000

    def __init__(self, *, exit_after_polls: int | None = None) -> None:
        _FakeProcess._next_pid += 1
        self.pid = _FakeProcess._next_pid
        self._exit_after_polls = exit_after_polls
        self._polls = 0
        self.exit_code: int | None = None
        self.terminated = False

    def poll(self) -> int | None:
        self._polls += 1
        if (
            self.exit_code is None
            and self._exit_after_polls is not None
            and self._polls > self._exit_after_polls
        ):
            self.exit_code = 1
        return self.exit_code

    def terminate(self) -> None:
        self.terminated = True
        self.exit_code = 0

    def wait(self, timeout: float | None = None) -> int:
        return 0 if self.exit_code is None else self.exit_code


def _spec(role: str = "collector") -> ChildSpec:
    return ChildSpec(
        role=role,
        command=["fake", role],
        health_url="http://127.0.0.1:1/readyz",
        ready_timeout_s=1.0,
    )


def test_unexpected_exit_is_restarted() -> None:
    spawned: list[_FakeProcess] = []
    restarted = threading.Event()

    def spawn(spec: ChildSpec) -> _FakeProcess:
        # First child exits after a few polls; later children stay alive.
        proc = _FakeProcess(exit_after_polls=1 if not spawned else None)
        spawned.append(proc)
        if len(spawned) >= 2:
            restarted.set()
        return proc

    supervisor = Supervisor(
        [_spec()],
        spawn=spawn,
        probe=lambda url: True,
        restart_backoff_s=(0.01,),
        poll_interval_s=0.01,
    )
    stop = threading.Event()
    thread = threading.Thread(target=supervisor.run, args=(stop,))
    thread.start()
    try:
        assert restarted.wait(5), "the supervisor never restarted the child"
    finally:
        stop.set()
        thread.join(timeout=5)

    assert len(spawned) >= 2
    assert spawned[0].exit_code == 1


def test_ready_timeout_returns_failure_and_terminates_children() -> None:
    spawned: list[_FakeProcess] = []

    def spawn(spec: ChildSpec) -> _FakeProcess:
        proc = _FakeProcess()
        spawned.append(proc)
        return proc

    supervisor = Supervisor(
        [_spec()],
        spawn=spawn,
        probe=lambda url: False,
        poll_interval_s=0.01,
    )
    stop = threading.Event()
    started = time.monotonic()

    result = supervisor.run(stop)

    assert result == 1
    assert time.monotonic() - started < 5
    assert spawned and spawned[0].terminated


def test_stop_event_terminates_running_children() -> None:
    spawned: list[_FakeProcess] = []

    def spawn(spec: ChildSpec) -> _FakeProcess:
        proc = _FakeProcess()
        spawned.append(proc)
        return proc

    supervisor = Supervisor(
        [_spec()],
        spawn=spawn,
        probe=lambda url: True,
        poll_interval_s=0.01,
    )
    stop = threading.Event()
    thread = threading.Thread(target=supervisor.run, args=(stop,))
    thread.start()
    deadline = time.monotonic() + 5
    while not spawned and time.monotonic() < deadline:
        time.sleep(0.01)
    stop.set()
    thread.join(timeout=5)

    assert spawned and spawned[0].terminated
