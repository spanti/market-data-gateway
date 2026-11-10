# ruff: file-ignore[implicit-namespace-package]
"""Supervisor as the single controlled lifecycle adapter.

Controlled fake processes and an injected readiness probe cover the behaviours
the ticket requires without any real network or child process:

* roles start in dependency order and readiness is a real ``/readyz`` probe,
  not PID existence;
* graceful stop signals the child, drains within the grace period and records
  ``forced``/``timed_out`` only when the grace expires;
* reload validates configuration first (invalid = zero restart) and a success
  is recorded as an interrupting restart;
* a new Supervisor marks stale accepted/running operations ``unknown``.
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

import pytest

from mdgw.admin.db import AdminStore
from mdgw.admin.lifecycle.models import (
    STATUS_ACCEPTED,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    STATUS_UNKNOWN,
    NewOperation,
)
from mdgw.admin.lifecycle.store import LifecycleStore
from mdgw.cli import _build_up_specs  # ruff: ignore[import-private-name]
from mdgw.config import GatewayConfig
from mdgw.service.supervisor import (
    ChildSpec,
    LifecycleRejectedError,
    Supervisor,
)

if TYPE_CHECKING:
    from pathlib import Path

SECOND = 1_000_000_000
BASE_NS = 1_700_000_000 * SECOND


class FakeProcess:
    """Minimal controllable child process."""

    _next_pid = 5000

    def __init__(self, role: str) -> None:
        FakeProcess._next_pid += 1
        self.role = role
        self.pid = FakeProcess._next_pid
        self.exit_code: int | None = None
        self.terminate_calls = 0
        self.wait_calls = 0

    def poll(self) -> int | None:
        return self.exit_code

    def terminate(self) -> None:
        self.terminate_calls += 1
        self.exit_code = 0

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.wait_calls += 1
        return 0 if self.exit_code is None else self.exit_code


class World:
    """Spawn/probe/signal doubles shared by the tests."""

    def __init__(self) -> None:
        self.spawned: list[FakeProcess] = []
        self.signalled: list[FakeProcess] = []
        self.ready_urls: set[str] = set()
        self.exit_on_signal = True
        self.spawn_roles: list[str] = []

    def spawn(self, spec: ChildSpec) -> FakeProcess:
        process = FakeProcess(spec.role)
        self.spawned.append(process)
        self.spawn_roles.append(spec.role)
        return process

    def probe(self, url: str) -> bool:
        return url in self.ready_urls

    def signal(self, process: FakeProcess) -> None:
        self.signalled.append(process)
        if self.exit_on_signal:
            process.exit_code = 0

    @property
    def latest(self) -> FakeProcess:
        return self.spawned[-1]


def _specs(roles: tuple[str, ...]) -> list[ChildSpec]:
    return [
        ChildSpec(
            role=role,
            command=["fake", role],
            health_url=f"http://127.0.0.1/{role}/readyz",
            ready_timeout_s=2.0,
        )
        for role in roles
    ]


def _store(tmp_path: Path) -> LifecycleStore:
    path = tmp_path / "admin" / "admin.db"
    AdminStore(path).close()
    return LifecycleStore(path)


def _now() -> int:
    return BASE_NS


def _supervisor(  # ruff: ignore[too-many-arguments]  # explicit policy knobs
    tmp_path: Path,
    world: World,
    *,
    roles: tuple[str, ...] = ("collector", "api", "replay"),
    validate_config=None,
    grace_s: float = 2.0,
    terminate_timeout_s: float = 0.2,
    on_terminal=None,
) -> tuple[Supervisor, LifecycleStore]:
    store = _store(tmp_path)
    supervisor = Supervisor(
        _specs(roles),
        spawn=world.spawn,
        probe=world.probe,
        restart_backoff_s=(0.01,),
        poll_interval_s=0.01,
        terminate_timeout_s=terminate_timeout_s,
        signal_child=world.signal,
        lifecycle=store,
        grace_s=grace_s,
        validate_config=validate_config,
        on_terminal=on_terminal,
        clock=_now,
    )
    return supervisor, store


def _run_in_thread(supervisor: Supervisor) -> tuple[threading.Event, threading.Thread]:
    stop = threading.Event()
    thread = threading.Thread(target=supervisor.run, args=(stop,), daemon=True)
    thread.start()
    return stop, thread


def _wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    msg = "condition was not met before the deadline"
    raise AssertionError(msg)


def _operation(operation_id: str) -> NewOperation:
    return NewOperation(
        operation_id=operation_id,
        kind="service_instance.start",
        instance_id="api",
        service_role="api",
        actor_subject="subject-superadmin",
        actor_display_name="Super Admin",
        actor_access_role="SuperAdmin",
        reason="test",
        request_id="req_test",
        requested_at_ns=BASE_NS,
    )


# -- run: dependency order and true readiness -------------------------------


def test_roles_start_in_dependency_order_and_pid_is_not_readiness(
    tmp_path: Path,
) -> None:
    world = World()
    supervisor, store = _supervisor(tmp_path, world)
    stop, thread = _run_in_thread(supervisor)
    try:
        _wait_for(lambda: len(world.spawned) == 1)
        # Collector process exists, but the ready probe has not passed.
        _wait_for(
            lambda: (
                (snapshot := store.get_instance("collector")) is not None
                and snapshot.pid is not None
            )
        )
        snapshot = store.get_instance("collector")
        assert snapshot.ready is False
        assert snapshot.observed_state == "starting"
        assert world.spawn_roles == ["collector"]

        world.ready_urls.add("http://127.0.0.1/collector/readyz")
        _wait_for(lambda: len(world.spawned) == 2)
        world.ready_urls.add("http://127.0.0.1/api/readyz")
        _wait_for(lambda: len(world.spawned) == 3)
        assert world.spawn_roles == ["collector", "api", "replay"]

        world.ready_urls.add("http://127.0.0.1/replay/readyz")
        _wait_for(
            lambda: all(
                snapshot.ready for snapshot in store.list_instances()
            )
        )
        states = {
            snapshot.instance_id: snapshot.observed_state
            for snapshot in store.list_instances()
        }
        assert states == {
            "collector": "running",
            "api": "running",
            "replay": "running",
        }
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


def test_lifecycle_start_begins_accepted_then_waits_for_ready(
    tmp_path: Path,
) -> None:
    world = World()
    supervisor, store = _supervisor(tmp_path, world, roles=("api",))
    try:
        accepted = supervisor.accept_operation(
            operation_id="op_start",
            action="start",
            instance_id="api",
            reason="start api",
            actor_subject="subject-superadmin",
            actor_display_name="Super Admin",
            actor_access_role="SuperAdmin",
            request_id="req_start",
        )
        assert accepted.status == STATUS_ACCEPTED

        execution = threading.Thread(
            target=supervisor.execute_operation,
            args=(accepted,),
            daemon=True,
        )
        execution.start()
        _wait_for(lambda: world.spawned)
        _wait_for(
            lambda: (
                (snapshot := store.get_instance("api")) is not None
                and snapshot.pid is not None
            )
        )
        running = store.get_operation("op_start")
        assert running.status == STATUS_RUNNING
        assert store.get_instance("api").ready is False

        world.ready_urls.add("http://127.0.0.1/api/readyz")
        execution.join(timeout=5)
        finished = store.get_operation("op_start")
        assert finished.status == STATUS_SUCCEEDED
        assert finished.result["ready"] is True
        assert finished.result["observed_state"] == "running"
        assert store.get_instance("api").ready is True
    finally:
        store.close()


# -- graceful stop / forced stop -------------------------------------------


def test_graceful_stop_signals_drains_and_records_the_result(
    tmp_path: Path,
) -> None:
    world = World()
    supervisor, store = _supervisor(tmp_path, world, roles=("api",))
    stop, thread = _run_in_thread(supervisor)
    try:
        world.ready_urls.add("http://127.0.0.1/api/readyz")
        _wait_for(lambda: store.get_instance("api").ready)

        operation = supervisor.accept_operation(
            operation_id="op_stop",
            action="stop",
            instance_id="api",
            reason="stop api",
            actor_subject="subject-superadmin",
            actor_display_name="Super Admin",
            actor_access_role="SuperAdmin",
            request_id="req_stop",
        )
        supervisor.execute_operation(operation)

        finished = store.get_operation("op_stop")
        assert finished.status == STATUS_SUCCEEDED
        assert finished.result == {
            "observed_state": "stopped",
            "ready": False,
            "forced": False,
            "drained": True,
            "timed_out": False,
            "duration_s": finished.result["duration_s"],
        }
        assert world.signalled == [world.spawned[0]]
        assert world.spawned[0].terminate_calls == 0
        snapshot = store.get_instance("api")
        assert snapshot.observed_state == "stopped"
        assert snapshot.available_actions == ["start"]
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


def test_stop_timeout_forces_the_process_and_records_timed_out(
    tmp_path: Path,
) -> None:
    world = World()
    world.exit_on_signal = False
    supervisor, store = _supervisor(
        tmp_path,
        world,
        roles=("api",),
        grace_s=0.05,
    )
    stop, thread = _run_in_thread(supervisor)
    try:
        world.ready_urls.add("http://127.0.0.1/api/readyz")
        _wait_for(lambda: store.get_instance("api").ready)

        operation = supervisor.accept_operation(
            operation_id="op_stop_forced",
            action="stop",
            instance_id="api",
            reason="stop api",
            actor_subject="subject-superadmin",
            actor_display_name="Super Admin",
            actor_access_role="SuperAdmin",
            request_id="req_stop",
        )
        started = time.monotonic()
        supervisor.execute_operation(operation)

        assert time.monotonic() - started < 5
        finished = store.get_operation("op_stop_forced")
        assert finished.status == STATUS_SUCCEEDED
        assert finished.result["forced"] is True
        assert finished.result["drained"] is False
        assert finished.result["timed_out"] is True
        assert world.spawned[0].terminate_calls == 1
        assert store.get_instance("api").observed_state == "stopped"
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


def test_stop_rejects_an_instance_that_is_not_running(tmp_path: Path) -> None:
    world = World()
    supervisor, store = _supervisor(tmp_path, world, roles=("api",))
    try:
        with pytest.raises(LifecycleRejectedError) as rejected:
            supervisor.accept_operation(
                operation_id="op_bad_stop",
                action="stop",
                instance_id="api",
                reason="stop api",
                actor_subject="subject-superadmin",
                actor_display_name="Super Admin",
                actor_access_role="SuperAdmin",
                request_id="req_stop",
            )
        assert rejected.value.code == "invalid_state"
        assert rejected.value.details["reason"] == "not_running"
    finally:
        store.close()


def test_a_second_operation_on_the_same_instance_is_rejected(
    tmp_path: Path,
) -> None:
    world = World()
    supervisor, store = _supervisor(tmp_path, world, roles=("api",))
    try:
        supervisor.accept_operation(
            operation_id="op_first",
            action="start",
            instance_id="api",
            reason="start api",
            actor_subject="subject-superadmin",
            actor_display_name="Super Admin",
            actor_access_role="SuperAdmin",
            request_id="req_start",
        )
        with pytest.raises(LifecycleRejectedError) as rejected:
            supervisor.accept_operation(
                operation_id="op_second",
                action="start",
                instance_id="api",
                reason="start api again",
                actor_subject="subject-superadmin",
                actor_display_name="Super Admin",
                actor_access_role="SuperAdmin",
                request_id="req_start",
            )
        assert rejected.value.code == "operation_in_progress"
        assert rejected.value.details["active_operation_id"] == "op_first"
    finally:
        store.close()


# -- reload ----------------------------------------------------------------


def test_reload_with_invalid_config_restarts_nothing(tmp_path: Path) -> None:
    world = World()

    def validate() -> None:
        msg = "server.http_port out of range"
        raise ValueError(msg)

    supervisor, store = _supervisor(
        tmp_path, world, roles=("api",), validate_config=validate
    )
    stop, thread = _run_in_thread(supervisor)
    try:
        world.ready_urls.add("http://127.0.0.1/api/readyz")
        _wait_for(lambda: store.get_instance("api").ready)
        spawned_before = len(world.spawned)
        signalled_before = len(world.signalled)

        operation = supervisor.accept_operation(
            operation_id="op_reload_bad",
            action="reload",
            instance_id="api",
            reason="reload api",
            actor_subject="subject-superadmin",
            actor_display_name="Super Admin",
            actor_access_role="SuperAdmin",
            request_id="req_reload",
        )
        supervisor.execute_operation(operation)

        finished = store.get_operation("op_reload_bad")
        assert finished.status == "failed"
        assert finished.error["code"] == "config_invalid"
        assert len(world.spawned) == spawned_before
        assert len(world.signalled) == signalled_before
        snapshot = store.get_instance("api")
        assert snapshot.observed_state == "running"
        assert snapshot.ready is True
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


def test_reload_success_is_recorded_as_an_interrupting_restart(
    tmp_path: Path,
) -> None:
    world = World()
    supervisor, store = _supervisor(
        tmp_path, world, roles=("api",), validate_config=lambda: None
    )
    stop, thread = _run_in_thread(supervisor)
    try:
        world.ready_urls.add("http://127.0.0.1/api/readyz")
        _wait_for(lambda: store.get_instance("api").ready)

        operation = supervisor.accept_operation(
            operation_id="op_reload_ok",
            action="reload",
            instance_id="api",
            reason="reload api",
            actor_subject="subject-superadmin",
            actor_display_name="Super Admin",
            actor_access_role="SuperAdmin",
            request_id="req_reload",
        )
        supervisor.execute_operation(operation)

        finished = store.get_operation("op_reload_ok")
        assert finished.status == STATUS_SUCCEEDED
        assert finished.result["interrupting_restart"] is True
        assert finished.result["ready"] is True
        assert len(world.spawned) == 2
        assert world.signalled == [world.spawned[0]]
        snapshot = store.get_instance("api")
        assert snapshot.observed_state == "running"
        assert snapshot.pid == world.spawned[1].pid
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


# -- unknown recovery / audit terminal callback -----------------------------


def test_a_new_supervisor_marks_stale_operations_unknown(
    tmp_path: Path,
) -> None:
    world = World()
    store = _store(tmp_path)
    try:
        store.create_operation(_operation("op_accepted"))
        store.create_operation(_operation("op_running"))
        store.start_operation("op_running", now_ns=BASE_NS + SECOND)
        store.create_operation(_operation("op_done"))
        store.finish_operation(
            "op_done", status=STATUS_SUCCEEDED, now_ns=BASE_NS + SECOND
        )

        Supervisor(
            _specs(("api",)),
            spawn=world.spawn,
            probe=world.probe,
            lifecycle=store,
            clock=_now,
        )

        assert store.get_operation("op_accepted").status == STATUS_UNKNOWN
        unknown = store.get_operation("op_running")
        assert unknown.status == STATUS_UNKNOWN
        assert unknown.error["code"] == "interrupted"
        assert store.get_operation("op_done").status == STATUS_SUCCEEDED
    finally:
        store.close()


def test_up_specs_supervise_replay_after_api(tmp_path: Path) -> None:
    config = GatewayConfig()
    specs = _build_up_specs(config, None, tmp_path, None)
    assert [spec.role for spec in specs] == ["collector", "api", "replay"]
    assert specs[2].health_url is not None

    disabled = config.model_copy(
        update={
            "service": config.service.model_copy(
                update={"replay_health_port": 0}
            )
        }
    )
    assert [
        spec.role for spec in _build_up_specs(disabled, None, tmp_path, None)
    ] == ["collector", "api"]


def test_terminal_callback_receives_the_finished_operation(
    tmp_path: Path,
) -> None:
    world = World()
    terminals = []
    supervisor, store = _supervisor(
        tmp_path,
        world,
        roles=("api",),
        on_terminal=terminals.append,
    )
    try:
        world.ready_urls.add("http://127.0.0.1/api/readyz")
        accepted = supervisor.accept_operation(
            operation_id="op_audited",
            action="start",
            instance_id="api",
            reason="start api",
            actor_subject="subject-superadmin",
            actor_display_name="Super Admin",
            actor_access_role="SuperAdmin",
            request_id="req_start",
        )
        supervisor.execute_operation(accepted)

        assert len(terminals) == 1
        assert terminals[0].operation_id == "op_audited"
        assert terminals[0].status == STATUS_SUCCEEDED
        assert terminals[0].actor_subject == "subject-superadmin"
    finally:
        store.close()
