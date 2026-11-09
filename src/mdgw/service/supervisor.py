"""`mdgw up`: start Service Roles in order and restart them if they exit.

Children are ordinary `mdgw run --service ...` processes. The supervisor waits
for each role's `/readyz` before starting the next one, then monitors exit
codes and restarts with a bounded exponential backoff. Process creation and
readiness probing are injectable so the restart policy can be unit tested
without real subprocesses.

Ticket 05 extends this process into the single controlled lifecycle adapter
for ``collector``/``api``/``replay``:

* the registry records the real observed state, readiness from the health
  probe (never PID existence), last observation and last error;
* Start spawns in dependency order and waits for real readiness;
* Graceful Stop revokes readiness, signals the child to drain (CTRL_BREAK on
  Windows, SIGTERM elsewhere), waits for the configured grace and only then
  escalates to ``TerminateProcess``/``kill``;
* Reload validates configuration first (invalid configuration restarts
  nothing) and otherwise performs an interrupting stop+start;
* every operation transition is persisted through an injected recorder; a new
  supervisor process marks stale accepted/running operations ``unknown``.

The control transport (loopback TCP + token) lives in
``mdgw.service.supervisor_control``; this module stays transport-agnostic.
"""

from __future__ import annotations

import contextlib
import ctypes
import logging
import os
import signal
import subprocess  # ruff: ignore[suspicious-subprocess-import] - spawns `mdgw run` children
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from mdgw.admin.lifecycle.models import (
    ACTION_KINDS,
    DESIRED_RUNNING,
    DESIRED_STOPPED,
    KIND_RELOAD,
    KIND_START,
    KIND_STOP,
    OBSERVED_RELOADING,
    OBSERVED_RUNNING,
    OBSERVED_STARTING,
    OBSERVED_STOPPED,
    OBSERVED_STOPPING,
    OBSERVED_UNKNOWN,
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    InstanceSnapshot,
    NewOperation,
    OperationRecord,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

logger = logging.getLogger(__name__)

DEFAULT_RESTART_BACKOFF_S = (0.5, 1.0, 2.0, 5.0, 10.0)
DEFAULT_POLL_INTERVAL_S = 0.2
DEFAULT_READY_TIMEOUT_S = 30.0
DEFAULT_TERMINATE_TIMEOUT_S = 10.0
DEFAULT_GRACE_S = 10.0
DEFAULT_PROBE_INTERVAL_S = 1.0
_NO_SPECS_MESSAGE = "Supervisor requires at least one ChildSpec"
_DUPLICATE_ROLE_MESSAGE = "Supervisor ChildSpec roles must be unique"
_FORCED_STOP_FAILED = "the service did not stop after a forced termination"

#: Stable operation error codes shared with the API wire contract.
_CODE_CONTROL_UNAVAILABLE = "control_unavailable"
_CODE_INVALID_PARAMETER = "invalid_parameter"
_CODE_INSTANCE_NOT_FOUND = "instance_not_found"
_CODE_OPERATION_IN_PROGRESS = "operation_in_progress"
_CODE_IDEMPOTENCY_KEY_REUSE = "idempotency_key_reuse"
_CODE_INVALID_STATE = "invalid_state"
_CODE_CONFIG_INVALID = "config_invalid"
_CODE_TIMEOUT = "timeout"
_CODE_SUPERVISOR_ERROR = "supervisor_error"


class ChildProcess(Protocol):
    """The small part of `subprocess.Popen` the supervisor needs."""

    pid: int

    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


class LifecycleRecorder(Protocol):
    """The persisted-operation seam used by the Supervisor executor."""

    def mark_interrupted(self, *, now_ns: int) -> int: ...

    def upsert_instance(self, snapshot: InstanceSnapshot) -> None: ...

    def create_operation(self, operation: NewOperation) -> OperationRecord: ...

    def start_operation(
        self, operation_id: str, *, now_ns: int
    ) -> OperationRecord: ...

    def finish_operation(
        self,
        operation_id: str,
        *,
        status: str,
        now_ns: int,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> OperationRecord: ...

    def get_operation(
        self, operation_id: str
    ) -> OperationRecord | None: ...

    def find_idempotent(
        self, key: str, actor_subject: str
    ) -> OperationRecord | None: ...


class LifecycleRejectedError(Exception):
    """A lifecycle request was refused before anything was accepted."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})


class _OperationFailureError(Exception):
    """Internal terminal failure carrying a stable wire error."""

    def __init__(
        self,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})

    def to_error(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


@dataclass(frozen=True)
class ChildSpec:
    role: str
    command: list[str]
    health_url: str | None = None
    ready_timeout_s: float = DEFAULT_READY_TIMEOUT_S


class _ManagedInstance:
    """Mutable observed state for one supervised Service Role."""

    def __init__(self, spec: ChildSpec, *, now_ns: int) -> None:
        self.spec = spec
        self.lock = threading.RLock()
        self.proc: ChildProcess | None = None
        self.desired_state = DESIRED_STOPPED
        self.observed_state = OBSERVED_STOPPED
        self.ready = False
        self.ready_reason: str | None = "not started"
        self.started_at_ns: int | None = None
        self.last_observed_at_ns: int | None = now_ns
        self.last_error: dict[str, Any] | None = None
        self.active_operation_id: str | None = None
        self.restarts = 0
        self.next_restart_at = 0.0
        self.last_probe_monotonic = 0.0
        self.last_persist_monotonic = 0.0


class Supervisor:
    """Run a fixed list of Service Role children and control their lifecycle."""

    def __init__(  # ruff: ignore[too-many-arguments]  # keyword-only policy knobs
        self,
        specs: Sequence[ChildSpec],
        *,
        spawn: Callable[[ChildSpec], ChildProcess] | None = None,
        probe: Callable[[str], bool] | None = None,
        restart_backoff_s: Sequence[float] = DEFAULT_RESTART_BACKOFF_S,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
        terminate_timeout_s: float = DEFAULT_TERMINATE_TIMEOUT_S,
        signal_child: Callable[[ChildProcess], None] | None = None,
        lifecycle: LifecycleRecorder | None = None,
        grace_s: float = DEFAULT_GRACE_S,
        validate_config: Callable[[], None] | None = None,
        on_terminal: Callable[[OperationRecord], None] | None = None,
        clock: Callable[[], int] | None = None,
    ) -> None:
        if not specs:
            raise ValueError(_NO_SPECS_MESSAGE)
        roles = [spec.role for spec in specs]
        if len(set(roles)) != len(roles):
            raise ValueError(_DUPLICATE_ROLE_MESSAGE)
        if grace_s <= 0:
            msg = "grace_s must be positive"
            raise ValueError(msg)
        self._specs = tuple(specs)
        self._spawn = spawn or _spawn_process
        self._probe = probe or _probe_http
        self._backoff = tuple(restart_backoff_s) or (0.0,)
        self._poll_interval_s = poll_interval_s
        self._terminate_timeout_s = terminate_timeout_s
        self._signal_child = signal_child
        self._lifecycle = lifecycle
        self._grace_s = float(grace_s)
        self._validate_config = validate_config
        self._on_terminal = on_terminal
        self._clock = clock or time.time_ns
        now_ns = self._now_ns()
        self._ordered = [
            _ManagedInstance(spec, now_ns=now_ns) for spec in self._specs
        ]
        self._instances = {item.spec.role: item for item in self._ordered}
        self._stop_requested = False
        if self._lifecycle is not None:
            # This process is the only executor: anything left accepted or
            # running by a previous supervisor cannot have completed.
            self._lifecycle.mark_interrupted(now_ns=now_ns)
            for instance in self._ordered:
                self._persist(instance)

    # -- lifecycle ---------------------------------------------------------

    def run(self, stop_event: Any) -> int:
        """Start children in order, then monitor until `stop_event` is set.

        Returns 0 on a clean stop and 1 when a child never became ready.
        """
        try:
            for instance in self._ordered:
                if stop_event.is_set():
                    return 0
                if not self._start_instance(
                    instance, stop_event=stop_event, reason="starting"
                ):
                    logger.error(
                        "child did not become ready",
                        extra={
                            "event": "supervisor.ready_timeout",
                            "service": instance.spec.role,
                            "status": "not_ready",
                        },
                    )
                    return 1

            while not stop_event.is_set():
                self._monitor()
                if stop_event.wait(self._poll_interval_s):
                    break
            return 0
        finally:
            self._stop_all()

    def _start_instance(  # ruff: ignore[too-many-return-statements, too-many-statements] - explicit start state machine
        self,
        instance: _ManagedInstance,
        *,
        stop_event: Any = None,
        reason: str = "starting",
    ) -> bool:
        now_ns = self._now_ns()
        with instance.lock:
            proc = instance.proc
            if proc is not None and proc.poll() is None:
                return bool(
                    instance.observed_state == OBSERVED_RUNNING
                    and instance.ready
                )
            instance.desired_state = DESIRED_RUNNING
            instance.observed_state = OBSERVED_STARTING
            instance.ready = False
            instance.ready_reason = reason
            instance.last_error = None
            instance.last_observed_at_ns = now_ns
            instance.last_probe_monotonic = 0.0
        try:
            proc = self._spawn(instance.spec)
        except Exception:
            logger.exception(
                "child spawn failed",
                extra={
                    "event": "supervisor.child_restart_failed",
                    "service": instance.spec.role,
                },
            )
            with instance.lock:
                instance.observed_state = OBSERVED_UNKNOWN
                instance.last_error = {
                    "code": "supervisor_error",
                    "message": "the service process could not be started",
                    "details": {},
                }
                instance.last_observed_at_ns = self._now_ns()
            self._safe_persist(instance)
            return False

        with instance.lock:
            instance.proc = proc
            instance.started_at_ns = now_ns
            instance.last_observed_at_ns = now_ns
        self._safe_persist(instance)
        logger.info(
            "child started",
            extra={
                "event": "supervisor.child_started",
                "service": instance.spec.role,
                "status": "running",
            },
        )

        deadline = time.monotonic() + instance.spec.ready_timeout_s
        while time.monotonic() < deadline:
            proc = instance.proc
            if proc is None:
                return False
            if proc.poll() is not None:
                with instance.lock:
                    instance.observed_state = OBSERVED_UNKNOWN
                    instance.ready = False
                    instance.ready_reason = "process exited before ready"
                    instance.last_error = {
                        "code": "supervisor_error",
                        "message": "the service process exited before readiness",
                        "details": {"exit_code": proc.poll()},
                    }
                    instance.last_observed_at_ns = self._now_ns()
                self._safe_persist(instance)
                logger.warning(
                    "child exited before becoming ready",
                    extra={
                        "event": "supervisor.child_exited",
                        "service": instance.spec.role,
                    },
                )
                self._terminate_process(instance)
                return False
            if instance.spec.health_url is None or self._probe(
                instance.spec.health_url
            ):
                with instance.lock:
                    instance.observed_state = OBSERVED_RUNNING
                    instance.ready = True
                    instance.ready_reason = f"{instance.spec.role} ready"
                    instance.last_observed_at_ns = self._now_ns()
                    instance.last_probe_monotonic = time.monotonic()
                self._safe_persist(instance)
                logger.info(
                    "child is ready",
                    extra={
                        "event": "supervisor.child_ready",
                        "service": instance.spec.role,
                    },
                )
                return True
            if stop_event is not None:
                if stop_event.wait(0.05):
                    return False
            else:
                time.sleep(0.05)

        with instance.lock:
            instance.observed_state = OBSERVED_UNKNOWN
            instance.ready = False
            instance.ready_reason = "ready timeout"
            instance.last_error = {
                "code": "timeout",
                "message": "the service did not become ready in time",
                "details": {"timeout_s": instance.spec.ready_timeout_s},
            }
            instance.last_observed_at_ns = self._now_ns()
        self._safe_persist(instance)
        self._terminate_process(instance)
        return False

    # -- monitoring --------------------------------------------------------

    def _monitor(self) -> None:
        now_monotonic = time.monotonic()
        for instance in self._ordered:
            with instance.lock:
                proc = instance.proc
                active_operation = instance.active_operation_id
            if proc is None:
                continue
            code = proc.poll()
            if code is not None:
                if active_operation is not None:
                    # The operation executor owns this transition.
                    continue
                self._handle_unexpected_exit(
                    instance, code=code, now_monotonic=now_monotonic
                )
                continue
            self._observe_running(instance, now_monotonic=now_monotonic)

    def _handle_unexpected_exit(
        self,
        instance: _ManagedInstance,
        *,
        code: int,
        now_monotonic: float,
    ) -> None:
        with instance.lock:
            instance.proc = None
            instance.ready = False
            if instance.desired_state == DESIRED_STOPPED:
                instance.observed_state = OBSERVED_STOPPED
                instance.ready_reason = "stopped"
            else:
                instance.observed_state = OBSERVED_UNKNOWN
                instance.ready_reason = "process exited"
                instance.last_error = {
                    "code": "supervisor_error",
                    "message": "the service process exited unexpectedly",
                    "details": {"exit_code": code},
                }
            instance.last_observed_at_ns = self._now_ns()
            should_restart = (
                instance.desired_state == DESIRED_RUNNING
                and not self._stop_requested
            )
        self._safe_persist(instance)
        logger.warning(
            "child exited",
            extra={
                "event": "supervisor.child_exited",
                "service": instance.spec.role,
                "status": code,
            },
        )
        if not should_restart:
            return
        if now_monotonic < instance.next_restart_at:
            return
        delay = self._backoff[
            min(instance.restarts, len(self._backoff) - 1)
        ]
        try:
            proc = self._spawn(instance.spec)
        except Exception:
            logger.exception(
                "child restart failed",
                extra={
                    "event": "supervisor.child_restart_failed",
                    "service": instance.spec.role,
                },
            )
            instance.restarts += 1
            instance.next_restart_at = now_monotonic + delay
            return
        with instance.lock:
            instance.proc = proc
            instance.started_at_ns = self._now_ns()
            instance.observed_state = OBSERVED_STARTING
            instance.ready = False
            instance.ready_reason = "restarting"
            instance.last_observed_at_ns = self._now_ns()
            instance.restarts += 1
            instance.next_restart_at = now_monotonic + delay
        self._safe_persist(instance)
        logger.info(
            "child restarted",
            extra={
                "event": "supervisor.child_restarted",
                "service": instance.spec.role,
            },
        )

    def _observe_running(
        self,
        instance: _ManagedInstance,
        *,
        now_monotonic: float,
    ) -> None:
        with instance.lock:
            if (
                now_monotonic - instance.last_probe_monotonic
                < DEFAULT_PROBE_INTERVAL_S
            ):
                return
            instance.last_probe_monotonic = now_monotonic
            observed_state = instance.observed_state
        if observed_state != OBSERVED_RUNNING:
            return
        url = instance.spec.health_url
        ready = True if url is None else self._probe(url)
        with instance.lock:
            changed = instance.ready != ready
            instance.ready = ready
            instance.ready_reason = (
                f"{instance.spec.role} ready"
                if ready
                else f"{instance.spec.role} not ready"
            )
            instance.last_observed_at_ns = self._now_ns()
            should_persist = (
                changed
                or now_monotonic - instance.last_persist_monotonic
                >= DEFAULT_PROBE_INTERVAL_S
            )
            if should_persist:
                instance.last_persist_monotonic = now_monotonic
        if should_persist:
            self._safe_persist(instance)

    def _stop_all(self) -> None:
        self._stop_requested = True
        now_ns = self._now_ns()
        for instance in self._ordered:
            with instance.lock:
                instance.desired_state = DESIRED_STOPPED
                instance.ready = False
                instance.ready_reason = "supervisor stopping"
                instance.last_observed_at_ns = now_ns
        for instance in self._ordered:
            self._terminate_process(instance)
            with instance.lock:
                instance.observed_state = OBSERVED_STOPPED
                instance.ready = False
                instance.ready_reason = "stopped"
                instance.last_observed_at_ns = self._now_ns()
            self._safe_persist(instance)
            logger.info(
                "child stopped",
                extra={
                    "event": "supervisor.child_stopped",
                    "service": instance.spec.role,
                },
            )

    # -- lifecycle operations ---------------------------------------------

    def registry(self) -> list[InstanceSnapshot]:
        """Return the observed registry in dependency order."""
        return [self._snapshot(instance) for instance in self._ordered]

    def instance_snapshot(self, instance_id: str) -> InstanceSnapshot | None:
        instance = self._instances.get(instance_id)
        return None if instance is None else self._snapshot(instance)

    def operation_status(self, operation_id: str) -> OperationRecord | None:
        if self._lifecycle is None:
            return None
        return self._lifecycle.get_operation(operation_id)

    def accept_operation(  # ruff: ignore[too-many-arguments]  # explicit request fields
        self,
        *,
        operation_id: str,
        action: str,
        instance_id: str,
        reason: str,
        actor_subject: str,
        actor_display_name: str,
        actor_access_role: str | None,
        request_id: str,
        idempotency_key: str | None = None,
        idempotency_body_hash: str | None = None,
        idempotency_expires_at_ns: int | None = None,
        now_ns: int | None = None,
    ) -> OperationRecord:
        """Validate and persist one ``accepted`` operation.

        The caller must not execute anything when this raises: the rejection
        happens before acceptance, and the matching HTTP error is a ``409``/
        ``404``/``503`` with no operation created.
        """
        if self._lifecycle is None:
            msg = "lifecycle persistence is not configured"
            raise LifecycleRejectedError(_CODE_CONTROL_UNAVAILABLE, msg)
        kind = ACTION_KINDS.get(action)
        if kind is None:
            msg = f"unknown lifecycle action: {action!r}"
            raise LifecycleRejectedError(
                _CODE_INVALID_PARAMETER,
                msg,
                details={"field": "action", "allowed": sorted(ACTION_KINDS)},
            )
        if idempotency_key is not None:
            # A replay must win over state validation: the original operation
            # may have changed the instance state (for example start -> running),
            # and re-applying it must still return the original row.
            existing = self._lifecycle.find_idempotent(
                idempotency_key, actor_subject
            )
            if existing is not None:
                if (
                    existing.idempotency_body_hash
                    != idempotency_body_hash
                ):
                    msg = (
                        "the idempotency key was already used with a "
                        "different request body"
                    )
                    raise LifecycleRejectedError(
                        _CODE_IDEMPOTENCY_KEY_REUSE, msg
                    )
                return existing
        instance = self._instances.get(instance_id)
        if instance is None:
            msg = "service instance not found"
            raise LifecycleRejectedError(
                _CODE_INSTANCE_NOT_FOUND,
                msg,
                details={"resource": "service_instance", "id": instance_id},
            )
        now = self._now_ns(now_ns)
        with instance.lock:
            active = instance.active_operation_id
            if active is not None:
                msg = "another operation is already in progress"
                raise LifecycleRejectedError(
                    _CODE_OPERATION_IN_PROGRESS,
                    msg,
                    details={"active_operation_id": active},
                )
            self._validate_transition(instance, kind)
            instance.active_operation_id = operation_id

        try:
            record = self._lifecycle.create_operation(
                NewOperation(
                    operation_id=operation_id,
                    kind=kind,
                    instance_id=instance_id,
                    service_role=instance.spec.role,
                    actor_subject=actor_subject,
                    actor_display_name=actor_display_name,
                    actor_access_role=actor_access_role,
                    reason=reason,
                    request_id=request_id,
                    requested_at_ns=now,
                    idempotency_key=idempotency_key,
                    idempotency_body_hash=idempotency_body_hash,
                    idempotency_expires_at_ns=idempotency_expires_at_ns,
                )
            )
        except Exception as exc:
            self._release_operation(instance, operation_id)
            if type(exc).__name__ == "IdempotencyKeyReuseError":
                raise LifecycleRejectedError(
                    _CODE_IDEMPOTENCY_KEY_REUSE, str(exc)
                ) from exc
            raise
        if record.operation_id != operation_id:
            # An idempotent replay of an already accepted operation: return
            # the original row and never execute it a second time.
            self._release_operation(instance, operation_id)
            return record

        with instance.lock:
            instance.observed_state = {
                KIND_START: OBSERVED_STARTING,
                KIND_STOP: OBSERVED_STOPPING,
                KIND_RELOAD: OBSERVED_RELOADING,
            }[kind]
            instance.desired_state = (
                DESIRED_STOPPED if kind == KIND_STOP else DESIRED_RUNNING
            )
            instance.ready = False
            instance.ready_reason = {
                KIND_START: "starting",
                KIND_STOP: "stop requested",
                KIND_RELOAD: "reload requested",
            }[kind]
            instance.last_observed_at_ns = self._now_ns()
            instance.last_probe_monotonic = 0.0
        self._safe_persist(instance)
        return record

    def execute_operation(self, record: OperationRecord) -> None:
        """Execute one accepted operation, persisting every transition."""
        instance = self._instances.get(record.instance_id)
        if instance is None or self._lifecycle is None:
            return
        try:
            self._lifecycle.start_operation(
                record.operation_id, now_ns=self._now_ns()
            )
        except ValueError:
            # Already terminal (for example recovered as unknown).
            self._release_operation(instance, record.operation_id)
            return
        except Exception:
            logger.exception(
                "lifecycle operation could not start",
                extra={
                    "event": "supervisor.operation_start_failed",
                    "operation_id": record.operation_id,
                },
            )
            self._release_operation(instance, record.operation_id)
            return

        started = time.monotonic()
        status = STATUS_FAILED
        result: dict[str, Any] | None = None
        error: dict[str, Any] | None = None
        try:  # ruff: ignore[too-many-statements-in-try-clause]
            if record.kind == KIND_START:
                result = self._execute_start(instance)
            elif record.kind == KIND_STOP:
                result = self._execute_stop(instance)
            else:
                result = self._execute_reload(instance)
            status = STATUS_SUCCEEDED
        except _OperationFailureError as failure:
            error = failure.to_error()
            logger.warning(
                "lifecycle operation failed",
                extra={
                    "event": "supervisor.operation_failed",
                    "operation_id": record.operation_id,
                    "failure_reason": failure.code,
                },
            )
        except Exception:
            logger.exception(
                "lifecycle operation crashed",
                extra={
                    "event": "supervisor.operation_crashed",
                    "operation_id": record.operation_id,
                },
            )
            error = {
                "code": "supervisor_error",
                "message": "the supervisor failed to execute the operation",
                "details": {},
            }

        with instance.lock:
            instance.active_operation_id = None
            instance.last_observed_at_ns = self._now_ns()
        self._safe_persist(instance)
        finished: OperationRecord | None = None
        try:
            finished = self._lifecycle.finish_operation(
                record.operation_id,
                status=status,
                now_ns=self._now_ns(),
                result=result if status == STATUS_SUCCEEDED else None,
                error=error,
            )
        except Exception:
            logger.exception(
                "lifecycle operation completion could not be persisted",
                extra={
                    "event": "supervisor.operation_persist_failed",
                    "operation_id": record.operation_id,
                },
            )
        if finished is not None and self._on_terminal is not None:
            try:
                self._on_terminal(finished)
            except Exception:
                logger.exception(
                    "lifecycle terminal audit failed",
                    extra={
                        "event": "supervisor.operation_audit_failed",
                        "operation_id": record.operation_id,
                    },
                )
        logger.info(
            "lifecycle operation finished",
            extra={
                "event": "supervisor.operation_finished",
                "operation_id": record.operation_id,
                "service": record.instance_id,
                "status": status,
                "duration_s": round(time.monotonic() - started, 3),
            },
        )

    def _execute_start(self, instance: _ManagedInstance) -> dict[str, Any]:
        started = time.monotonic()
        if not self._start_instance(instance, reason="starting"):
            with instance.lock:
                observed = instance.observed_state
                last_error = dict(instance.last_error or {})
            code = (
                _CODE_TIMEOUT
                if last_error.get("code") == _CODE_TIMEOUT
                else _CODE_SUPERVISOR_ERROR
            )
            raise _OperationFailureError(
                code,
                "the service did not become ready",
                {"observed_state": observed},
            )
        with instance.lock:
            observed = instance.observed_state
            ready = instance.ready
        return {
            "observed_state": observed,
            "ready": ready,
            "forced": False,
            "drained": False,
            "timed_out": False,
            "duration_s": round(time.monotonic() - started, 3),
        }

    def _execute_stop(self, instance: _ManagedInstance) -> dict[str, Any]:
        started = time.monotonic()
        grace = self._graceful_stop(instance)
        return {
            "observed_state": OBSERVED_STOPPED,
            "ready": False,
            "forced": grace["forced"],
            "drained": grace["drained"],
            "timed_out": grace["timed_out"],
            "duration_s": round(time.monotonic() - started, 3),
        }

    def _execute_reload(self, instance: _ManagedInstance) -> dict[str, Any]:
        started = time.monotonic()
        if self._validate_config is not None:
            try:
                self._validate_config()
            except Exception as exc:
                logger.warning(
                    "reload configuration validation failed",
                    extra={
                        "event": "supervisor.reload_config_invalid",
                        "service": instance.spec.role,
                    },
                )
                self._restore_running(instance)
                msg = (
                    "configuration validation failed; nothing was restarted"
                )
                raise _OperationFailureError(
                    _CODE_CONFIG_INVALID,
                    msg,
                    {"reason": type(exc).__name__},
                ) from exc

        grace = self._graceful_stop(instance)
        if not self._start_instance(instance, reason="reloading"):
            with instance.lock:
                observed = instance.observed_state
                last_error = dict(instance.last_error or {})
            code = (
                _CODE_TIMEOUT
                if last_error.get("code") == _CODE_TIMEOUT
                else _CODE_SUPERVISOR_ERROR
            )
            raise _OperationFailureError(
                code,
                "the service did not become ready after the restart",
                {"observed_state": observed},
            )
        with instance.lock:
            observed = instance.observed_state
            ready = instance.ready
        return {
            "observed_state": observed,
            "ready": ready,
            "forced": grace["forced"],
            "drained": grace["drained"],
            "timed_out": grace["timed_out"],
            "interrupting_restart": True,
            "duration_s": round(time.monotonic() - started, 3),
        }

    def _graceful_stop(self, instance: _ManagedInstance) -> dict[str, bool]:
        """Revoke readiness, signal the child and escalate after ``grace_s``."""
        with instance.lock:
            instance.desired_state = DESIRED_STOPPED
            instance.observed_state = OBSERVED_STOPPING
            instance.ready = False
            instance.ready_reason = "stop requested"
            instance.last_observed_at_ns = self._now_ns()
            proc = instance.proc
        self._safe_persist(instance)
        if proc is None or proc.poll() is not None:
            self._mark_stopped(instance)
            return {"forced": False, "drained": True, "timed_out": False}

        try:
            self._send_graceful(proc)
        except OSError:  # pragma: no cover - the process may already be gone
            logger.debug("graceful signal failed", exc_info=True)

        deadline = time.monotonic() + self._grace_s
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            time.sleep(0.05)
        timed_out = proc.poll() is None
        if timed_out and not self._terminate_process(instance):
            raise _OperationFailureError(
                _CODE_SUPERVISOR_ERROR, _FORCED_STOP_FAILED, {}
            )
        with instance.lock:
            instance.proc = None
        self._mark_stopped(instance)
        if timed_out:
            logger.warning(
                "child required a forced stop",
                extra={
                    "event": "supervisor.forced_stop",
                    "service": instance.spec.role,
                    "status": "timed_out",
                },
            )
        return {
            "forced": timed_out,
            "drained": not timed_out,
            "timed_out": timed_out,
        }

    def _mark_stopped(self, instance: _ManagedInstance) -> None:
        with instance.lock:
            instance.observed_state = OBSERVED_STOPPED
            instance.desired_state = DESIRED_STOPPED
            instance.ready = False
            instance.ready_reason = "stopped"
            instance.last_error = None
            instance.started_at_ns = None
            instance.last_observed_at_ns = self._now_ns()
        self._safe_persist(instance)

    def _restore_running(self, instance: _ManagedInstance) -> None:
        url = instance.spec.health_url
        ready = True if url is None else self._probe(url)
        with instance.lock:
            instance.desired_state = DESIRED_RUNNING
            instance.observed_state = OBSERVED_RUNNING
            instance.ready = ready
            instance.ready_reason = (
                f"{instance.spec.role} ready"
                if ready
                else f"{instance.spec.role} not ready"
            )
            instance.last_observed_at_ns = self._now_ns()
        self._safe_persist(instance)

    # -- internals ---------------------------------------------------------

    @staticmethod
    def _validate_transition(
        instance: _ManagedInstance, kind: str
    ) -> None:
        proc = instance.proc
        alive = proc is not None and proc.poll() is None
        observed = instance.observed_state
        if kind == KIND_START:
            if alive or observed in {
                OBSERVED_RUNNING,
                OBSERVED_STARTING,
                OBSERVED_RELOADING,
            }:
                msg = "the service instance is already running"
                raise LifecycleRejectedError(
                    _CODE_INVALID_STATE,
                    msg,
                    details={
                        "reason": "already_running",
                        "observed_state": observed,
                    },
                )
            return
        if kind == KIND_STOP:
            if not alive and observed in {OBSERVED_STOPPED, OBSERVED_UNKNOWN}:
                msg = "the service instance is not running"
                raise LifecycleRejectedError(
                    _CODE_INVALID_STATE,
                    msg,
                    details={
                        "reason": "not_running",
                        "observed_state": observed,
                    },
                )
            return
        if observed != OBSERVED_RUNNING:
            msg = "the service instance is not running"
            raise LifecycleRejectedError(
                _CODE_INVALID_STATE,
                msg,
                details={"reason": "not_running", "observed_state": observed},
            )

    @staticmethod
    def _release_operation(
        instance: _ManagedInstance, operation_id: str
    ) -> None:
        with instance.lock:
            if instance.active_operation_id == operation_id:
                instance.active_operation_id = None

    def _send_graceful(self, proc: ChildProcess) -> None:
        if self._signal_child is not None:
            self._signal_child(proc)
            return
        _send_graceful_signal(proc)

    def _terminate_process(self, instance: _ManagedInstance) -> bool:
        with instance.lock:
            proc = instance.proc
        if proc is None:
            return True
        if proc.poll() is None:
            with contextlib.suppress(Exception):  # pragma: no cover
                proc.terminate()
            try:
                proc.wait(timeout=self._terminate_timeout_s)
            except subprocess.TimeoutExpired:
                kill = getattr(proc, "kill", None)
                if kill is not None:
                    with contextlib.suppress(Exception):  # pragma: no cover
                        kill()
                with contextlib.suppress(Exception):  # pragma: no cover
                    proc.wait(timeout=5)
            except Exception:  # ruff: ignore[blind-except, try-except-pass]  # pragma: no cover - defensive
                pass
        stopped = proc.poll() is not None
        if stopped:
            with instance.lock:
                if instance.proc is proc:
                    instance.proc = None
        return stopped

    def _snapshot(self, instance: _ManagedInstance) -> InstanceSnapshot:
        with instance.lock:
            proc = instance.proc
            pid = None if proc is None or proc.poll() is not None else proc.pid
            return InstanceSnapshot(
                instance_id=instance.spec.role,
                service_role=instance.spec.role,
                display_name=instance.spec.role,
                desired_state=instance.desired_state,
                observed_state=instance.observed_state,
                ready=instance.ready,
                ready_reason=instance.ready_reason,
                pid=pid,
                started_at_ns=instance.started_at_ns,
                last_observed_at_ns=(
                    instance.last_observed_at_ns
                    if instance.last_observed_at_ns is not None
                    else self._now_ns()
                ),
                last_error=instance.last_error,
                updated_at_ns=self._now_ns(),
            )

    def _persist(self, instance: _ManagedInstance) -> None:
        if self._lifecycle is None:
            return
        self._lifecycle.upsert_instance(self._snapshot(instance))

    def _safe_persist(self, instance: _ManagedInstance) -> None:
        try:
            self._persist(instance)
        except Exception:
            logger.exception(
                "service instance registry could not be persisted",
                extra={
                    "event": "supervisor.registry_persist_failed",
                    "service": instance.spec.role,
                },
            )

    def _now_ns(self, value: int | None = None) -> int:
        return int(self._clock()) if value is None else int(value)


def _spawn_process(spec: ChildSpec) -> subprocess.Popen:
    # Inherit stdio so child JSON logs land on the supervisor's stream.
    creationflags = 0
    if os.name == "nt":
        # CTRL_BREAK_EVENT can only reach a process in its own new group.
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    proc = subprocess.Popen(  # ruff: ignore[subprocess-without-shell-equals-true] - argv list, no shell
        spec.command, creationflags=creationflags
    )
    _assign_to_windows_job(proc)
    return proc


def _send_graceful_signal(proc: ChildProcess) -> None:
    """Ask a child to stop gracefully.

    Windows ``terminate()`` is ``TerminateProcess`` (no cleanup), so the
    supervisor sends CTRL_BREAK_EVENT to the child's new process group and
    lets ``mdgw run`` stop its Service Role (drain, flush, End of Session).
    On POSIX SIGTERM is already the graceful path.
    """
    if os.name == "nt":
        os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        return
    proc.terminate()


# -- Windows Job Object ------------------------------------------------------
#
# `terminate()` on Windows is `TerminateProcess`: it cannot run the supervisor's
# cleanup, so children would survive an operator kill. Placing every child in a
# Job Object configured with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE makes the OS
# kill them when the supervisor's handle (and therefore the job) closes.

_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
_JobObjectExtendedLimitInformation = 9
_PROCESS_SET_QUOTA = 0x0100
_PROCESS_TERMINATE = 0x0001


class _WindowsJobHandle:
    """Process-wide Job Object handle, created on first use."""

    value: int | None = None


if os.name == "nt":  # pragma: no cover - Windows-only structures

    class _BasicLimitInformation(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", ctypes.c_ulong),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", ctypes.c_ulong),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", ctypes.c_ulong),
            ("SchedulingClass", ctypes.c_ulong),
        ]

    class _IoCounters(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class _ExtendedLimitInformation(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimitInformation),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]


def _ensure_windows_job() -> int | None:
    if os.name != "nt":  # pragma: no cover - non-Windows
        return None
    if _WindowsJobHandle.value is not None:
        return _WindowsJobHandle.value
    try:  # ruff: ignore[too-many-statements-in-try-clause]  # pragma: no cover - Windows-only
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.restype = ctypes.c_void_p
        kernel32.SetInformationJobObject.restype = ctypes.c_int
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            return None
        info = _ExtendedLimitInformation()
        info.BasicLimitInformation.LimitFlags = (
            _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        )
        configured = kernel32.SetInformationJobObject(
            job,
            _JobObjectExtendedLimitInformation,
            ctypes.byref(info),
            ctypes.sizeof(info),
        )
        if not configured:
            kernel32.CloseHandle(job)
            return None
    except Exception:  # ruff: ignore[blind-except]  # pragma: no cover - defensive
        return None
    else:
        _WindowsJobHandle.value = job
        return job


def _assign_to_windows_job(proc: subprocess.Popen) -> None:
    job = _ensure_windows_job()
    if job is None:
        return
    try:  # ruff: ignore[too-many-statements-in-try-clause]  # pragma: no cover - Windows-only
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.restype = ctypes.c_void_p
        kernel32.AssignProcessToJobObject.restype = ctypes.c_int
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        handle = kernel32.OpenProcess(
            _PROCESS_SET_QUOTA | _PROCESS_TERMINATE, False, proc.pid
        )
        if not handle:
            return
        try:
            kernel32.AssignProcessToJobObject(job, handle)
        finally:
            kernel32.CloseHandle(handle)
    except Exception:  # ruff: ignore[blind-except]  # pragma: no cover - defensive
        # Some CI runners already place us in a non-breakaway job; the normal
        # terminate path still stops children in that case.
        return


def _probe_http(url: str, *, timeout: float = 1.0) -> bool:
    try:
        with urllib.request.urlopen(  # ruff: ignore[suspicious-url-open-usage] - local health probe
            url, timeout=timeout
        ) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError, ValueError):
        return False


__all__ = [
    "ChildProcess",
    "ChildSpec",
    "LifecycleRecorder",
    "LifecycleRejectedError",
    "Supervisor",
]
