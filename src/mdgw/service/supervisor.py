"""`mdgw up`: start Service Roles in order and restart them if they exit.

Children are ordinary `mdgw run --service ...` processes. The supervisor waits
for each role's `/readyz` before starting the next one, then monitors exit
codes and restarts with a bounded exponential backoff. Process creation and
readiness probing are injectable so the restart policy can be unit tested
without real subprocesses.
"""

from __future__ import annotations

import ctypes
import logging
import os
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

logger = logging.getLogger(__name__)

DEFAULT_RESTART_BACKOFF_S = (0.5, 1.0, 2.0, 5.0, 10.0)
DEFAULT_POLL_INTERVAL_S = 0.2
DEFAULT_READY_TIMEOUT_S = 30.0
DEFAULT_TERMINATE_TIMEOUT_S = 10.0


class ChildProcess(Protocol):
    """The small part of `subprocess.Popen` the supervisor needs."""

    pid: int

    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


@dataclass(frozen=True)
class ChildSpec:
    role: str
    command: list[str]
    health_url: str | None = None
    ready_timeout_s: float = DEFAULT_READY_TIMEOUT_S


@dataclass
class _Child:
    spec: ChildSpec
    proc: ChildProcess
    restarts: int = 0
    next_restart_at: float = 0.0


class Supervisor:
    """Run a fixed list of Service Role children and monitor them."""

    def __init__(
        self,
        specs: Sequence[ChildSpec],
        *,
        spawn: Callable[[ChildSpec], ChildProcess] | None = None,
        probe: Callable[[str], bool] | None = None,
        restart_backoff_s: Sequence[float] = DEFAULT_RESTART_BACKOFF_S,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
        terminate_timeout_s: float = DEFAULT_TERMINATE_TIMEOUT_S,
    ) -> None:
        if not specs:
            raise ValueError("Supervisor requires at least one ChildSpec")
        self._specs = tuple(specs)
        self._spawn = spawn or _spawn_process
        self._probe = probe or _probe_http
        self._backoff = tuple(restart_backoff_s) or (0.0,)
        self._poll_interval_s = poll_interval_s
        self._terminate_timeout_s = terminate_timeout_s
        self._children: list[_Child] = []

    # -- lifecycle ---------------------------------------------------------

    def run(self, stop_event: Any) -> int:
        """Start children in order, then monitor until `stop_event` is set.

        Returns 0 on a clean stop and 1 when a child never became ready.
        """
        try:
            for spec in self._specs:
                if stop_event.is_set():
                    return 0
                child = self._start_child(spec)
                if spec.health_url is None:
                    continue
                if not self._wait_ready(child, stop_event):
                    logger.error(
                        "child did not become ready",
                        extra={
                            "event": "supervisor.ready_timeout",
                            "service": spec.role,
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

    def _start_child(self, spec: ChildSpec) -> _Child:
        proc = self._spawn(spec)
        child = _Child(spec=spec, proc=proc)
        self._children.append(child)
        logger.info(
            "child started",
            extra={
                "event": "supervisor.child_started",
                "service": spec.role,
                "status": "running",
            },
        )
        return child

    def _wait_ready(self, child: _Child, stop_event: Any) -> bool:
        assert child.spec.health_url is not None
        deadline = time.monotonic() + child.spec.ready_timeout_s
        while time.monotonic() < deadline:
            if child.proc.poll() is not None:
                logger.warning(
                    "child exited before becoming ready",
                    extra={
                        "event": "supervisor.child_exited",
                        "service": child.spec.role,
                    },
                )
                return False
            if self._probe(child.spec.health_url):
                logger.info(
                    "child is ready",
                    extra={
                        "event": "supervisor.child_ready",
                        "service": child.spec.role,
                    },
                )
                return True
            if stop_event.wait(0.1):
                return False
        return False

    def _monitor(self) -> None:
        now = time.monotonic()
        for index, child in enumerate(self._children):
            code = child.proc.poll()
            if code is None:
                continue
            logger.warning(
                "child exited",
                extra={
                    "event": "supervisor.child_exited",
                    "service": child.spec.role,
                    "status": code,
                },
            )
            if now < child.next_restart_at:
                continue
            delay = self._backoff[min(child.restarts, len(self._backoff) - 1)]
            try:
                proc = self._spawn(child.spec)
            except Exception:
                logger.exception(
                    "child restart failed",
                    extra={
                        "event": "supervisor.child_restart_failed",
                        "service": child.spec.role,
                    },
                )
                child.next_restart_at = now + delay
                child.restarts += 1
                continue
            child.proc = proc
            child.restarts += 1
            child.next_restart_at = now + delay
            self._children[index] = child
            logger.info(
                "child restarted",
                extra={
                    "event": "supervisor.child_restarted",
                    "service": child.spec.role,
                },
            )

    def _stop_all(self) -> None:
        children = list(self._children)
        self._children.clear()
        for child in children:
            if child.proc.poll() is not None:
                continue
            try:
                child.proc.terminate()
            except Exception:  # pragma: no cover - already gone
                pass
        for child in children:
            if child.proc.poll() is not None:
                continue
            try:
                child.proc.wait(timeout=self._terminate_timeout_s)
            except subprocess.TimeoutExpired:
                kill = getattr(child.proc, "kill", None)
                if kill is not None:
                    try:
                        kill()
                    except Exception:  # pragma: no cover - defensive
                        pass
                try:
                    child.proc.wait(timeout=5)
                except Exception:  # pragma: no cover - defensive
                    pass
            except Exception:  # pragma: no cover - defensive
                pass
            logger.info(
                "child stopped",
                extra={
                    "event": "supervisor.child_stopped",
                    "service": child.spec.role,
                },
            )


def _spawn_process(spec: ChildSpec) -> subprocess.Popen:
    # Inherit stdio so child JSON logs land on the supervisor's stream.
    proc = subprocess.Popen(spec.command)
    _assign_to_windows_job(proc)
    return proc


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

_windows_job: int | None = None


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
    global _windows_job
    if os.name != "nt":  # pragma: no cover - non-Windows
        return None
    if _windows_job is not None:
        return _windows_job
    try:  # pragma: no cover - Windows-only
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
        _windows_job = job
        return job
    except Exception:  # pragma: no cover - defensive
        return None


def _assign_to_windows_job(proc: subprocess.Popen) -> None:
    job = _ensure_windows_job()
    if job is None:
        return
    try:  # pragma: no cover - Windows-only
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
    except Exception:  # pragma: no cover - defensive
        # Some CI runners already place us in a non-breakaway job; the normal
        # terminate path still stops children in that case.
        return


def _probe_http(url: str, *, timeout: float = 1.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError, ValueError):
        return False


__all__ = ["ChildProcess", "ChildSpec", "Supervisor"]
