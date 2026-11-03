"""In-process Replay Service: owns concurrent Replay Sessions.

`ReplayService` is transport-agnostic. It returns from `create_session`
immediately; reading and publishing happen on each Session's daemon thread.
`ReplayServer` supplies the PUB callback once its socket is bound.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING
from uuid import uuid4

from mdgw.core.errors import (
    InvalidParameterError,
    SessionNotFoundError,
    UnknownInstrumentError,
)
from mdgw.replay.session import Publish, ReplaySession, ReplaySessionSpec

if TYPE_CHECKING:
    from collections.abc import Callable

    from mdgw.reference import InstrumentStore
    from mdgw.store.parquet_dataset import ParquetBarStore

logger = logging.getLogger(__name__)

SESSION_ID_PREFIX = "rpl-"


def _null_publish(topic: str, payload: bytes) -> None:
    """Default publisher for transport-free use (messages are dropped)."""


def default_session_id_factory() -> str:
    return f"{SESSION_ID_PREFIX}{uuid4().hex}"


class ReplayService:
    """Creates, tracks and stops Replay Sessions inside one process."""

    # The keyword-only configuration knobs are part of this public constructor.
    def __init__(  # ruff: ignore[too-many-arguments]
        self,
        store: ParquetBarStore,
        *,
        max_sessions: int = 4,
        heartbeat_s: float = 1.0,
        publish: Publish | None = None,
        session_id_factory: Callable[[], str] | None = None,
        instrument_store: InstrumentStore | None = None,
    ) -> None:
        if max_sessions < 1:
            msg = "max_sessions must be at least 1"
            raise ValueError(msg)
        if heartbeat_s <= 0:
            msg = "heartbeat_s must be positive"
            raise ValueError(msg)
        self._store = store
        self._max_sessions = max_sessions
        self._heartbeat_s = heartbeat_s
        self._publish: Publish = publish or _null_publish
        self._session_id_factory = session_id_factory or default_session_id_factory
        # When supplied, every subscription must name an Instrument present in
        # the snapshot (Delisted Instruments included). Without a snapshot the
        # Session behaves as before; the Service Role logs that it is missing.
        self._instrument_store = instrument_store

        self._sessions: dict[str, ReplaySession] = {}
        self._lock = threading.Lock()
        self._closed = False

    # -- wiring ------------------------------------------------------------

    def set_publish(self, publish: Publish) -> None:
        """Replace the message sink, e.g. once `ReplayServer` binds its PUB."""
        self._publish = publish

    # -- control surface ---------------------------------------------------

    def create_session(self, spec: ReplaySessionSpec) -> str:
        """Start a Session and return its id without waiting for the read."""
        with self._lock:
            if self._closed:
                msg = "Replay Service is closed"
                raise InvalidParameterError(msg)
            self._validate_instruments(spec)
            active = [
                session
                for session in self._sessions.values()
                if not session.is_terminal
            ]
            if len(active) >= self._max_sessions:
                msg = (
                    f"maximum of {self._max_sessions} concurrent "
                    "Replay Sessions reached"
                )
                raise InvalidParameterError(msg)
            session_id = self._session_id_factory()
            if session_id in self._sessions:
                msg = f"Replay Session id collision: {session_id!r}"
                raise InvalidParameterError(msg)
            session = ReplaySession(
                session_id,
                spec,
                self._store,
                self._send,
                heartbeat_s=self._heartbeat_s,
            )
            self._sessions[session_id] = session
            session.start()
        logger.info(
            "replay session created",
            extra={
                "event": "replay.session_created",
                "session_id": session_id,
            },
        )
        return session_id

    def stop_session(self, session_id: str) -> bool:
        """Stop a Session; a terminal Session is an idempotent `ok`."""
        session = self._get(session_id)
        stopped = session.stop()
        logger.info(
            "replay session stopped",
            extra={
                "event": "replay.session_stopped",
                "session_id": session_id,
            },
        )
        return stopped

    def pause_session(self, session_id: str) -> bool:
        return self._get(session_id).pause()

    def resume_session(self, session_id: str) -> bool:
        return self._get(session_id).resume()

    def seek_session(self, session_id: str, ts_ns: int) -> bool:
        return self._get(session_id).seek(ts_ns)

    def set_speed_session(self, session_id: str, speed: float) -> bool:
        return self._get(session_id).set_speed(speed)

    def step_session(self, session_id: str, credit_count: int) -> bool:
        return self._get(session_id).step(credit_count)

    def session_info(self, session_id: str) -> dict[str, object]:
        return self._get(session_id).info()

    def wait_session(self, session_id: str, timeout: float | None = None) -> bool:
        return self._get(session_id).wait(timeout)

    def active_session_ids(self) -> list[str]:
        with self._lock:
            return sorted(
                session_id
                for session_id, session in self._sessions.items()
                if not session.is_terminal
            )

    def session_status(self, session_id: str) -> str:
        return self._get(session_id).status

    def session_bars_sent(self, session_id: str) -> int:
        return self._get(session_id).bars_sent

    def session_summaries(self) -> list[dict[str, object]]:
        """Return a status view of every tracked Session.

        Used by `/metrics` and `/status`.
        """
        with self._lock:
            sessions = list(self._sessions.items())
        return [
            {
                "session_id": session_id,
                "status": session.status,
                "bars_sent": session.bars_sent,
            }
            for session_id, session in sessions
        ]

    def close(self) -> None:
        """Stop every Session and wait for its worker to release resources."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            sessions = list(self._sessions.values())

        for session in sessions:
            session.stop()
        for session in sessions:
            session.wait(timeout=max(5.0, self._heartbeat_s * 2))

    # -- helpers -----------------------------------------------------------

    def _validate_instruments(self, spec: ReplaySessionSpec) -> None:
        store = self._instrument_store
        if store is None:
            return
        for subscription in spec.subscriptions:
            if store.get(subscription.instrument_id) is None:
                msg = f"unknown Instrument {subscription.instrument_id!r}"
                raise UnknownInstrumentError(
                    msg,
                    details={"instrument_id": subscription.instrument_id},
                )

    def _send(self, topic: str, payload: bytes) -> None:
        # Resolve the publisher at send time so `set_publish` also affects
        # Sessions that were created before the transport was bound.
        self._publish(topic, payload)

    def _get(self, session_id: str) -> ReplaySession:
        with self._lock:
            session = self._sessions.get(session_id)
        if session is None:
            msg = f"unknown Replay Session: {session_id!r}"
            raise SessionNotFoundError(msg)
        return session
