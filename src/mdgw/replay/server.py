"""Thin ZMQ adapter for the Replay Service.

Wire format mirrors the Streaming Interface: every market message is a
two-frame `[topic][Arrow IPC stream]` multipart message, and End of Session is
JSON on `replay.<session_id>.control`. Control is JSON over REQ/REP, which
stays in this module. Data publishing, namespaced topics and the
`_internal.ready` gesture are delegated to `StreamingInterface`, so an
external SUB can confirm its subscription was forwarded before it calls
`create_session`.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import zmq

from mdgw.core.errors import (
    INTERNAL_ERROR,
    GatewayError,
    InvalidParameterError,
)
from mdgw.core.time import iso_to_ns
from mdgw.replay.session import ReplaySessionSpec
from mdgw.streaming import StreamingInterface

if TYPE_CHECKING:
    from typing import Self

    from mdgw.core.bar import Bar
    from mdgw.replay.service import ReplayService

logger = logging.getLogger(__name__)

CONTROL_POLL_MS = 100


class ReplayServer:
    """Binds the Replay Service's PUB and REP sockets and pumps control replies."""

    def __init__(
        self,
        service: ReplayService,
        *,
        control_bind_host: str = "127.0.0.1",
        control_port: int = 0,
        publish_bind_host: str = "127.0.0.1",
        publish_port: int = 0,
    ) -> None:
        self._service = service
        self._control_bind_host = control_bind_host
        self._control_port = control_port

        # One PUB multiplexes every Session; session ids live in the topic
        # namespace of each publish call.
        self._streaming = StreamingInterface(
            bind=_endpoint(publish_bind_host, publish_port),
            zmq_hwm=10_000,
        )
        # The REQ/REP control plane keeps its own context, separate from the
        # data PUB/SUB context owned by the Streaming Interface.
        self._context: zmq.Context | None = None
        self._controller: zmq.Socket | None = None

        self._control_endpoint: str | None = None

        self._lifecycle_lock = threading.Lock()
        self._started = False
        self._stop = threading.Event()
        self._control_thread: threading.Thread | None = None

    # -- endpoints ---------------------------------------------------------

    @property
    def publish_endpoint(self) -> str | None:
        """Resolved PUB endpoint while started; ``None`` before/after."""
        if not self._started:
            return None
        endpoints = self._streaming.endpoints
        return endpoints[0] if endpoints else None

    @property
    def control_endpoint(self) -> str | None:
        return self._control_endpoint

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        with self._lifecycle_lock:
            if self._started:
                return

            streaming = self._streaming
            context: zmq.Context | None = None
            controller: zmq.Socket | None = None
            try:
                streaming.start()
                streaming.mark_ready()
                context = zmq.Context()
                controller = self._bind_control(context)
                self._service.set_publish(self._publish_bar, self._publish_event)
            except BaseException:
                self._control_endpoint = None
                if controller is not None:
                    controller.close(linger=0)
                if context is not None:
                    context.term()
                streaming.stop()
                raise

            self._context = context
            self._controller = controller
            self._stop.clear()
            self._started = True

            self._control_thread = threading.Thread(
                target=self._run_control,
                name="mdgw-replay-control",
                daemon=True,
            )
            self._control_thread.start()

    def _bind_control(self, context: zmq.Context) -> zmq.Socket:
        """Create, configure and bind the REP control socket."""
        controller = context.socket(zmq.REP)
        controller.setsockopt(zmq.LINGER, 0)
        controller.bind(
            _endpoint(self._control_bind_host, self._control_port)
        )
        self._control_endpoint = controller.getsockopt_string(
            zmq.LAST_ENDPOINT
        )
        return controller

    def stop(self) -> None:
        with self._lifecycle_lock:
            if not self._started:
                return
            self._started = False
            self._stop.set()

            # Stop Sessions before the Streaming Interface so each one can
            # still publish its final `reason="stopped"` End of Session.
            self._service.close()
            self._streaming.stop()

            if self._control_thread is not None:
                self._control_thread.join(timeout=5.0)
                self._control_thread = None

            controller = self._controller
            self._controller = None
            if controller is not None:
                controller.close(linger=0)
            context = self._context
            self._context = None
            if context is not None:
                context.term()
            self._control_endpoint = None

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    # -- publishing --------------------------------------------------------

    def _publish_bar(self, session_id: str, bar: Bar) -> None:
        self._streaming.publish_bars(
            [bar], namespace=f"replay.{session_id}"
        )

    def _publish_event(self, session_id: str, event: Mapping[str, Any]) -> None:
        self._streaming.publish_event(
            event, namespace=f"replay.{session_id}"
        )

    # -- control -----------------------------------------------------------

    def _run_control(self) -> None:
        controller = self._controller
        if controller is None:  # pragma: no cover - defensive
            return
        poller = zmq.Poller()
        poller.register(controller, zmq.POLLIN)
        while not self._stop.is_set():
            try:
                ready = dict(poller.poll(CONTROL_POLL_MS))
            except zmq.ZMQError:  # pragma: no cover - socket closed
                break
            if controller not in ready:
                continue

            try:
                frames = controller.recv_multipart()
            except zmq.ZMQError:  # pragma: no cover - socket closed
                break

            try:
                response = self._handle_frames(frames)
            except GatewayError as exc:
                response = _error_response(exc.code, exc.message)
            except Exception:  # pragma: no cover - defensive
                logger.exception("Unexpected Replay control failure")
                response = _error_response(INTERNAL_ERROR, "internal error")

            try:
                controller.send_string(json.dumps(response, ensure_ascii=False))
            except zmq.ZMQError:  # pragma: no cover - socket closed
                break

    def _handle_frames(self, frames: list[bytes]) -> dict[str, Any]:
        """Decode one request and always return a serializable response."""
        raw = frames[-1] if frames else b""
        try:
            request = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            msg = f"control request is not valid JSON: {exc}"
            raise InvalidParameterError(msg) from exc

        if not isinstance(request, Mapping):
            msg = "control request must be a JSON object"
            raise InvalidParameterError(msg)
        return self._handle_request(request)

    def _handle_request(self, request: Mapping[str, Any]) -> dict[str, Any]:
        op = request.get("op")
        if op == "create_session":
            spec = ReplaySessionSpec.from_mapping(request)
            session_id = self._service.create_session(spec)
            return {
                "ok": True,
                "session_id": session_id,
                "publish_endpoint": self.publish_endpoint,
            }

        if op in {"stop", "pause", "resume"}:
            session_id = _require_session_id(request, op)
            if op == "stop":
                self._service.stop_session(session_id)
            elif op == "pause":
                self._service.pause_session(session_id)
            else:
                self._service.resume_session(session_id)
            return {"ok": True, "session_id": session_id}

        if op == "seek":
            session_id = _require_session_id(request, op)
            raw_ts = request.get("ts")
            if not isinstance(raw_ts, str) or not raw_ts:
                msg = (
                    "'ts' is required for the seek op and must be "
                    "an ISO-8601 string"
                )
                raise InvalidParameterError(msg)
            try:
                ts_ns = iso_to_ns(raw_ts)
            except ValueError as exc:
                raise InvalidParameterError(str(exc)) from exc
            self._service.seek_session(session_id, ts_ns)
            return {"ok": True, "session_id": session_id}

        if op == "set_speed":
            session_id = _require_session_id(request, op)
            speed = request.get("speed")
            if isinstance(speed, bool) or not isinstance(speed, (int, float)):
                msg = (
                    "'speed' is required for the set_speed op and "
                    "must be a number"
                )
                raise InvalidParameterError(msg)
            self._service.set_speed_session(session_id, speed)
            return {"ok": True, "session_id": session_id}

        if op == "step":
            session_id = _require_session_id(request, op)
            credit_count = request.get("credits")
            if (
                isinstance(credit_count, bool)
                or not isinstance(credit_count, int)
                or credit_count <= 0
            ):
                msg = (
                    "'credits' is required for the step op and "
                    "must be a positive integer"
                )
                raise InvalidParameterError(msg)
            self._service.step_session(session_id, credit_count)
            return {"ok": True, "session_id": session_id}

        msg = f"unsupported control op: {op!r}"
        raise InvalidParameterError(msg)


def _require_session_id(request: Mapping[str, Any], op: str) -> str:
    session_id = request.get("session_id")
    if not isinstance(session_id, str) or not session_id:
        msg = f"'session_id' is required for the {op} op"
        raise InvalidParameterError(msg)
    return session_id


def _endpoint(host: str, port: int) -> str:
    # Windows pyzmq wheels disable `ipc://`, so the Replay Service always uses
    # TCP on the configured loopback host.
    return f"tcp://{host}:{port}"


def _error_response(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "error": {"code": code, "message": message}}
