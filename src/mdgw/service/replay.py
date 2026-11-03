"""Replay Service Role wrapping the existing ReplayService and ReplayServer."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from mdgw.metrics import MetricsRegistry
from mdgw.reference import InstrumentStore, load_instrument_store
from mdgw.replay.server import ReplayServer
from mdgw.replay.service import ReplayService
from mdgw.service.health_server import ServiceHealthServer
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from pathlib import Path

    from mdgw.config import GatewayConfig

logger = logging.getLogger(__name__)

_NOT_STARTED_MESSAGE = "ReplayServiceRole.start() must be called first"


class ReplayServiceRole:
    """Run the Replay Protocol endpoints plus a health/metrics server."""

    def __init__(
        self,
        config: GatewayConfig,
        *,
        data_root: Path | None = None,
    ) -> None:
        self.config = config
        self.store = ParquetBarStore(
            data_root if data_root is not None else config.storage.root,
            compression=config.storage.compression,
            row_group_size=config.storage.row_group_size,
            format_version=config.storage.format_version,
            schema_version=config.storage.schema_version,
        )
        self.metrics = MetricsRegistry()
        self.instrument_store: InstrumentStore | None = None

        self.service: ReplayService | None = None
        self.server: ReplayServer | None = None
        self._health: ServiceHealthServer | None = None
        self._started = False
        self._started_at: float | None = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        if self._started:
            return
        try:
            self._start_service()
        except Exception:
            self.stop()
            raise

    def _start_service(self) -> None:
        # Replay reads the Instrument Snapshot exported by the collector,
        # never the YAML truth source (DESIGN §5.1). A missing snapshot is
        # reported through /status and the log instead of silently
        # disabling Instrument validation without a trace.
        self.instrument_store = load_instrument_store(self.store.root)
        if self.instrument_store is None:
            logger.warning(
                "Instrument Snapshot missing; Replay Sessions cannot "
                "validate Instruments",
                extra={
                    "event": "replay.instrument_snapshot_missing",
                    "directory": str(self.store.root / "instruments"),
                },
            )
        else:
            logger.info(
                "loaded Instrument Snapshot",
                extra={
                    "event": "replay.instrument_snapshot_loaded",
                    "snapshot": str(self.instrument_store.snapshot_path),
                    "instruments": len(self.instrument_store),
                },
            )

        self.service = ReplayService(
            self.store,
            max_sessions=self.config.replay.max_sessions,
            heartbeat_s=self.config.replay.heartbeat_s,
            instrument_store=self.instrument_store,
        )
        self.server = ReplayServer(
            self.service,
            control_bind_host=self.config.replay.control_bind_host,
            control_port=self.config.replay.control_port,
            publish_bind_host=self.config.replay.publish_bind_host,
            publish_port=self.config.replay.publish_port,
        )
        self.server.start()

        self._health = ServiceHealthServer(
            service="replay",
            host=self.config.service.health_host,
            port=self.config.service.replay_health_port,
            ready=self._ready_payload,
            metrics_render=self.render_metrics,
            status=self.status_payload,
        )
        self._health.start()
        self._started = True
        self._started_at = time.monotonic()

        logger.info(
            "replay service started",
            extra={
                "event": "replay.started",
                "status": "ready",
                "control_endpoint": self.server.control_endpoint,
                "publish_endpoint": self.server.publish_endpoint,
            },
        )

    def stop(self) -> None:
        if self.server is not None:
            self.server.stop()
            self.server = None
        self.service = None
        self.instrument_store = None
        if self._health is not None:
            self._health.stop()
            self._health = None
        self._started = False
        self._started_at = None

    # -- properties --------------------------------------------------------

    @property
    def control_endpoint(self) -> str:
        if self.server is None or self.server.control_endpoint is None:
            raise RuntimeError(_NOT_STARTED_MESSAGE)
        return self.server.control_endpoint

    @property
    def publish_endpoint(self) -> str:
        if self.server is None or self.server.publish_endpoint is None:
            raise RuntimeError(_NOT_STARTED_MESSAGE)
        return self.server.publish_endpoint

    @property
    def health_url(self) -> str:
        if self._health is None:
            raise RuntimeError(_NOT_STARTED_MESSAGE)
        return self._health.base_url

    # -- readiness / status ------------------------------------------------

    def _ready_payload(self) -> tuple[bool, str]:
        if self._started and self.server is not None:
            return True, "replay service ready"
        return False, "replay service starting"

    def status_payload(self) -> dict:
        summaries = (
            [] if self.service is None else self.service.session_summaries()
        )
        return {
            "service": "replay",
            "role": "replay",
            "uptime_s": self._uptime_s(),
            "control_endpoint": (
                None if self.server is None else self.server.control_endpoint
            ),
            "publish_endpoint": (
                None if self.server is None else self.server.publish_endpoint
            ),
            "active_sessions": (
                [] if self.service is None else self.service.active_session_ids()
            ),
            "sessions": summaries,
            "instrument_snapshot": self._instrument_snapshot_status(),
        }

    def _instrument_snapshot_status(self) -> dict:
        store = self.instrument_store
        if store is None:
            return {
                "status": "missing",
                "loaded": False,
                "snapshot": None,
                "count": 0,
            }
        return {
            "status": "loaded",
            "loaded": True,
            "snapshot": str(store.snapshot_path),
            "count": len(store),
        }

    # -- metrics -----------------------------------------------------------

    def render_metrics(self) -> str:
        self.metrics.set(
            "mdgw_up", 1 if self._started else 0, help="Service process is up"
        )
        self.metrics.set(
            "mdgw_ready",
            1 if self._started else 0,
            help="Service process is ready",
        )
        summaries = (
            [] if self.service is None else self.service.session_summaries()
        )
        active = sum(
            1
            for summary in summaries
            if summary.get("status") not in {"finished", "stopped", "failed"}
        )
        finished = sum(
            1 for summary in summaries if summary.get("status") == "finished"
        )
        stopped = sum(
            1 for summary in summaries if summary.get("status") == "stopped"
        )
        bars_sent = sum(int(summary.get("bars_sent", 0)) for summary in summaries)
        self.metrics.set(
            "mdgw_replay_active_sessions",
            active,
            help="Replay Sessions currently active",
        )
        self.metrics.set(
            "mdgw_replay_bars_sent",
            bars_sent,
            help="Bars sent by all Replay Sessions",
        )
        self.metrics.set(
            "mdgw_replay_sessions_finished",
            finished,
            help="Replay Sessions that ran to completion",
        )
        self.metrics.set(
            "mdgw_replay_sessions_stopped",
            stopped,
            help="Replay Sessions stopped by an operator",
        )
        return self.metrics.render()

    def _uptime_s(self) -> float:
        if self._started_at is None:
            return 0.0
        return max(0.0, time.monotonic() - self._started_at)


__all__ = ["ReplayServiceRole"]
