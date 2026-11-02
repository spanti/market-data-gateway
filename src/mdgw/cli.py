"""Operator CLI: `run --service`, `up`, `compact`, and `gc`.

`run` starts exactly one Service Role and blocks until SIGINT/SIGTERM; `up` is
a small supervisor that starts collector -> ready -> api and restarts unexpected
exits. Only `argparse` is used; framework imports for the roles are lazy so the
maintenance commands stay lightweight.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
from datetime import date, timedelta
from pathlib import Path

from mdgw.config import GatewayConfig, load_config
from mdgw.core.time import utc_now
from mdgw.logging import configure_json_logging
from mdgw.store.maintenance import parse_duration
from mdgw.store.parquet_dataset import ParquetBarStore

DEFAULT_CONFIG_PATH = Path("config/gateway.toml")
SERVICE_ROLES = ("collector", "api", "replay")

logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid date {value!r}; expected YYYY-MM-DD"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mdgw",
        description="Market Data Gateway operator commands",
    )
    _add_common_options(parser)

    subcommands = parser.add_subparsers(dest="command", required=True)

    compact = subcommands.add_parser(
        "compact", help="merge small Data Files per partition"
    )
    _add_common_options(compact, suppress_defaults=True)
    compact.add_argument(
        "--older-than",
        required=True,
        metavar="DURATION",
        help="only compact Data Files older than this (for example 24h or 7d)",
    )

    gc = subcommands.add_parser(
        "gc", help="delete whole month partitions past the Retention window"
    )
    _add_common_options(gc, suppress_defaults=True)
    mode = gc.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="report what would be deleted (default)",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="actually delete Data Files and switch the Manifest",
    )

    run = subcommands.add_parser(
        "run", help="run exactly one Service Role until SIGINT/SIGTERM"
    )
    run.add_argument(
        "--service",
        required=True,
        choices=SERVICE_ROLES,
        help="Service Role to start",
    )
    _add_common_options(run, suppress_defaults=True)
    run.add_argument(
        "--log-level",
        default=None,
        metavar="LEVEL",
        help="override [service].log_level",
    )
    run.add_argument(
        "--wait-for-api",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "collector: wait for the api READY handshake before ingesting "
            "(overrides [service].wait_for_api_ready)"
        ),
    )

    up = subcommands.add_parser(
        "up",
        help="start collector -> ready -> api and restart unexpected exits",
    )
    _add_common_options(up, suppress_defaults=True)
    up.add_argument(
        "--log-level",
        default=None,
        metavar="LEVEL",
        help="override [service].log_level",
    )

    backfill = subcommands.add_parser(
        "backfill",
        help="import historical Bars (v1: Binance archive backfill)",
    )
    _add_common_options(backfill, suppress_defaults=True)
    backfill.add_argument(
        "--archive",
        action="store_true",
        help="import Binance data.binance.vision K-line archives (v1 only mode)",
    )
    backfill.add_argument(
        "--log-level",
        default=None,
        metavar="LEVEL",
        help="override [service].log_level",
    )
    backfill.add_argument(
        "--instrument",
        action="append",
        default=[],
        metavar="INSTRUMENT_ID",
        help=(
            "import this InstrumentId (repeatable); defaults to every "
            "venue/market-type match in Instrument Reference Data"
        ),
    )
    backfill.add_argument(
        "--start",
        type=_parse_date,
        default=None,
        metavar="YYYY-MM-DD",
        help="first UTC date to import (inclusive; defaults to --end)",
    )
    backfill.add_argument(
        "--end",
        type=_parse_date,
        default=None,
        metavar="YYYY-MM-DD",
        help="last UTC date to import (inclusive; defaults to yesterday UTC)",
    )
    backfill.add_argument("--venue", default="binance")
    backfill.add_argument("--market-type", default="spot")
    backfill.add_argument(
        "--interval",
        default="1m",
        help="archive K-line interval (v1 only supports 1m)",
    )
    backfill.add_argument(
        "--granularity",
        choices=("auto", "daily", "monthly"),
        default="auto",
        help="archive file granularity; auto uses monthly for complete months",
    )
    backfill.add_argument(
        "--archive-dir",
        type=Path,
        default=None,
        help="read archives from this local directory; never touches the network",
    )
    backfill.add_argument(
        "--base-url",
        default="https://data.binance.vision",
        help="data.binance.vision base URL",
    )
    backfill.add_argument("--timeout-s", type=float, default=30.0)
    backfill.add_argument("--retries", type=int, default=3)
    backfill.add_argument(
        "--feed",
        default="binance_archive",
        help="Feed Handler name stamped on imported Bars",
    )

    return parser


def _add_common_options(
    parser: argparse.ArgumentParser, *, suppress_defaults: bool = False
) -> None:
    default = argparse.SUPPRESS if suppress_defaults else None
    parser.add_argument(
        "--config",
        type=Path,
        default=default,
        help="path to gateway.toml",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=default,
        help="override the Dataset parent directory",
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        return _run_service(args)
    if args.command == "up":
        return _run_up(args)
    if args.command == "backfill":
        return _run_archive_backfill(args, parser)

    config = _load_config(args)
    store = _build_store(args, config)

    if args.command == "compact":
        try:
            older_than = parse_duration(args.older_than)
        except ValueError as exc:
            parser.error(str(exc))
        report = store.compact(older_than=older_than)
        print(
            "compact: merged "
            f"{len(report.merged_partitions)} partition(s), wrote "
            f"{len(report.new_files)} file(s), removed "
            f"{len(report.removed_files)} file(s), skipped "
            f"{len(report.skipped_locked)} locked partition(s)"
        )
        for failure in report.failures:
            print(f"error: {failure}", file=sys.stderr)
        return 1 if report.failures else 0

    apply = bool(getattr(args, "apply", False))
    if not config.retention.enabled:
        print("gc: retention is disabled; nothing to do")
        return 0

    report = store.gc(
        bars_days=config.retention.bars_days,
        apply=apply,
    )
    mode = "apply" if apply else "dry-run"
    print(
        f"gc ({mode}): {len(report.removed_partitions)} month partition(s), "
        f"{len(report.removed_files)} file(s), skipped "
        f"{len(report.skipped_locked)} locked and "
        f"{len(report.skipped_current_month)} current month"
    )
    for failure in report.failures:
        print(f"error: {failure}", file=sys.stderr)
    return 1 if report.failures else 0


# -- Service Role commands ---------------------------------------------------


def _run_service(args: argparse.Namespace) -> int:
    config = _load_config(args)
    level = getattr(args, "log_level", None) or config.service.log_level
    configure_json_logging(level, service=args.service)
    root = getattr(args, "root", None) or config.storage.root

    try:
        service = _build_service(
            args.service,
            config,
            root,
            wait_for_api=getattr(args, "wait_for_api", None),
        )
    except Exception:
        logger.exception(
            "failed to build Service Role",
            extra={
                "event": "service.build_failed",
                "service": args.service,
            },
        )
        return 1

    stop = threading.Event()
    _install_signal_handlers(stop)
    try:
        service.start()
    except Exception:
        logger.exception(
            "Service Role failed to start",
            extra={
                "event": "service.start_failed",
                "service": args.service,
            },
        )
        _safe_stop(service)
        return 1

    logger.info(
        "Service Role started",
        extra={"event": "service.started", "service": args.service},
    )
    try:
        while not stop.wait(0.5):
            pass
    finally:
        _safe_stop(service)
        logger.info(
            "Service Role stopped",
            extra={"event": "service.stopped", "service": args.service},
        )
    return 0


def _build_service(
    role: str,
    config: GatewayConfig,
    root: Path,
    *,
    wait_for_api: bool | None = None,
):
    if role == "collector":
        from mdgw.service.collector import CollectorService

        return CollectorService(
            config, data_root=root, wait_for_api=wait_for_api
        )
    if role == "api":
        from mdgw.service.api import ApiService

        return ApiService(config, data_root=root)
    if role == "replay":
        from mdgw.service.replay import ReplayServiceRole

        return ReplayServiceRole(config, data_root=root)
    raise ValueError(f"unsupported Service Role: {role!r}")


def _safe_stop(service) -> None:
    try:
        service.stop()
    except Exception:  # pragma: no cover - defensive shutdown path
        logger.exception(
            "error while stopping Service Role",
            extra={"event": "service.stop_failed"},
        )


def _run_up(args: argparse.Namespace) -> int:
    config = _load_config(args)
    level = getattr(args, "log_level", None) or config.service.log_level
    configure_json_logging(level, service="supervisor")
    root = getattr(args, "root", None) or config.storage.root

    from mdgw.service.supervisor import Supervisor

    specs = _build_up_specs(args, config, root, level)
    supervisor = Supervisor(specs)
    stop = threading.Event()
    _install_signal_handlers(stop)
    try:
        return supervisor.run(stop)
    except Exception:
        logger.exception(
            "supervisor failed",
            extra={"event": "supervisor.failed"},
        )
        return 1


def _build_up_specs(
    args: argparse.Namespace,
    config: GatewayConfig,
    root: Path,
    level: str,
):
    from mdgw.service.supervisor import ChildSpec

    config_path = _resolved_config_path(args)
    common: list[str] = []
    if config_path is not None:
        common.extend(["--config", str(config_path)])
    common.extend(["--root", str(root)])
    if level:
        common.extend(["--log-level", level])

    collector = ChildSpec(
        role="collector",
        command=[sys.executable, "-m", "mdgw", "run", "--service", "collector"]
        + ["--wait-for-api"]
        + common,
        health_url=_local_url(
            config.service.health_host,
            config.service.collector_health_port,
            "/readyz",
        ),
        ready_timeout_s=config.service.collector_ready_timeout_s,
    )
    api = ChildSpec(
        role="api",
        command=[sys.executable, "-m", "mdgw", "run", "--service", "api"]
        + common,
        health_url=_local_url(
            config.server.host,
            config.server.http_port,
            "/readyz",
        ),
        ready_timeout_s=config.service.collector_ready_timeout_s,
    )
    return [collector, api]


def _resolved_config_path(args: argparse.Namespace) -> Path | None:
    config_path = getattr(args, "config", None)
    if config_path is None and DEFAULT_CONFIG_PATH.exists():
        config_path = DEFAULT_CONFIG_PATH
    return config_path


def _local_url(host: str, port: int, path: str) -> str:
    if host in {"0.0.0.0", "::", ""}:
        host = "127.0.0.1"
    return f"http://{host}:{port}{path}"


def _install_signal_handlers(stop: threading.Event) -> None:
    def _handle(signum, frame) -> None:  # noqa: ANN001 - signal signature
        stop.set()

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        signum = getattr(signal, name, None)
        if signum is None:
            continue
        try:
            signal.signal(signum, _handle)
        except (ValueError, OSError):
            # Not the main thread, or the signal is unsupported here.
            continue


# -- maintenance helpers -----------------------------------------------------


def _load_config(args: argparse.Namespace) -> GatewayConfig:
    config_path = getattr(args, "config", None)
    if config_path is None and DEFAULT_CONFIG_PATH.exists():
        config_path = DEFAULT_CONFIG_PATH
    if config_path is None:
        return GatewayConfig()
    try:
        return load_config(config_path)
    except (OSError, ValueError) as exc:
        raise SystemExit(f"mdgw: cannot load config {config_path}: {exc}") from exc


def _build_store(
    args: argparse.Namespace, config: GatewayConfig
) -> ParquetBarStore:
    root = getattr(args, "root", None) or config.storage.root
    return ParquetBarStore(
        root,
        compression=config.storage.compression,
        row_group_size=config.storage.row_group_size,
        format_version=config.storage.format_version,
        schema_version=config.storage.schema_version,
    )


# -- archive backfill --------------------------------------------------------


def _run_archive_backfill(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    """Import Binance `data.binance.vision` archives into the Bar Dataset."""
    if not getattr(args, "archive", False):
        parser.error(
            "backfill requires --archive (v1 only supports archive backfill)"
        )
    if args.interval != "1m":
        parser.error("--interval only supports '1m' in v1")

    config = _load_config(args)
    level = getattr(args, "log_level", None) or config.service.log_level
    configure_json_logging(level, service="backfill")
    store = _build_store(args, config)

    today = utc_now().date()
    end = args.end or (today - timedelta(days=1))
    start = args.start or end
    if end < start:
        print(
            f"error: --end {end} precedes --start {start}",
            file=sys.stderr,
        )
        return 1

    from mdgw.archive import (
        ArchiveFetcher,
        ArchiveInstrument,
        import_archive,
        market_symbol,
    )
    from mdgw.aggregate import BarAggregator
    from mdgw.core.instrument import InstrumentId
    from mdgw.reference import load_instrument_reference
    from mdgw.state.high_water_mark import HighWaterMarkStore

    try:
        reference = load_instrument_reference(Path(config.instruments.path))
    except (OSError, ValueError) as exc:
        print(f"error: cannot load Instrument Reference Data: {exc}", file=sys.stderr)
        return 1

    by_id = {instrument.instrument_id: instrument for instrument in reference.instruments}
    requested = list(dict.fromkeys(args.instrument))
    if requested:
        instruments = []
        for raw in requested:
            try:
                instrument_id = InstrumentId.parse(raw).value
            except ValueError:
                print(f"error: invalid --instrument {raw!r}", file=sys.stderr)
                return 1
            instrument = by_id.get(instrument_id)
            if instrument is None:
                print(
                    f"error: Instrument {instrument_id!r} is not in "
                    "Instrument Reference Data",
                    file=sys.stderr,
                )
                return 1
            if (
                instrument.venue != args.venue
                or instrument.instrument_type != args.market_type
            ):
                print(
                    f"error: archive import only supports "
                    f"{args.venue}/{args.market_type}; "
                    f"{instrument_id!r} is "
                    f"{instrument.venue}/{instrument.instrument_type}",
                    file=sys.stderr,
                )
                return 1
            instruments.append(instrument)
    else:
        instruments = [
            instrument
            for instrument in reference.instruments
            if instrument.venue == args.venue
            and instrument.instrument_type == args.market_type
        ]

    archive_instruments = [
        ArchiveInstrument(
            instrument_id=instrument.instrument_id,
            symbol=market_symbol(instrument.instrument_id),
            asset_class=instrument.asset_class,
        )
        for instrument in instruments
    ]
    aggregator = BarAggregator(
        intervals=config.aggregation.intervals,
        base_interval=config.aggregation.base_interval,
        retention_parent_periods=config.aggregation.retention_parent_periods,
    )
    fetcher = ArchiveFetcher(
        base_url=args.base_url,
        archive_dir=args.archive_dir,
        timeout_s=args.timeout_s,
        retries=args.retries,
    )
    retention_bars_days = (
        config.retention.bars_days if config.retention.enabled else 0
    )
    hwm = HighWaterMarkStore(store.root / config.collector.state_db_name)
    try:
        report = import_archive(
            instruments=archive_instruments,
            store=store,
            hwm=hwm,
            aggregator=aggregator,
            fetcher=fetcher,
            start=start,
            end=end,
            feed=args.feed,
            market_type=args.market_type,
            interval=args.interval,
            granularity=args.granularity,
            retention_bars_days=retention_bars_days,
        )
    finally:
        hwm.close()

    print(
        "backfill --archive: "
        f"instruments={report.instruments} "
        f"files={report.files} "
        f"bars_read={report.bars_read} "
        f"bars_written={report.bars_written} "
        f"skipped_existing={report.skipped_existing} "
        f"revisions={report.revisions} "
        f"missing_files={report.missing_files} "
        f"skipped_by_retention={report.skipped_by_retention} "
        f"failures={report.failures}"
    )
    return 0 if report.ok else 1
