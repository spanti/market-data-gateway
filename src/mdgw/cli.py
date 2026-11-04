"""Operator CLI: `run --service`, `up`, `compact`, `gc`, and `backfill`.

`run` starts exactly one Service Role and blocks until SIGINT/SIGTERM; `up` is
a small supervisor that starts collector -> ready -> api and restarts unexpected
exits. The command tree is built with Typer; framework imports for the roles are
lazy so the maintenance commands stay lightweight.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click
import typer

from mdgw.config import GatewayConfig, load_config
from mdgw.core.time import utc_now
from mdgw.logging import configure_json_logging
from mdgw.store.maintenance import parse_duration
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from mdgw.core.instrument import Instrument
    from mdgw.reference import InstrumentReference

DEFAULT_CONFIG_PATH = Path("config/gateway.toml")
SERVICE_ROLES = ("collector", "api", "replay")
BACKFILL_GRANULARITIES = ("auto", "daily", "monthly")

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="mdgw",
    help="Market Data Gateway operator commands.",
    no_args_is_help=True,
    add_completion=False,
)


@dataclass(frozen=True)
class _CommonOptions:
    """Options accepted both before and after the subcommand name."""

    config: Path | None = None
    root: Path | None = None


def _common_options(
    ctx: typer.Context,
    config: Path | None,
    root: Path | None,
) -> tuple[Path | None, Path | None]:
    parent = ctx.obj if isinstance(ctx.obj, _CommonOptions) else _CommonOptions()
    return (
        config if config is not None else parent.config,
        root if root is not None else parent.root,
    )


@app.callback()
def _root_options(
    ctx: typer.Context,
    config: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None, "--config", metavar="PATH", help="path to gateway.toml"
    ),
    root: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None,
        "--root",
        metavar="PATH",
        help="override the Dataset parent directory",
    ),
) -> None:
    ctx.obj = _CommonOptions(config=config, root=root)


@app.command()
def compact(
    ctx: typer.Context,
    older_than: str = typer.Option(
        ...,
        "--older-than",
        metavar="DURATION",
        help="only compact Data Files older than this (for example 24h or 7d)",
    ),
    config: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None, "--config", metavar="PATH", help="path to gateway.toml"
    ),
    root: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None,
        "--root",
        metavar="PATH",
        help="override the Dataset parent directory",
    ),
) -> int:
    """Merge small Data Files per partition."""
    config_path, root_path = _common_options(ctx, config, root)
    gateway_config = _load_config(config_path)
    store = _build_store(root_path, gateway_config)

    try:
        duration = parse_duration(older_than)
    except ValueError as exc:
        raise typer.BadParameter(
            str(exc), param_hint="--older-than"
        ) from exc

    report = store.compact(older_than=duration)
    print(
        "compact: merged "
        f"{len(report.merged_partitions)} partition(s), wrote "
        f"{len(report.new_files)} file(s), removed "
        f"{len(report.removed_files)} file(s), skipped "
        f"{len(report.skipped_locked)} locked partition(s)"
    )
    for failure in report.failures:
        typer.echo(f"error: {failure}", err=True)
    if report.failures:
        raise typer.Exit(code=1)
    return 0


@app.command()
def gc(
    ctx: typer.Context,
    dry_run: bool = typer.Option(
        False, "--dry-run", help="report what would be deleted (default)"
    ),
    apply: bool = typer.Option(
        False,
        "--apply",
        help="actually delete Data Files and switch the Manifest",
    ),
    config: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None, "--config", metavar="PATH", help="path to gateway.toml"
    ),
    root: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None,
        "--root",
        metavar="PATH",
        help="override the Dataset parent directory",
    ),
) -> int:
    """Delete whole month partitions past the Retention window."""
    if dry_run and apply:
        msg = "--dry-run and --apply are mutually exclusive"
        raise typer.BadParameter(msg, param_hint="--apply")

    config_path, root_path = _common_options(ctx, config, root)
    gateway_config = _load_config(config_path)
    store = _build_store(root_path, gateway_config)

    if not gateway_config.retention.enabled:
        print("gc: retention is disabled; nothing to do")
        return 0

    report = store.gc(
        bars_days=gateway_config.retention.bars_days,
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
        typer.echo(f"error: {failure}", err=True)
    if report.failures:
        raise typer.Exit(code=1)
    return 0


@app.command()
def run(  # ruff: ignore[too-many-arguments, too-many-positional-arguments] - Typer command surface
    ctx: typer.Context,
    service: str = typer.Option(
        ...,
        "--service",
        click_type=click.Choice(SERVICE_ROLES),
        help="Service Role to start",
    ),
    config: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None, "--config", metavar="PATH", help="path to gateway.toml"
    ),
    root: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None,
        "--root",
        metavar="PATH",
        help="override the Dataset parent directory",
    ),
    log_level: Optional[str] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional] - Typer 0.9 lacks PEP 604 support
        None,
        "--log-level",
        metavar="LEVEL",
        help="override [service].log_level",
    ),
    wait_for_api: Optional[bool] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional] - Typer 0.9 lacks PEP 604 support
        None,
        "--wait-for-api/--no-wait-for-api",
        help=(
            "collector: wait for the api READY handshake before ingesting "
            "(overrides [service].wait_for_api_ready)"
        ),
    ),
) -> int:
    """Run exactly one Service Role until SIGINT/SIGTERM."""
    config_path, root_path = _common_options(ctx, config, root)
    return _run_service(
        service,
        config_path=config_path,
        root=root_path,
        log_level=log_level,
        wait_for_api=wait_for_api,
    )


@app.command()
def up(
    ctx: typer.Context,
    config: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None, "--config", metavar="PATH", help="path to gateway.toml"
    ),
    root: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None,
        "--root",
        metavar="PATH",
        help="override the Dataset parent directory",
    ),
    log_level: Optional[str] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional] - Typer 0.9 lacks PEP 604 support
        None,
        "--log-level",
        metavar="LEVEL",
        help="override [service].log_level",
    ),
) -> int:
    """Start collector -> ready -> api and restart unexpected exits."""
    config_path, root_path = _common_options(ctx, config, root)
    return _run_up(config_path, root_path, log_level)


@app.command()
def backfill(  # ruff: ignore[too-many-arguments, too-many-positional-arguments] - Typer command surface
    ctx: typer.Context,
    archive: bool = typer.Option(
        False,
        "--archive",
        help=(
            "import Binance data.binance.vision K-line archives "
            "(v1 only mode)"
        ),
    ),
    config: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None, "--config", metavar="PATH", help="path to gateway.toml"
    ),
    root: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None,
        "--root",
        metavar="PATH",
        help="override the Dataset parent directory",
    ),
    log_level: Optional[str] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional] - Typer 0.9 lacks PEP 604 support
        None,
        "--log-level",
        metavar="LEVEL",
        help="override [service].log_level",
    ),
    instrument: Optional[list[str]] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None,
        "--instrument",
        metavar="INSTRUMENT_ID",
        help=(
            "import this InstrumentId (repeatable); defaults to every "
            "venue/market-type match in Instrument Reference Data"
        ),
    ),
    start: Optional[str] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional] - Typer 0.9 lacks PEP 604 support
        None,
        "--start",
        metavar="YYYY-MM-DD",
        help="first UTC date to import (inclusive; required)",
    ),
    end: Optional[str] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional] - Typer 0.9 lacks PEP 604 support
        None,
        "--end",
        metavar="YYYY-MM-DD",
        help=(
            "last UTC date to import (inclusive; defaults to yesterday UTC)"
        ),
    ),
    venue: str = typer.Option("binance", "--venue"),
    market_type: str = typer.Option("spot", "--market-type"),
    interval: str = typer.Option(
        "1m",
        "--interval",
        help="archive K-line interval (v1 only supports 1m)",
    ),
    granularity: str = typer.Option(
        "auto",
        "--granularity",
        click_type=click.Choice(BACKFILL_GRANULARITIES),
        help=(
            "archive file granularity; auto uses monthly for complete months"
        ),
    ),
    archive_dir: Optional[Path] = typer.Option(  # ruff: ignore[non-pep604-annotation-optional, function-call-in-default-argument] - Typer 0.9 lacks PEP 604 support
        None,
        "--archive-dir",
        help="read archives from this local directory; never touches the network",
    ),
    base_url: str = typer.Option(
        "https://data.binance.vision/data",
        "--base-url",
        help="archive base URL (includes the /data prefix)",
    ),
    timeout_s: float = typer.Option(30.0, "--timeout-s"),
    retries: int = typer.Option(3, "--retries"),
    feed: str = typer.Option(
        "binance_archive",
        "--feed",
        help="Feed Handler name stamped on imported Bars",
    ),
) -> int:
    """Import historical Bars (v1: Binance archive backfill)."""
    config_path, root_path = _common_options(ctx, config, root)
    return _run_archive_backfill(
        archive=archive,
        config_path=config_path,
        root=root_path,
        log_level=log_level,
        instruments=instrument or [],
        start=_parse_date_option(start, "--start"),
        end=_parse_date_option(end, "--end"),
        venue=venue,
        market_type=market_type,
        interval=interval,
        granularity=granularity,
        archive_dir=archive_dir,
        base_url=base_url,
        timeout_s=timeout_s,
        retries=retries,
        feed=feed,
    )


def _parse_date_option(value: str | None, param_hint: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        msg = f"invalid date {value!r}; expected YYYY-MM-DD"
        raise typer.BadParameter(msg, param_hint=param_hint) from exc


def main(argv: list[str] | None = None) -> int:
    """Console entry point; returns the process exit code."""
    try:
        result = app(args=argv, standalone_mode=False)
    except click.exceptions.ClickException as exc:
        exc.show()
        raise SystemExit(exc.exit_code) from None
    except click.exceptions.Abort:
        click.echo("Aborted!", err=True)
        return 1
    return 0 if result is None else int(result)


# -- Service Role commands ---------------------------------------------------


def _run_service(
    service: str,
    *,
    config_path: Path | None,
    root: Path | None,
    log_level: str | None,
    wait_for_api: bool | None,
) -> int:
    config = _load_config(config_path)
    level = log_level or config.service.log_level
    configure_json_logging(level, service=service)
    data_root = root or config.storage.root

    try:
        role = _build_service(
            service,
            config,
            data_root,
            wait_for_api=wait_for_api,
        )
    except Exception:
        logger.exception(
            "failed to build Service Role",
            extra={
                "event": "service.build_failed",
                "service": service,
            },
        )
        raise typer.Exit(code=1) from None

    stop = threading.Event()
    _install_signal_handlers(stop)
    try:
        role.start()
    except Exception:
        logger.exception(
            "Service Role failed to start",
            extra={
                "event": "service.start_failed",
                "service": service,
            },
        )
        _safe_stop(role)
        raise typer.Exit(code=1) from None

    logger.info(
        "Service Role started",
        extra={"event": "service.started", "service": service},
    )
    try:
        while not stop.wait(0.5):
            pass
    finally:
        _safe_stop(role)
        logger.info(
            "Service Role stopped",
            extra={"event": "service.stopped", "service": service},
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
        from mdgw.service.collector import CollectorService  # ruff: ignore[import-outside-top-level] - lazy import keeps maintenance commands lightweight

        return CollectorService(
            config, data_root=root, wait_for_api=wait_for_api
        )
    if role == "api":
        from mdgw.service.api import ApiService  # ruff: ignore[import-outside-top-level] - lazy import keeps maintenance commands lightweight

        return ApiService(config, data_root=root)
    if role == "replay":
        from mdgw.service.replay import ReplayServiceRole  # ruff: ignore[import-outside-top-level] - lazy import keeps maintenance commands lightweight

        return ReplayServiceRole(config, data_root=root)
    msg = f"unsupported Service Role: {role!r}"
    raise ValueError(msg)


def _safe_stop(service) -> None:
    try:
        service.stop()
    except Exception:  # pragma: no cover - defensive shutdown path
        logger.exception(
            "error while stopping Service Role",
            extra={"event": "service.stop_failed"},
        )


def _run_up(
    config_path: Path | None,
    root: Path | None,
    log_level: str | None,
) -> int:
    config = _load_config(config_path)
    level = log_level or config.service.log_level
    configure_json_logging(level, service="supervisor")
    data_root = root or config.storage.root

    from mdgw.service.supervisor import Supervisor  # ruff: ignore[import-outside-top-level] - lazy import keeps maintenance commands lightweight

    specs = _build_up_specs(
        config, _resolved_config_path(config_path), data_root, level
    )
    supervisor = Supervisor(specs)
    stop = threading.Event()
    _install_signal_handlers(stop)
    try:
        code = supervisor.run(stop)
    except Exception:
        logger.exception(
            "supervisor failed",
            extra={"event": "supervisor.failed"},
        )
        raise typer.Exit(code=1) from None
    if code:
        raise typer.Exit(code=code)
    return 0


def _build_up_specs(
    config: GatewayConfig,
    config_path: Path | None,
    root: Path,
    level: str | None,
):
    from mdgw.service.supervisor import ChildSpec  # ruff: ignore[import-outside-top-level] - lazy import keeps maintenance commands lightweight

    common: list[str] = []
    if config_path is not None:
        common.extend(["--config", str(config_path)])
    common.extend(["--root", str(root)])
    if level:
        common.extend(["--log-level", level])

    collector = ChildSpec(
        role="collector",
        command=[
            sys.executable,
            "-m",
            "mdgw",
            "run",
            "--service",
            "collector",
            "--wait-for-api",
            *common,
        ],
        health_url=_local_url(
            config.service.health_host,
            config.service.collector_health_port,
            "/readyz",
        ),
        ready_timeout_s=config.service.collector_ready_timeout_s,
    )
    api = ChildSpec(
        role="api",
        command=[
            sys.executable,
            "-m",
            "mdgw",
            "run",
            "--service",
            "api",
            *common,
        ],
        health_url=_local_url(
            config.server.host,
            config.server.http_port,
            "/readyz",
        ),
        ready_timeout_s=config.service.collector_ready_timeout_s,
    )
    return [collector, api]


def _resolved_config_path(config_path: Path | None) -> Path | None:
    if config_path is None and DEFAULT_CONFIG_PATH.exists():
        config_path = DEFAULT_CONFIG_PATH
    return config_path


def _local_url(host: str, port: int, path: str) -> str:
    if host in {"0.0.0.0", "::", ""}:  # ruff: ignore[hardcoded-bind-all-interfaces] - compares the configured host; no socket bind
        host = "127.0.0.1"
    return f"http://{host}:{port}{path}"


def _install_signal_handlers(stop: threading.Event) -> None:
    def _handle(_signum, _frame) -> None:
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


def _load_config(config_path: Path | None) -> GatewayConfig:
    config_path = _resolved_config_path(config_path)
    if config_path is None:
        return GatewayConfig()
    try:
        return load_config(config_path)
    except (OSError, ValueError) as exc:
        msg = f"mdgw: cannot load config {config_path}: {exc}"
        raise SystemExit(msg) from exc


def _build_store(
    root: Path | None, config: GatewayConfig
) -> ParquetBarStore:
    root = root or config.storage.root
    return ParquetBarStore(
        root,
        compression=config.storage.compression,
        row_group_size=config.storage.row_group_size,
        format_version=config.storage.format_version,
        schema_version=config.storage.schema_version,
    )


# -- archive backfill --------------------------------------------------------


def _select_archive_instruments(
    reference: InstrumentReference,
    instruments: list[str],
    venue: str,
    market_type: str,
) -> list[Instrument]:
    """Resolve requested InstrumentIds, or every venue/market-type match."""
    from mdgw.core.instrument import InstrumentId  # ruff: ignore[import-outside-top-level] - lazy import keeps maintenance commands lightweight

    by_id = {
        instrument.instrument_id: instrument
        for instrument in reference.instruments
    }
    requested = list(dict.fromkeys(instruments))
    if not requested:
        return [
            instrument
            for instrument in reference.instruments
            if instrument.venue == venue
            and instrument.instrument_type == market_type
        ]

    selected: list[Instrument] = []
    for raw in requested:
        try:
            instrument_id = InstrumentId.parse(raw).value
        except ValueError:
            typer.echo(f"error: invalid --instrument {raw!r}", err=True)
            raise typer.Exit(code=1) from None
        instrument = by_id.get(instrument_id)
        if instrument is None:
            typer.echo(
                f"error: Instrument {instrument_id!r} is not in "
                "Instrument Reference Data",
                err=True,
            )
            raise typer.Exit(code=1)
        if (
            instrument.venue != venue
            or instrument.instrument_type != market_type
        ):
            typer.echo(
                f"error: archive import only supports "
                f"{venue}/{market_type}; "
                f"{instrument_id!r} is "
                f"{instrument.venue}/{instrument.instrument_type}",
                err=True,
            )
            raise typer.Exit(code=1)
        selected.append(instrument)
    return selected


def _run_archive_backfill(  # ruff: ignore[too-many-arguments] - keyword-only import pipeline knobs
    *,
    archive: bool,
    config_path: Path | None,
    root: Path | None,
    log_level: str | None,
    instruments: list[str],
    start: date | None,
    end: date | None,
    venue: str,
    market_type: str,
    interval: str,
    granularity: str,
    archive_dir: Path | None,
    base_url: str,
    timeout_s: float,
    retries: int,
    feed: str,
) -> int:
    """Import Binance `data.binance.vision` archives into the Bar Dataset."""
    if not archive:
        msg = "backfill requires --archive (v1 only supports archive backfill)"
        raise typer.BadParameter(msg, param_hint="--archive")
    if interval != "1m":
        msg = "--interval only supports '1m' in v1"
        raise typer.BadParameter(msg, param_hint="--interval")
    if venue != "binance":
        msg = "archive backfill only supports --venue binance in v1"
        raise typer.BadParameter(msg, param_hint="--venue")
    if market_type != "spot":
        msg = "archive backfill only supports --market-type spot in v1"
        raise typer.BadParameter(msg, param_hint="--market-type")
    if start is None:
        msg = "--start is required for archive backfill"
        raise typer.BadParameter(msg, param_hint="--start")
    if archive_dir is not None and not archive_dir.is_dir():
        msg = f"--archive-dir is not a directory: {archive_dir}"
        raise typer.BadParameter(msg, param_hint="--archive-dir")

    config = _load_config(config_path)
    level = log_level or config.service.log_level
    configure_json_logging(level, service="backfill")
    store = _build_store(root, config)

    end = end or (utc_now().date() - timedelta(days=1))
    if end < start:
        typer.echo(
            f"error: --end {end} precedes --start {start}", err=True
        )
        raise typer.Exit(code=1)

    from mdgw.aggregate import BarAggregator  # ruff: ignore[import-outside-top-level] - lazy import keeps maintenance commands lightweight
    from mdgw.archive import (  # ruff: ignore[import-outside-top-level] - lazy import keeps maintenance commands lightweight
        ArchiveFetcher,
        ArchiveInstrument,
        import_archive,
        market_symbol,
    )
    from mdgw.reference import load_instrument_reference  # ruff: ignore[import-outside-top-level] - lazy import keeps maintenance commands lightweight
    from mdgw.state.high_water_mark import HighWaterMarkStore  # ruff: ignore[import-outside-top-level] - lazy import keeps maintenance commands lightweight

    try:
        reference = load_instrument_reference(Path(config.instruments.path))
    except (OSError, ValueError) as exc:
        typer.echo(
            f"error: cannot load Instrument Reference Data: {exc}", err=True
        )
        raise typer.Exit(code=1) from None

    selected = _select_archive_instruments(
        reference, instruments, venue, market_type
    )

    archive_instruments = [
        ArchiveInstrument(
            instrument_id=instrument.instrument_id,
            symbol=market_symbol(instrument.instrument_id),
            asset_class=instrument.asset_class,
        )
        for instrument in selected
    ]
    aggregator = BarAggregator(
        intervals=config.aggregation.intervals,
        base_interval=config.aggregation.base_interval,
        retention_parent_periods=config.aggregation.retention_parent_periods,
    )
    fetcher = ArchiveFetcher(
        base_url=base_url,
        archive_dir=archive_dir,
        timeout_s=timeout_s,
        retries=retries,
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
            feed=feed,
            market_type=market_type,
            interval=interval,
            granularity=granularity,
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
    if not report.ok:
        raise typer.Exit(code=1)
    return 0
