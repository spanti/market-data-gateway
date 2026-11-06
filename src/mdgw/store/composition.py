"""Storage Composition: one validated config to concrete Store objects.

This is an internal architecture module, not a domain concept. It owns the
production mapping from an already-validated ``GatewayConfig`` plus an
optional actual storage-root override to the concrete ``ParquetBarStore`` and
``QuarantineStore`` used by Service Roles and CLIs.

Composition is deliberate and on demand: each operation returns new objects
and nothing is cached. The module never owns object lifecycles, background
threads or a global registry, and it never builds a Feed Handler.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from mdgw.quarantine import QuarantineStore
from mdgw.store.parquet_dataset import ParquetBarStore

if TYPE_CHECKING:
    from mdgw.config import GatewayConfig


@dataclass(frozen=True)
class StorageBundle:
    """The Bars and Quarantine Stores created by one joint composition."""

    bars: ParquetBarStore
    quarantine: QuarantineStore


class StorageComposition:
    """Map a validated config and optional root override to Store objects."""

    def __init__(
        self,
        config: GatewayConfig,
        *,
        root: Path | None = None,
    ) -> None:
        self._config = config
        self._root = None if root is None else Path(root)

    @property
    def effective_root(self) -> Path:
        """The actual storage root: explicit override, else configured root."""
        if self._root is not None:
            return self._root
        return Path(self._config.storage.root)

    @property
    def bars_dataset_root(self) -> Path:
        return self.effective_root / "bars"

    @property
    def quarantine_root(self) -> Path:
        return self._config.storage.quarantine.resolved_root(self.effective_root)

    @property
    def rule_version(self) -> str:
        """The one effective Normalization Rule Version for this composition."""
        return self._config.storage.quarantine.rule_version

    def _check_independence(self) -> None:
        """Recheck ADR-0014 against the roots actually in effect.

        The config model validates the static storage root only; a root
        override, a ``..`` path fragment or a programmatically copied config
        can all change the effective layout. Normalized comparison does not
        require the paths to exist.
        """
        bars_root = self.bars_dataset_root.resolve()
        quarantine_root = self.quarantine_root.resolve()
        if quarantine_root == bars_root or bars_root in quarantine_root.parents:
            msg = (
                "storage.quarantine.root must not be inside the Bars Dataset "
                f"({bars_root})"
            )
            raise ValueError(msg)

    def _create_bars(self) -> ParquetBarStore:
        storage = self._config.storage
        return ParquetBarStore(
            self.effective_root,
            compression=storage.compression,
            row_group_size=storage.row_group_size,
            format_version=storage.format_version,
            schema_version=storage.schema_version,
            duckdb=storage.duckdb.to_options(),
            rule_versions=(self.rule_version,),
        )

    def _create_quarantine(self) -> QuarantineStore:
        # Physical format settings deliberately keep QuarantineStore defaults;
        # only the Dataset's own schema and retention come from config.
        quarantine = self._config.storage.quarantine
        return QuarantineStore(
            self.quarantine_root,
            schema_version=quarantine.schema_version,
            retention_days=quarantine.retention_days,
        )

    def bars(self) -> ParquetBarStore:
        """Compose a Bars Store for the actual storage root."""
        # ParquetBarStore creates the DuckDB temp directory eagerly, so the
        # independence check must run before construction.
        self._check_independence()
        return self._create_bars()

    def quarantine(self) -> QuarantineStore:
        """Compose a Quarantine Store for the actual layout."""
        self._check_independence()
        return self._create_quarantine()

    def bars_and_quarantine(self) -> StorageBundle:
        """Compose both Stores, validating the full layout first."""
        # Validate both roots before either constructor runs: the Bars Store
        # creates its DuckDB temp directory eagerly.
        self._check_independence()
        return StorageBundle(
            bars=self._create_bars(),
            quarantine=self._create_quarantine(),
        )


__all__ = ["StorageBundle", "StorageComposition"]
