"""Read Binance archive ZIPs and `.CHECKSUM` files from disk or HTTP.

`--archive-dir` makes the importer fully local: when it is set, no network
request is ever made. Without it, files are downloaded from `--base-url` with
a timeout and a small retry budget.

The local layout is the same layout `data.binance.vision` exposes:

    {archive_dir}/{market_type}/{granularity}/klines/{SYMBOL}/{interval}/{file}

A flat `{archive_dir}/{file}` fallback is supported so small test fixtures stay
easy to build. A missing ZIP is reported as `None` (a `missing_file`); a missing
`.CHECKSUM` is also `None`, but the importer treats that as a failed file.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from mdgw.archive.binance import (
    DEFAULT_BASE_URL,
    ArchivePeriod,
    archive_filename,
    archive_url,
)

if TYPE_CHECKING:
    from pathlib import Path


class ArchiveFetchError(RuntimeError):
    """A non-404 transport or local read error while fetching an archive file."""


@dataclass(frozen=True)
class ArchiveFetcher:
    base_url: str = DEFAULT_BASE_URL
    archive_dir: Path | None = None
    timeout_s: float = 30.0
    retries: int = 3

    def __post_init__(self) -> None:
        if self.timeout_s <= 0:
            msg = "timeout_s must be positive"
            raise ValueError(msg)
        if self.retries < 0:
            msg = "retries must not be negative"
            raise ValueError(msg)

    def read_zip(
        self,
        *,
        market_type: str,
        period: ArchivePeriod,
        symbol: str,
        interval: str,
    ) -> bytes | None:
        filename = archive_filename(symbol, interval, period)
        return self._read(filename, market_type, period, symbol, interval)

    def read_checksum(
        self,
        *,
        market_type: str,
        period: ArchivePeriod,
        symbol: str,
        interval: str,
    ) -> str | None:
        filename = archive_filename(symbol, interval, period) + ".CHECKSUM"
        data = self._read(filename, market_type, period, symbol, interval)
        if data is None:
            return None
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as exc:
            msg = f"CHECKSUM for {filename} is not valid UTF-8: {exc}"
            raise ArchiveFetchError(msg) from exc

    # -- internals ---------------------------------------------------------

    def _read(
        self,
        filename: str,
        market_type: str,
        period: ArchivePeriod,
        symbol: str,
        interval: str,
    ) -> bytes | None:
        if self.archive_dir is not None:
            return self._read_local(
                filename, market_type, period, symbol, interval
            )
        url = archive_url(
            self.base_url, market_type, symbol, interval, period
        ) + (".CHECKSUM" if filename.endswith(".CHECKSUM") else "")
        return self._download(url)

    def _read_local(
        self,
        filename: str,
        market_type: str,
        period: ArchivePeriod,
        symbol: str,
        interval: str,
    ) -> bytes | None:
        assert self.archive_dir is not None
        preferred = (
            self.archive_dir
            / market_type
            / period.granularity
            / "klines"
            / symbol
            / interval
            / filename
        )
        flat = self.archive_dir / filename
        for path in (preferred, flat):
            if path.is_file():
                try:
                    return path.read_bytes()
                except OSError as exc:
                    msg = f"cannot read archive file {path}: {exc}"
                    raise ArchiveFetchError(msg) from exc
        return None

    def _download(self, url: str) -> bytes | None:
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                # The URL is built from the configured archive base URL; archive
                # download is this module's documented purpose.
                request = Request(  # ruff: ignore[suspicious-url-open-usage]
                    url, headers={"User-Agent": "mdgw-archive/1"}
                )
                with urlopen(  # ruff: ignore[suspicious-url-open-usage]
                    request, timeout=self.timeout_s
                ) as response:
                    return response.read()
            except HTTPError as exc:
                if exc.code == 404:
                    return None
                last_error = exc
                if exc.code >= 500 and attempt < self.retries:
                    _backoff(attempt)
                    continue
                msg = f"HTTP {exc.code} while fetching {url}"
                raise ArchiveFetchError(msg) from exc
            except (URLError, TimeoutError, OSError) as exc:
                last_error = exc
                if attempt < self.retries:
                    _backoff(attempt)
                    continue
                msg = f"cannot fetch {url}: {exc}"
                raise ArchiveFetchError(msg) from exc
        msg = f"cannot fetch {url}: {last_error}"
        raise ArchiveFetchError(msg)


def _backoff(attempt: int) -> None:
    # Small bounded exponential backoff; tests never reach this path.
    time.sleep(min(0.1 * (2**attempt), 2.0))


__all__ = ["ArchiveFetchError", "ArchiveFetcher"]
