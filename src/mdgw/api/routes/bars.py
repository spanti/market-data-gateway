"""Bar history endpoints (ticket 06 fills this router in place)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter

if TYPE_CHECKING:
    from mdgw.store.parquet_dataset import ParquetBarStore


def create_router(*, store: ParquetBarStore) -> APIRouter:
    """Placeholder router; Bar history routes are added by ticket 06."""
    _ = store
    return APIRouter()


__all__ = ["create_router"]
