"""Instrument Reference Data endpoints backed by the read-only snapshot."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter
from starlette.responses import JSONResponse

from mdgw.core.errors import (
    InvalidInstrumentError,
    InvalidParameterError,
    UnknownInstrumentError,
)
from mdgw.core.instrument import InstrumentId
from mdgw.reference import instrument_to_json

if TYPE_CHECKING:
    from mdgw.reference import InstrumentStore

_TRUE_VALUES = frozenset({"true", "1"})
_FALSE_VALUES = frozenset({"false", "0", ""})


def create_router(*, instrument_store: InstrumentStore | None) -> APIRouter:
    """Build the Instrument router for one application instance."""
    router = APIRouter()

    @router.get("/api/v1/instruments")
    async def list_instruments(
        venue: str | None = None,
        asset_class: str | None = None,
        include_delisted: str | None = None,
    ) -> JSONResponse:
        flag = _parse_include_delisted(include_delisted)
        instruments = (
            []
            if instrument_store is None
            else instrument_store.list(
                venue=venue,
                asset_class=asset_class,
                include_delisted=flag,
            )
        )
        return JSONResponse(
            {"instruments": [instrument_to_json(item) for item in instruments]}
        )

    @router.get("/api/v1/instruments/{instrument_id:path}")
    async def get_instrument(instrument_id: str) -> JSONResponse:
        try:
            InstrumentId.parse(instrument_id)
        except ValueError:
            message = "path parameter 'instrument_id' must be an InstrumentId"
            raise InvalidInstrumentError(message) from None

        instrument = (
            None
            if instrument_store is None
            else instrument_store.get(instrument_id)
        )
        if instrument is None:
            message = f"unknown Instrument {instrument_id!r}"
            raise UnknownInstrumentError(message)
        return JSONResponse({"instrument": instrument_to_json(instrument)})

    return router


def _parse_include_delisted(raw: str | None) -> bool:
    value = "false" if raw is None else raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    message = "query parameter 'include_delisted' must be a boolean"
    raise InvalidParameterError(message)


__all__ = ["create_router"]
