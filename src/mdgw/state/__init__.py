"""Collector process state that must survive a restart.

The High-Water Mark lives in a small SQLite database under the data root, next
to the Parquet Dataset but outside it. It is deliberately not part of the Data
Contract: it is operational state, and losing it only forces a wider backfill.
"""

from mdgw.state.health import GapEvent, HealthTracker
from mdgw.state.high_water_mark import HighWaterMarkStore

__all__ = ["GapEvent", "HealthTracker", "HighWaterMarkStore"]
