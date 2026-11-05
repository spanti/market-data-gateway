"""Normalization Rule Versions: audit identity of the rules that produced data.

This module is the single source of truth for Normalization Rule Versions. It
is deliberately separate from the Data Contract Version: a Rule Version says
which Normalization rules produced a batch or rejected an input, while the Data
Contract Version decides whether a reader understands the Bar Schema. A rule
change that alters public Bar semantics must raise the Data Contract Version,
not merely this value.
"""

from __future__ import annotations

from typing import Any

# The single Polars batch Normalization rule source
# (``mdgw.normalize.batch``), used by live ingest, Backfill and archive import.
NORMALIZATION_RULE_VERSION = "polars-1"

NORMALIZATION_RULE_VERSIONS: frozenset[str] = frozenset(
    {NORMALIZATION_RULE_VERSION}
)


def normalize_rule_versions(value: Any) -> tuple[str, ...]:
    """Return a sorted, de-duplicated tuple of non-empty Rule Versions.

    Missing or unrecognized metadata yields an empty tuple, which means
    "legacy/unknown". This is a read-path compatibility rule: an old Manifest
    entry must stay readable, and no historical version is ever invented.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        candidates: Any = (value,)
    elif isinstance(value, (list, tuple, set, frozenset)):
        candidates = value
    else:
        return ()
    cleaned = {
        candidate.strip()
        for candidate in candidates
        if isinstance(candidate, str) and candidate.strip()
    }
    return tuple(sorted(cleaned))


__all__ = [
    "NORMALIZATION_RULE_VERSION",
    "NORMALIZATION_RULE_VERSIONS",
    "normalize_rule_versions",
]
