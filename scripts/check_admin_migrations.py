#!/usr/bin/env python3
"""Reject Admin migrations added at or below the Git base schema version.

Run locally with ``--base <git-ref>`` before merging a ticket. In CI the base
is the PR target branch (or the previous push commit). The check is deliberately
stdlib-only so it also runs with ``python -I``.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable

MIGRATION_DIR = Path("src/mdgw/admin/migrations")
_NAME_RE = re.compile(r"^(?P<version>\d{3})_[a-z0-9_]+\.sql$")


def _versions(names: Iterable[str]) -> dict[int, str]:
    versions: dict[int, str] = {}
    for raw_name in names:
        name = Path(raw_name).name
        match = _NAME_RE.match(name)
        if match is None:
            msg = f"invalid admin migration file name: {name!r}"
            raise ValueError(msg)
        version = int(match.group("version"))
        if version in versions and versions[version] != name:
            msg = (
                f"duplicate admin migration version {version}: "
                f"{versions[version]!r} and {name!r}"
            )
            raise ValueError(msg)
        versions[version] = name
    return versions


def validate_added_migrations(
    current_names: Iterable[str], base_names: Iterable[str] = ()
) -> list[tuple[int, str]]:
    """Return added migrations, rejecting duplicates and late low versions."""
    current = _versions(current_names)
    base = _versions(base_names)
    base_file_names = set(base.values())
    added = sorted(
        (version, name)
        for version, name in current.items()
        if name not in base_file_names
    )
    if not added:
        return []
    highest_base = max(base, default=0)
    late = [(version, name) for version, name in added if version <= highest_base]
    if late:
        rendered = ", ".join(name for _version, name in late)
        msg = (
            "new admin migrations must be numbered above the base schema "
            f"({highest_base}): {rendered}"
        )
        raise ValueError(msg)
    return added


def _base_migration_names(ref: str) -> list[str]:
    executable = shutil.which("git")
    if executable is None:
        msg = "git executable not found"
        raise RuntimeError(msg)
    result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
        [executable, "ls-tree", "-r", "--name-only", ref, "--", str(MIGRATION_DIR)],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.splitlines()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base",
        help="Git ref whose existing migration versions new files must exceed",
    )
    args = parser.parse_args(argv)
    current = sorted(path.name for path in MIGRATION_DIR.glob("*.sql"))
    try:
        base = _base_migration_names(args.base) if args.base else []
        added = validate_added_migrations(current, base)
    except (ValueError, subprocess.CalledProcessError) as exc:
        print(f"admin migration gate failed: {exc}", file=sys.stderr)
        return 1
    print(
        "admin migration gate passed: "
        f"{len(current)} migration(s), {len(added)} new above the base"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
