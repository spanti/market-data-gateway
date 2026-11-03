"""AST guard: the SDK must not import gateway internals or frameworks."""

from __future__ import annotations

import ast
from pathlib import Path

CLIENT_ROOT = Path(__file__).resolve().parents[2] / "src" / "mdgw_client"

FORBIDDEN_ROOTS = {
    "mdgw",  # the SDK is usable without the gateway package
    "ccxt",
    "aiohttp",
    "pydantic",
    "yaml",
    "nautilus_trader",
    "vnpy",
    "requests",  # REST uses urllib.request only
}


def _imported_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                # Relative imports stay inside `mdgw_client`.
                continue
            if node.module:
                roots.add(node.module.split(".")[0])
    return roots


def test_client_package_has_no_forbidden_imports() -> None:
    modules = sorted(CLIENT_ROOT.rglob("*.py"))
    assert modules, "mdgw_client package is missing"

    violations: list[str] = []
    for path in modules:
        for root in sorted(_imported_roots(path) & FORBIDDEN_ROOTS):
            violations.append(f"{path.relative_to(CLIENT_ROOT)}: {root}")
    assert violations == []
