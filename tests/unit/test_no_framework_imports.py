from __future__ import annotations

import ast
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "src" / "mdgw"

FORBIDDEN_ROOTS = {
    "nautilus_trader",
    "vnpy",
    "backtrader",
    "backtesting",
    "zipline",
    "vectorbt",
}


def _imported_roots(tree: ast.AST) -> set[str]:
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            roots.add(node.module.split(".")[0])
    return roots


def test_gateway_package_does_not_import_trading_frameworks() -> None:
    violations: list[str] = []
    for path in PACKAGE_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        forbidden = _imported_roots(tree) & FORBIDDEN_ROOTS
        if forbidden:
            violations.append(f"{path.name}: {sorted(forbidden)}")

    assert violations == []
