# ruff: file-ignore[implicit-namespace-package]
"""Guard: the WebSocket hub must not depend on aiohttp types anymore."""

from __future__ import annotations

import ast
from pathlib import Path

WS_HUB = (
    Path(__file__).resolve().parents[2] / "src" / "mdgw" / "api" / "ws_hub.py"
)
FORBIDDEN_IMPORT_ROOT = "aiohttp"


def _tree() -> ast.Module:
    return ast.parse(WS_HUB.read_text(encoding="utf-8"), filename=str(WS_HUB))


def _imported_modules(tree: ast.Module) -> list[str]:
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.append(node.module)
    return modules


def _annotation_texts(tree: ast.Module) -> list[str]:
    texts: list[str] = []
    for node in ast.walk(tree):
        annotation = getattr(node, "annotation", None)
        if annotation is not None:
            texts.append(ast.unparse(annotation))
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.returns is not None
        ):
            texts.append(ast.unparse(node.returns))
    return texts


def test_ws_hub_does_not_import_aiohttp() -> None:
    imports = _imported_modules(_tree())
    offenders = [
        module
        for module in imports
        if module.split(".", 1)[0] == FORBIDDEN_IMPORT_ROOT
    ]
    assert offenders == []


def test_ws_hub_has_no_aiohttp_annotations() -> None:
    annotations = _annotation_texts(_tree())
    assert annotations  # a broken scan must not pass vacuously
    offenders = [
        text
        for text in annotations
        if "aiohttp" in text or "WebSocketResponse" in text
    ]
    assert offenders == []


def test_transport_protocol_declares_send_json_and_close() -> None:
    protocol = next(
        node
        for node in ast.walk(_tree())
        if isinstance(node, ast.ClassDef)
        and node.name == "WebSocketTransport"
    )
    methods = {
        node.name
        for node in protocol.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert {"send_json", "close"} <= methods
