"""Validation for same-origin relative redirect paths.

Both OIDC configuration (``default_return_to`` / ``error_redirect_path``) and
the login ``return_to`` query parameter must accept only a same-origin
relative path. Keeping one validator here prevents the two call sites from
drifting on what they reject.
"""

from __future__ import annotations

MAX_RELATIVE_PATH_LENGTH = 2_048


def validate_relative_path(value: str, *, field: str = "path") -> str:
    """Return ``value`` when it is a safe relative path, else raise ValueError.

    Rejected forms: absolute URLs, protocol-relative ``//host`` paths, any
    backslash (some browsers normalize it into a separator) and control
    characters. The message names ``field`` so pydantic validators can reuse
    the same wording.
    """
    if not isinstance(value, str):
        msg = f"{field} must be a string"
        # Pydantic validators (the config call site) signal bad input as
        # ValueError; the session call site wraps it in its domain error.
        raise ValueError(msg)  # ruff: ignore[type-check-without-type-error]
    if len(value) > MAX_RELATIVE_PATH_LENGTH:
        msg = f"{field} is too long"
        raise ValueError(msg)
    if not value.startswith("/") or value.startswith("//"):
        msg = f"{field} must be a same-origin relative path starting with '/'"
        raise ValueError(msg)
    if "\\" in value or any(
        ord(character) < 0x20 or ord(character) == 0x7F for character in value
    ):
        msg = f"{field} must not contain control characters or backslashes"
        raise ValueError(msg)
    return value


__all__ = ["MAX_RELATIVE_PATH_LENGTH", "validate_relative_path"]
