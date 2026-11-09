# ruff: file-ignore[implicit-namespace-package]
"""Every Admin request model is frozen (T2).

Request bodies are immutable values: FastAPI parses them once and the use
cases must never mutate them while computing fingerprints, auditing or
persisting state.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from mdgw.admin.feed.flow_control import (
    ApiRateLimitUpdateBody,
    WsSlowConsumerUpdateBody,
)
from mdgw.admin.feed.service import (
    FeedSubscriptionCreateBody,
    FeedSubscriptionUpdateBody,
)
from mdgw.admin.lifecycle.routes import LifecycleActionBody

REQUEST_MODELS = (
    FeedSubscriptionCreateBody,
    FeedSubscriptionUpdateBody,
    ApiRateLimitUpdateBody,
    WsSlowConsumerUpdateBody,
    LifecycleActionBody,
)


@pytest.mark.parametrize("model", REQUEST_MODELS)
def test_admin_request_models_are_frozen(model) -> None:
    assert model.model_config.get("frozen") is True
    assert model.model_config.get("extra") == "forbid"


def test_a_frozen_lifecycle_body_rejects_mutation() -> None:
    body = LifecycleActionBody(reason="maintenance", confirm=True)

    with pytest.raises(ValidationError):
        body.reason = "changed"
