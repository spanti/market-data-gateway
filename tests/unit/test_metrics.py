from __future__ import annotations

import threading

from mdgw.metrics import MetricsRegistry


def test_counter_and_gauge_render_in_prometheus_format() -> None:
    registry = MetricsRegistry()
    registry.inc(
        "mdgw_api_http_requests_total",
        labels={"method": "GET", "path": "/api/v1/bars", "status": "200"},
        help="HTTP requests",
    )
    registry.inc(
        "mdgw_api_http_requests_total",
        labels={"method": "GET", "path": "/api/v1/bars", "status": "200"},
    )
    registry.set("mdgw_up", 1, help="Process is up")

    text = registry.render()

    assert "# HELP mdgw_up Process is up" in text
    assert "# TYPE mdgw_up gauge" in text
    assert "mdgw_up 1" in text
    assert "# TYPE mdgw_api_http_requests_total counter" in text
    assert (
        'mdgw_api_http_requests_total{method="GET",'
        'path="/api/v1/bars",status="200"} 2'
    ) in text


def test_concurrent_increments_are_not_lost() -> None:
    registry = MetricsRegistry()
    threads = [
        threading.Thread(
            target=lambda: [registry.inc("mdgw_test_total") for _ in range(1000)]
        )
        for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert registry.get("mdgw_test_total") == 8000


def test_label_values_are_escaped() -> None:
    registry = MetricsRegistry()
    registry.set("mdgw_labelled", 1, labels={"value": 'a"b\\c'})

    assert 'value="a\\"b\\\\c"' in registry.render()
