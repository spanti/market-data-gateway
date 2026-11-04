# ruff: file-ignore[implicit-namespace-package]
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


def test_histogram_renders_cumulative_buckets_sum_and_count() -> None:
    registry = MetricsRegistry()
    labels = {"method": "GET", "path": "/api/v1/bars", "status": "200"}
    registry.observe(
        "mdgw_api_http_request_duration_seconds",
        0.03125,
        labels=labels,
        help="HTTP request duration",
    )
    registry.observe(
        "mdgw_api_http_request_duration_seconds",
        0.0625,
        labels=labels,
    )

    text = registry.render()

    assert (
        "# HELP mdgw_api_http_request_duration_seconds"
        " HTTP request duration"
    ) in text
    assert (
        "# TYPE mdgw_api_http_request_duration_seconds histogram"
    ) in text
    series = (
        'method="GET",path="/api/v1/bars",status="200"'
    )
    name = "mdgw_api_http_request_duration_seconds"
    assert f'{name}_bucket{{{series},le="0.005"}} 0' in text
    assert f'{name}_bucket{{{series},le="0.01"}} 0' in text
    assert f'{name}_bucket{{{series},le="0.025"}} 0' in text
    assert f'{name}_bucket{{{series},le="0.05"}} 1' in text
    assert f'{name}_bucket{{{series},le="0.1"}} 2' in text
    assert f'{name}_bucket{{{series},le="0.25"}} 2' in text
    assert f'{name}_bucket{{{series},le="0.5"}} 2' in text
    assert f'{name}_bucket{{{series},le="1"}} 2' in text
    assert f'{name}_bucket{{{series},le="2.5"}} 2' in text
    assert f'{name}_bucket{{{series},le="5"}} 2' in text
    assert f'{name}_bucket{{{series},le="+Inf"}} 2' in text
    assert f"{name}_sum{{{series}}} 0.09375" in text
    assert f"{name}_count{{{series}}} 2" in text


def test_histogram_custom_buckets_are_sorted_and_include_positive_inf() -> None:
    registry = MetricsRegistry()
    registry.observe("mdgw_custom_seconds", 3.0, buckets=(2.0, 1.0))

    text = registry.render()

    assert 'mdgw_custom_seconds_bucket{le="1"} 0' in text
    assert 'mdgw_custom_seconds_bucket{le="2"} 0' in text
    assert 'mdgw_custom_seconds_bucket{le="+Inf"} 1' in text
    assert (
        text.index('mdgw_custom_seconds_bucket{le="1"}')
        < text.index('mdgw_custom_seconds_bucket{le="2"}')
        < text.index('mdgw_custom_seconds_bucket{le="+Inf"}')
    )


def test_histogram_label_values_are_escaped() -> None:
    registry = MetricsRegistry()
    registry.observe("mdgw_escaped_seconds", 0.001, labels={"value": 'a"b\\c'})

    text = registry.render()

    escaped = 'value="a\\"b\\\\c"'
    assert f'mdgw_escaped_seconds_bucket{{{escaped},le="0.005"}} 1' in text
    assert f"mdgw_escaped_seconds_sum{{{escaped}}} 0.001" in text
    assert f"mdgw_escaped_seconds_count{{{escaped}}} 1" in text


def test_concurrent_histogram_observes_are_not_lost() -> None:
    registry = MetricsRegistry()

    def observe_many() -> None:
        for _ in range(1000):
            registry.observe("mdgw_concurrent_seconds", 0.0625)

    threads = [threading.Thread(target=observe_many) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    text = registry.render()
    assert 'mdgw_concurrent_seconds_bucket{le="+Inf"} 8000' in text
    assert "mdgw_concurrent_seconds_sum 500" in text
    assert "mdgw_concurrent_seconds_count 8000" in text
