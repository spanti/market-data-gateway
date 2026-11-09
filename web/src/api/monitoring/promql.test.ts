import { describe, expect, it } from "vitest";
import {
  buildLatencyQuantileQuery,
  buildPeakQpsQuery,
  buildQpsQuery,
  formatPrometheusDuration,
  LATENCY_QUANTILES,
} from "@/api/monitoring/promql";

describe("PromQL templates", () => {
  it("builds the documented HTTP QPS rate query", () => {
    expect(buildQpsQuery("5m")).toBe(
      "sum(rate(mdgw_api_http_requests_total[5m]))",
    );
  });

  it("builds peak QPS as max_over_time over a sampled 1m rate", () => {
    expect(
      buildPeakQpsQuery({
        lookbackMs: 60 * 60 * 1000,
        rateWindow: "1m",
        stepSeconds: 15,
      }),
    ).toBe(
      "max_over_time((sum(rate(mdgw_api_http_requests_total[1m])))[1h:15s])",
    );

    expect(
      buildPeakQpsQuery({
        lookbackMs: 24 * 60 * 60 * 1000,
        rateWindow: "1m",
        stepSeconds: 15,
      }),
    ).toBe(
      "max_over_time((sum(rate(mdgw_api_http_requests_total[1m])))[24h:15s])",
    );
  });

  it("builds the documented histogram_quantile queries", () => {
    expect(buildLatencyQuantileQuery(0.5, "5m")).toBe(
      "histogram_quantile(0.50, sum by (le) (rate(mdgw_api_http_request_duration_seconds_bucket[5m])))",
    );
    expect(buildLatencyQuantileQuery(0.95, "5m")).toBe(
      "histogram_quantile(0.95, sum by (le) (rate(mdgw_api_http_request_duration_seconds_bucket[5m])))",
    );
    expect(buildLatencyQuantileQuery(0.99, "5m")).toBe(
      "histogram_quantile(0.99, sum by (le) (rate(mdgw_api_http_request_duration_seconds_bucket[5m])))",
    );
  });

  it("keeps the percentile order fixed at P50, P95, P99", () => {
    expect(LATENCY_QUANTILES.map((item) => item.label)).toEqual([
      "P50",
      "P95",
      "P99",
    ]);
    expect(LATENCY_QUANTILES.map((item) => item.quantile)).toEqual([
      0.5, 0.95, 0.99,
    ]);
  });

  it("formats Prometheus durations without inventing sub-second steps", () => {
    expect(formatPrometheusDuration(15 * 60 * 1000)).toBe("15m");
    expect(formatPrometheusDuration(60 * 60 * 1000)).toBe("1h");
    expect(formatPrometheusDuration(6 * 60 * 60 * 1000)).toBe("6h");
    expect(formatPrometheusDuration(24 * 60 * 60 * 1000)).toBe("24h");
  });
});
