import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import {
  fetchLatencyTrends,
  fetchPeakQps,
  fetchQpsTrend,
} from "@/api/monitoring/direct";
import { server } from "@/test/server";

function matrix(points: Array<[number, string]>) {
  return {
    status: "success",
    data: {
      resultType: "matrix",
      result: [{ metric: {}, values: points }],
    },
  };
}

const window = {
  startMs: Date.parse("2026-11-09T11:00:00.000Z"),
  endMs: Date.parse("2026-11-09T12:00:00.000Z"),
  stepSeconds: 30,
};

describe("direct Prometheus adapter", () => {
  it("queries the fixed QPS template and maps samples to trend points", async () => {
    let requestUrl = "";
    server.use(
      http.get("*/prometheus/api/v1/query_range", ({ request }) => {
        requestUrl = request.url;
        return HttpResponse.json(
          matrix([
            [1794222000, "12.5"],
            [1794222030, "NaN"],
            [1794222060, "7"],
          ]),
        );
      }),
    );

    const series = await fetchQpsTrend(window, "5m");

    const url = new URL(requestUrl);
    expect(url.pathname).toBe("/prometheus/api/v1/query_range");
    expect(url.searchParams.get("query")).toBe(
      "sum(rate(mdgw_api_http_requests_total[5m]))",
    );
    expect(url.searchParams.get("start")).toBe("1794222000");
    expect(url.searchParams.get("end")).toBe("1794225600");
    expect(url.searchParams.get("step")).toBe("30s");
    expect(series).toHaveLength(1);
    expect(series[0]?.name).toBe("QPS");
    expect(series[0]?.points).toEqual([
      { ts: 1794222000000, value: 12.5 },
      { ts: 1794222030000, value: null },
      { ts: 1794222060000, value: 7 },
    ]);
  });

  it("queries peak QPS as an instant max_over_time and returns null with no samples", async () => {
    let requestUrl = "";
    server.use(
      http.get("*/prometheus/api/v1/query", ({ request }) => {
        requestUrl = request.url;
        return HttpResponse.json({
          status: "success",
          data: {
            resultType: "vector",
            result: [{ metric: {}, value: [1794225600, "18.25"] }],
          },
        });
      }),
    );

    const peak = await fetchPeakQps({
      endMs: window.endMs,
      lookbackMs: 60 * 60 * 1000,
      rateWindow: "1m",
      stepSeconds: 15,
    });

    const url = new URL(requestUrl);
    expect(url.pathname).toBe("/prometheus/api/v1/query");
    expect(url.searchParams.get("query")).toBe(
      "max_over_time((sum(rate(mdgw_api_http_requests_total[1m])))[1h:15s])",
    );
    expect(url.searchParams.get("time")).toBe("1794225600");
    expect(peak).toBe(18.25);
  });

  it("returns null peak when the source has no finite value", async () => {
    server.use(
      http.get("*/prometheus/api/v1/query", () =>
        HttpResponse.json({
          status: "success",
          data: { resultType: "vector", result: [] },
        }),
      ),
    );

    await expect(
      fetchPeakQps({
        endMs: window.endMs,
        lookbackMs: 60 * 60 * 1000,
        rateWindow: "1m",
        stepSeconds: 15,
      }),
    ).resolves.toBeNull();
  });

  it("queries P50/P95/P99 with the documented histogram templates", async () => {
    const queries: string[] = [];
    server.use(
      http.get("*/prometheus/api/v1/query_range", ({ request }) => {
        const url = new URL(request.url);
        const query = url.searchParams.get("query") ?? "";
        queries.push(query);
        const value = query.includes("0.50")
          ? "0.02"
          : query.includes("0.95")
            ? "0.25"
            : "2.5";
        return HttpResponse.json(
          matrix([[1794222000, value]]),
        );
      }),
    );

    const series = await fetchLatencyTrends(window, "5m");

    expect(queries).toEqual([
      "histogram_quantile(0.50, sum by (le) (rate(mdgw_api_http_request_duration_seconds_bucket[5m])))",
      "histogram_quantile(0.95, sum by (le) (rate(mdgw_api_http_request_duration_seconds_bucket[5m])))",
      "histogram_quantile(0.99, sum by (le) (rate(mdgw_api_http_request_duration_seconds_bucket[5m])))",
    ]);
    expect(series.map((item) => item.name)).toEqual(["P50", "P95", "P99"]);
  });

  it("propagates a Prometheus error instead of fabricating a zero", async () => {
    server.use(
      http.get("*/prometheus/api/v1/query_range", () =>
        HttpResponse.json(
          { status: "error", errorType: "bad_data", error: "parse error" },
          { status: 400 },
        ),
      ),
    );

    await expect(fetchQpsTrend(window, "5m")).rejects.toMatchObject({
      code: "prometheus_error",
    });
  });
});
