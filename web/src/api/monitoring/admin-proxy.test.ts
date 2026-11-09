import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import type { CapabilityState } from "@/api/capabilities";
import {
  adminMonitoringAvailability,
  adminResponseToTrendSeries,
  buildAdminQueryParams,
  fetchAdminMonitoringTemplates,
  maxAdminSample,
  queryAdminMonitoring,
  type AdminMonitoringTemplate,
} from "@/api/monitoring/admin-proxy";
import { server } from "@/test/server";

const templates = {
  items: [
    {
      name: "http_qps",
      description: "HTTP request rate",
      unit: "requests_per_second",
      params: [
        {
          name: "service_role",
          type: "enum" as const,
          values: ["api"],
          required: false,
        },
      ],
      max_range_s: 86400,
      max_points: 2000,
      min_step: "15s",
    },
  ],
};

const template: AdminMonitoringTemplate = templates.items[0]!;

function adminCapability(
  state: "enabled" | "unsupported" | "adapter_unavailable",
): CapabilityState {
  return {
    mode: "admin-api",
    document: {
      server_time: "2026-11-09T12:00:00.000000000Z",
      version: { admin_api: "1.0.0", contract: 1, supported_contracts: [1] },
      auth: { required: true, roles: ["SuperAdmin", "Operator", "Auditor"] },
      adapters: [],
      capabilities: { monitoring_proxy: { state } },
    },
    compatible: true,
    lastSuccessAt: "2026-11-09T12:00:00.000Z",
  };
}

describe("monitoring proxy availability", () => {
  it("is enabled only in Admin API mode with an enabled monitoring_proxy capability", () => {
    expect(adminMonitoringAvailability(adminCapability("enabled"))).toEqual({
      kind: "enabled",
    });
  });

  it("names the disabled reason instead of falling back to direct Prometheus", () => {
    expect(adminMonitoringAvailability(adminCapability("unsupported"))).toEqual(
      {
        kind: "disabled",
        reason: { code: "backend-unsupported", message: "后端未实现" },
      },
    );
    expect(
      adminMonitoringAvailability(adminCapability("adapter_unavailable")),
    ).toEqual({
      kind: "disabled",
      reason: { code: "adapter-unavailable", message: "适配器不可用" },
    });
  });

  it("requires login when the Admin API exists but no session role is available", () => {
    const state: CapabilityState = {
      mode: "admin-api",
      document: null,
      compatible: null,
      lastSuccessAt: null,
    };
    expect(adminMonitoringAvailability(state)).toEqual({
      kind: "disabled",
      reason: { code: "requires-login", message: "需要登录" },
    });
  });

  it("is disabled outside Admin API mode", () => {
    expect(
      adminMonitoringAvailability({
        mode: "current-contract",
        lastSuccessAt: null,
      }),
    ).toMatchObject({ kind: "disabled" });
    expect(
      adminMonitoringAvailability({
        mode: "capabilities-unknown",
        lastSuccessAt: null,
        errorCode: "network_error",
        message: "offline",
      }),
    ).toMatchObject({
      kind: "disabled",
      reason: { code: "capabilities-unknown" },
    });
    expect(adminMonitoringAvailability(null)).toMatchObject({
      kind: "disabled",
    });
  });
});

describe("monitoring proxy contract target", () => {
  it("fetches the allowed template list from the authenticated Admin API", async () => {
    let path = "";
    server.use(
      http.get("*/api/v1/admin/monitoring/templates", ({ request }) => {
        path = new URL(request.url).pathname;
        return HttpResponse.json(templates);
      }),
    );

    const result = await fetchAdminMonitoringTemplates();

    expect(path).toBe("/api/v1/admin/monitoring/templates");
    expect(result[0]).toMatchObject({
      name: "http_qps",
      unit: "requests_per_second",
      min_step: "15s",
    });
  });

  it("sends only a declared template and declared parameters, never PromQL", async () => {
    let requestUrl = "";
    server.use(
      http.get("*/api/v1/admin/monitoring/query", ({ request }) => {
        requestUrl = request.url;
        return HttpResponse.json({
          template: "http_qps",
          unit: "requests_per_second",
          source: "prometheus",
          from: "2026-11-09T11:00:00.000000000Z",
          to: "2026-11-09T12:00:00.000000000Z",
          step: "30s",
          series: [
            {
              labels: { service_role: "api" },
              points: [
                { ts: "2026-11-09T11:00:00.000000000Z", value: 12.5 },
                { ts: "2026-11-09T11:00:30.000000000Z", value: null },
              ],
            },
          ],
          truncated: false,
          warnings: [],
        });
      }),
    );

    const response = await queryAdminMonitoring({
      template: "http_qps",
      params: buildAdminQueryParams(template, {
        service_role: "api",
        query: "up",
        expr: "up",
        promql: "up",
        undeclared: "x",
      }),
      from: "2026-11-09T11:00:00.000000000Z",
      to: "2026-11-09T12:00:00.000000000Z",
      step: "30s",
    });

    const url = new URL(requestUrl);
    expect(url.pathname).toBe("/api/v1/admin/monitoring/query");
    expect(url.searchParams.get("template")).toBe("http_qps");
    expect(url.searchParams.get("service_role")).toBe("api");
    for (const forbidden of ["query", "expr", "promql", "undeclared"]) {
      expect(url.searchParams.has(forbidden)).toBe(false);
    }
    expect(response.series[0]?.points[1]?.value).toBeNull();
  });

  it("preserves null values, truncation and warnings from the proxy", async () => {
    server.use(
      http.get("*/api/v1/admin/monitoring/query", () =>
        HttpResponse.json({
          template: "http_latency_p99",
          unit: "seconds",
          source: "prometheus",
          from: "2026-11-09T11:00:00.000000000Z",
          to: "2026-11-09T12:00:00.000000000Z",
          step: "30s",
          series: [
            {
              labels: { path: "/api/v1/bars" },
              points: [{ ts: "2026-11-09T11:00:00.000000000Z", value: null }],
            },
          ],
          truncated: true,
          warnings: ["max_points reached"],
        }),
      ),
    );

    const response = await queryAdminMonitoring({
      template: "http_latency_p99",
      from: "2026-11-09T11:00:00.000000000Z",
      to: "2026-11-09T12:00:00.000000000Z",
      step: "30s",
    });

    expect(response.truncated).toBe(true);
    expect(response.warnings).toEqual(["max_points reached"]);
    expect(response.series[0]?.points[0]?.value).toBeNull();
  });

  it("rejects a malformed proxy response instead of inventing points", async () => {
    server.use(
      http.get("*/api/v1/admin/monitoring/query", () =>
        HttpResponse.json({ template: "http_qps", series: "nope" }),
      ),
    );

    await expect(
      queryAdminMonitoring({
        template: "http_qps",
        from: "2026-11-09T11:00:00.000000000Z",
        to: "2026-11-09T12:00:00.000000000Z",
        step: "30s",
      }),
    ).rejects.toMatchObject({ code: "monitoring_proxy_malformed" });
  });

  it("maps proxy series to chart trends with millisecond timestamps", () => {
    const series = adminResponseToTrendSeries(
      {
        series: [
          {
            labels: { path: "/api/v1/bars" },
            points: [
              { ts: "2026-11-09T11:00:00.000000000Z", value: 3 },
              { ts: "2026-11-09T11:00:30.000000000Z", value: null },
            ],
          },
        ],
      },
      "QPS",
    );

    expect(series[0]?.name).toBe("QPS · /api/v1/bars");
    expect(series[0]?.points).toEqual([
      { ts: Date.parse("2026-11-09T11:00:00.000000000Z"), value: 3 },
      { ts: Date.parse("2026-11-09T11:00:30.000000000Z"), value: null },
    ]);
  });

  it("takes the peak only from finite proxy samples", () => {
    expect(
      maxAdminSample({
        series: [
          {
            labels: {},
            points: [
              { ts: "2026-11-09T11:00:00.000000000Z", value: null },
              { ts: "2026-11-09T11:00:30.000000000Z", value: 9.5 },
              { ts: "2026-11-09T11:01:00.000000000Z", value: 4 },
            ],
          },
        ],
      }),
    ).toBe(9.5);
    expect(
      maxAdminSample({
        series: [
          {
            labels: {},
            points: [{ ts: "2026-11-09T11:00:00.000000000Z", value: null }],
          },
        ],
      }),
    ).toBeNull();
  });
});
