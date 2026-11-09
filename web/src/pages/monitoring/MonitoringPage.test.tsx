import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import type { CollectorHealth } from "@/api/endpoints/ops";
import { MONITORING_QUERY_KEYS } from "@/pages/monitoring/use-monitoring-data";
import { renderApp } from "@/test/render-app";
import { server } from "@/test/server";

const iso = (ms: number) => new Date(ms).toISOString();

function kpiCell(label: string): HTMLElement {
  const cell = document.querySelector<HTMLElement>(
    `[data-slot="kpi-cell"][data-kpi-label="${label}"]`,
  );
  if (!cell) {
    throw new Error(`KPI cell not found: ${label}`);
  }
  return cell;
}

async function expectKpiValue(label: string, value: string) {
  await waitFor(() => {
    expect(within(kpiCell(label)).getByText(value)).toBeInTheDocument();
  });
}

function datasetStatus(overrides: Record<string, unknown> = {}) {
  const lastFlushed = iso(Date.now() - 60_000);
  return {
    dataset: "bars",
    schema_version: 1,
    last_flushed_ts: lastFlushed,
    active_file_count: 1,
    median_file_size_bytes: 500,
    intervals: {
      "1m": {
        last_flushed_ts: lastFlushed,
        active_file_count: 1,
        median_file_size_bytes: 500,
      },
    },
    ...overrides,
  };
}

function apiStatus(overrides: Record<string, unknown> = {}) {
  return {
    ...datasetStatus(),
    service: "api",
    role: "api",
    uptime_s: 120,
    http: { host: "127.0.0.1", port: 8080 },
    zmq: {
      publish_endpoint: "tcp://127.0.0.1:5556",
      collector_endpoint: "tcp://127.0.0.1:5555",
    },
    messages_received: 7,
    messages_forwarded: 7,
    collector: { ready: true, last_bar_ts: null, status: null },
    ...overrides,
  };
}

function collectorStatus(overrides: Record<string, unknown> = {}) {
  const health: CollectorHealth = {
    tracked: [
      {
        feed: "testfeed",
        instrument_id: "venue-a:AAA/USD",
        interval: "1m",
        last_event_ts: iso(Date.now() - 20_000),
        age_s: 20,
        threshold_s: 120,
        stale: false,
      },
      {
        feed: "testfeed",
        instrument_id: "venue-a:BBB/USD",
        interval: "5m",
        last_event_ts: iso(Date.now() - 200_000),
        age_s: 200,
        threshold_s: 120,
        stale: true,
      },
    ],
    stale_count: 1,
    stale: [],
    gap_unrecoverable: [],
    gap_unrecoverable_count: 0,
  };
  health.stale = health.tracked.filter((entry) => entry.stale);
  return {
    service: "collector",
    role: "collector",
    ready: true,
    uptime_s: 60,
    feed: "testfeed",
    last_ingest_ts: iso(Date.now() - 20_000),
    last_bar_ts: iso(Date.now() - 20_000),
    bars_ingested: 12,
    bars_written: 4,
    stale_count: 1,
    gap_unrecoverable_count: 0,
    write_queue_depth: 0,
    write_dropped_bars: 0,
    write_lag_bars: 0,
    write_last_flush_ts: iso(Date.now() - 20_000),
    write_last_flush_duration_s: 0.01,
    write_sink: {
      mode: "buffered",
      queue_depth: 0,
      queue_capacity: 100,
      pending_bytes: 0,
      dropped_bars: 0,
      lag_bars: 0,
      dropped_identities: 0,
      flushes: 1,
      flush_failures: 0,
      persisted_bars: 4,
      last_flush_ts: iso(Date.now() - 20_000),
      last_flush_duration_s: 0.01,
    },
    quarantine: {
      schema_version: 1,
      root: "/tmp/quarantine",
      retention_days: 30,
      records_written: 0,
      write_failures: 0,
      last_write_ts: null,
      active_file_count: 0,
      active_bytes: 0,
      gc_runs: 0,
      last_gc: null,
      records_by_source_stage: {},
      records_by_reason_code: {},
      records_by_stage_and_reason: [],
    },
    health,
    ...overrides,
  };
}

function replayStatus(overrides: Record<string, unknown> = {}) {
  return {
    service: "replay",
    role: "replay",
    uptime_s: 30,
    control_endpoint: "tcp://127.0.0.1:5557",
    publish_endpoint: "tcp://127.0.0.1:5558",
    active_sessions: [],
    sessions: [],
    instrument_snapshot: {
      status: "loaded",
      loaded: true,
      snapshot: "/tmp/snapshot.parquet",
      count: 2,
    },
    ...overrides,
  };
}

function instruments() {
  const base = {
    asset_class: "crypto",
    instrument_type: "spot",
    base: "AAA",
    quote: "USD",
    tick_size: "0.01",
    lot_size: "0.001",
    multiplier: "1.0",
    timezone: "UTC",
    listing_ts: "2024-01-01T00:00:00.000000000Z",
    delisted_ts: null,
  };
  return {
    instruments: [
      {
        ...base,
        instrument_id: "venue-a:AAA/USD",
        venue: "venue-a",
        symbol: "AAA/USD",
      },
      {
        ...base,
        instrument_id: "venue-a:BBB/USD",
        venue: "venue-a",
        symbol: "BBB/USD",
      },
      {
        ...base,
        instrument_id: "venue-b:CCC/USD",
        venue: "venue-b",
        symbol: "CCC/USD",
        asset_class: "futures",
        instrument_type: "perp",
        delisted_ts: "2026-01-01T00:00:00.000000000Z",
      },
    ],
  };
}

function prometheusMatrix(
  values: Array<[number, number | string]>,
) {
  return {
    status: "success",
    data: {
      resultType: "matrix",
      result: [{ metric: {}, values }],
    },
  };
}

function installSources(
  overrides: {
    api?: Record<string, unknown>;
    collector?: Record<string, unknown>;
    replay?: Record<string, unknown>;
    dataset?: Record<string, unknown>;
    instrumentList?: Record<string, unknown>;
    ready?: { api?: boolean; collector?: boolean; replay?: boolean };
    prometheusRange?: (query: string) => Record<string, unknown>;
    prometheusInstant?: (query: string) => Record<string, unknown>;
    onPrometheusRequest?: (url: string) => void;
  } = {},
) {
  const now = Math.floor(Date.now() / 1000);
  server.use(
    http.get("*/status", ({ request }) => {
      const path = new URL(request.url).pathname;
      if (path === "/api/v1/status") {
        return HttpResponse.json(overrides.dataset ?? datasetStatus());
      }
      if (path === "/ops/collector/status") {
        return HttpResponse.json(overrides.collector ?? collectorStatus());
      }
      if (path === "/ops/replay/status") {
        return HttpResponse.json(overrides.replay ?? replayStatus());
      }
      return HttpResponse.json(overrides.api ?? apiStatus());
    }),
    http.get("*/readyz", ({ request }) => {
      const path = new URL(request.url).pathname;
      const ready =
        path === "/ops/collector/readyz"
          ? (overrides.ready?.collector ?? true)
          : path === "/ops/replay/readyz"
            ? (overrides.ready?.replay ?? true)
            : (overrides.ready?.api ?? true);
      return HttpResponse.json({
        ready,
        reason: ready ? "ready" : "starting",
        ...(path === "/ops/collector/readyz" ? { service: "collector" } : {}),
        ...(path === "/ops/replay/readyz" ? { service: "replay" } : {}),
      });
    }),
    http.get("*/api/v1/instruments", () =>
      HttpResponse.json(overrides.instrumentList ?? instruments()),
    ),
    http.get("*/prometheus/api/v1/query_range", ({ request }) => {
      const url = new URL(request.url);
      overrides.onPrometheusRequest?.(url.toString());
      const query = url.searchParams.get("query") ?? "";
      if (overrides.prometheusRange) {
        return HttpResponse.json(overrides.prometheusRange(query));
      }
      if (query.includes("histogram_quantile(0.50")) {
        return HttpResponse.json(prometheusMatrix([[now - 30, "0.02"]]));
      }
      if (query.includes("histogram_quantile(0.95")) {
        return HttpResponse.json(prometheusMatrix([[now - 30, "0.25"]]));
      }
      if (query.includes("histogram_quantile(0.99")) {
        return HttpResponse.json(prometheusMatrix([[now - 30, "0.5"]]));
      }
      return HttpResponse.json(
        prometheusMatrix([
          [now - 30, "10"],
          [now, "12.5"],
        ]),
      );
    }),
    http.get("*/prometheus/api/v1/query", ({ request }) => {
      const url = new URL(request.url);
      overrides.onPrometheusRequest?.(url.toString());
      if (overrides.prometheusInstant) {
        return HttpResponse.json(overrides.prometheusInstant(url.searchParams.get("query") ?? ""));
      }
      return HttpResponse.json({
        status: "success",
        data: {
          resultType: "vector",
          result: [{ metric: {}, value: [now, "18.25"] }],
        },
      });
    }),
  );
}

describe("Monitoring dashboard", () => {
  it("renders real status, freshness, dataset, venue and trend data", async () => {
    installSources();
    renderApp();

    expect(
      await screen.findByRole("heading", { name: "监控分析工作台" }),
    ).toBeInTheDocument();

    // QPS, Peak QPS and P99 KPI values come from Prometheus.
    await expectKpiValue("Realtime QPS", "12.5");
    expect(within(kpiCell("Peak QPS")).getByText("18.25")).toBeInTheDocument();
    expect(within(kpiCell("HTTP P99")).getByText("500")).toBeInTheDocument();
    expect(within(kpiCell("HTTP P99")).getByText("ms")).toBeInTheDocument();
    expect(screen.getByText(/P95 250 ms · P50 20 ms/)).toBeInTheDocument();
    expect(screen.getAllByText(/含完整响应流耗时/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/rate 5m/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/最近 1 小时/).length).toBeGreaterThan(0);

    // Feed Freshness uses server age/threshold/stale with a countdown.
    expect(screen.getByText(/剩余 100s/)).toBeInTheDocument();
    expect(screen.getAllByText("失活").length).toBeGreaterThan(0);
    expect(screen.getAllByText("正常").length).toBeGreaterThan(0);

    // Dataset Freshness is a separate visualization.
    expect(
      screen.getByRole("heading", { name: "Dataset Freshness" }),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/独立于 Feed Freshness/),
    ).toBeInTheDocument();

    // Service instances are only collector/api/replay.
    const ledger = screen.getByRole("region", {
      name: "Service Instance ledger",
    });
    expect(ledger).toHaveTextContent("collector");
    expect(ledger).toHaveTextContent("api");
    expect(ledger).toHaveTextContent("replay");
    expect(ledger).not.toHaveTextContent(/master|worker|leader|failover/i);

    // Venues are derived from real instruments.
    expect(screen.getByText("venue-a")).toBeInTheDocument();
    expect(screen.getByText("venue-b")).toBeInTheDocument();
    expect(screen.queryByText(/binance|okx|fix/i)).not.toBeInTheDocument();

    // Unsupported connection fields are explicit capability gaps.
    expect(screen.getAllByText("未暴露").length).toBeGreaterThan(0);
    expect(
      screen.getByText(/不证明当前连接健康/),
    ).toBeInTheDocument();

    // Every chart has a data-table fallback.
    expect(
      screen.getByText(/查看数据表（2 个时间点）/),
    ).toBeInTheDocument();

    // Per-source metadata: source, window, last successful update.
    expect(
      screen.getAllByText(/数据源：Service \/status/).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByText(/数据源：Dataset \/api\/v1\/status/).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByText(/数据源：Prometheus（\/prometheus）/).length,
    ).toBeGreaterThan(0);
    expect(screen.getAllByText(/最后成功/).length).toBeGreaterThanOrEqual(5);
  });

  it("shows a stale entry as failed and never as healthy", async () => {
    installSources({
      collector: collectorStatus({
        health: {
          tracked: [
            {
              feed: "testfeed",
              instrument_id: "venue-a:AAA/USD",
              interval: "1m",
              last_event_ts: iso(Date.now() - 200_000),
              age_s: 200,
              threshold_s: 120,
              stale: true,
            },
          ],
          stale: [],
          stale_count: 1,
          gap_unrecoverable: [],
          gap_unrecoverable_count: 0,
        },
      }),
    });
    renderApp();

    await waitFor(() => {
      const feed = screen.getByRole("region", { name: "Feed Freshness" });
      expect(within(feed).getByText("失活")).toBeInTheDocument();
      expect(within(feed).queryByText("正常")).not.toBeInTheDocument();
    });
  });

  it("moves to unknown with the last success time when a source fails", async () => {
    installSources();
    const { queryClient } = renderApp();

    expect(await screen.findByText(/剩余 100s/)).toBeInTheDocument();

    server.use(
      http.get("*/ops/collector/status", () =>
        HttpResponse.json(
          {
            error: {
              code: "internal_error",
              message: "collector status failed",
              details: {},
              request_id: "collector-boom",
            },
          },
          { status: 500 },
        ),
      ),
    );
    await queryClient.refetchQueries({
      queryKey: MONITORING_QUERY_KEYS.collectorStatus,
    });

    expect(
      await screen.findByText(/Collector \/status 加载失败/),
    ).toBeInTheDocument();
    expect(screen.queryByText(/剩余 100s/)).not.toBeInTheDocument();
    expect(screen.getAllByText(/最后成功/).length).toBeGreaterThan(0);

    const ledger = screen.getByRole("region", {
      name: "Service Instance ledger",
    });
    expect(within(ledger).getByText(/状态未知/)).toBeInTheDocument();
    expect(
      within(ledger).getAllByText(/Uptime —/).length,
    ).toBeGreaterThan(0);
  });

  it("marks trend KPIs unknown when Prometheus fails, with the last success", async () => {
    installSources();
    const { queryClient } = renderApp();

    await expectKpiValue("Realtime QPS", "12.5");

    server.use(
      http.get("*/prometheus/api/v1/query_range", () =>
        HttpResponse.json({ status: "error", error: "upstream down" }),
      ),
      http.get("*/prometheus/api/v1/query", () =>
        HttpResponse.json({ status: "error", error: "upstream down" }),
      ),
    );
    await queryClient.refetchQueries({
      queryKey: MONITORING_QUERY_KEYS.qpsTrend,
    });

    expect((await screen.findAllByText("能力未知")).length).toBeGreaterThan(0);
    expect(within(kpiCell("Realtime QPS")).queryByText("12.5")).not.toBeInTheDocument();
    expect(screen.getAllByText(/最后成功/).length).toBeGreaterThan(0);
  });

  it("shows explicit empty states without generating health records", async () => {
    installSources({
      collector: collectorStatus({
        health: {
          tracked: [],
          stale: [],
          stale_count: 0,
          gap_unrecoverable: [],
          gap_unrecoverable_count: 0,
        },
      }),
      dataset: datasetStatus({ intervals: {}, last_flushed_ts: null }),
      instrumentList: { instruments: [] },
    });
    renderApp();

    expect(
      await screen.findByText("暂无 Feed Freshness 数据"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("暂无 Dataset Freshness 数据"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("暂无 Instrument Reference 数据"),
    ).toBeInTheDocument();
  });

  it("drives every trend from one range control and labels the peak range", async () => {
    const requestUrls: string[] = [];
    installSources({
      onPrometheusRequest: (url) => requestUrls.push(url),
    });
    const user = userEvent.setup();
    renderApp();

    const group = await screen.findByRole("radiogroup", {
      name: "统一监控时间范围",
    });
    expect(
      await screen.findByText(/默认最近 1 小时/),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("radio", { name: "1 小时" }),
    ).toHaveAttribute("aria-checked", "true");

    await user.click(screen.getByRole("radio", { name: "6 小时" }));

    expect(
      screen.getByRole("radio", { name: "6 小时" }),
    ).toHaveAttribute("aria-checked", "true");
    expect(group).toBeInTheDocument();

    await waitFor(() => {
      const qpsUrls = requestUrls.filter((url) =>
        url.includes("mdgw_api_http_requests_total"),
      );
      expect(qpsUrls.length).toBeGreaterThan(0);
      const ranges = qpsUrls.map((url) => {
        const params = new URL(url).searchParams;
        return Number(params.get("end")) - Number(params.get("start"));
      });
      expect(Math.max(...ranges)).toBeGreaterThanOrEqual(6 * 60 * 60 - 5);
    });
    await waitFor(() => {
      const peakQueries = requestUrls
        .map((url) => new URL(url).searchParams.get("query") ?? "")
        .filter((query) => query.startsWith("max_over_time"));
      expect(peakQueries.some((query) => query.includes("[6h:15s]"))).toBe(
        true,
      );
    });
  });
});

describe("monitoring runtime-mode gating", () => {
  const capabilityDocument = {
    schema_version: 1,
    server_time: "2026-11-09T12:00:00.000000000Z",
    version: { admin_api: "1.0.0", contract: 1, supported_contracts: [1] },
    auth: {
      required: true,
      mode: "oidc",
      roles: ["SuperAdmin", "Operator", "Auditor"],
    },
    adapters: [],
    capabilities: {
      monitoring_proxy: { state: "enabled" },
    },
  };

  const adminTemplates = {
    items: [
      { name: "http_qps", description: "QPS", unit: "requests_per_second", params: [], max_range_s: 86400, max_points: 2000, min_step: "15s" },
      { name: "http_peak_qps", description: "Peak", unit: "requests_per_second", params: [], max_range_s: 86400, max_points: 2000, min_step: "15s" },
      { name: "http_latency_p50", description: "P50", unit: "seconds", params: [], max_range_s: 86400, max_points: 2000, min_step: "15s" },
      { name: "http_latency_p95", description: "P95", unit: "seconds", params: [], max_range_s: 86400, max_points: 2000, min_step: "15s" },
      { name: "http_latency_p99", description: "P99", unit: "seconds", params: [], max_range_s: 86400, max_points: 2000, min_step: "15s" },
    ],
  };

  function installAdmin(role: "Operator" | "Auditor" = "Operator") {
    const queryUrls: string[] = [];
    let prometheusCalls = 0;
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(capabilityDocument),
      ),
      http.get("*/api/v1/admin/auth/session", () =>
        HttpResponse.json({
          authenticated: true,
          server_time: "2026-11-09T12:00:00.000000000Z",
          user: {
            subject: "subject-1",
            display_name: "Test User",
            email: null,
          },
          access_role: role,
          session: null,
          csrf: null,
        }),
      ),
      http.get("*/api/v1/admin/monitoring/templates", () =>
        HttpResponse.json(adminTemplates),
      ),
      http.get("*/api/v1/admin/monitoring/query", ({ request }) => {
        const url = new URL(request.url);
        queryUrls.push(url.toString());
        const template = url.searchParams.get("template") ?? "";
        const now = Date.now();
        const unit = template.startsWith("http_latency") ? "seconds" : "requests_per_second";
        const value = template === "http_peak_qps" ? 18.25 : template === "http_latency_p99" ? 0.5 : template === "http_latency_p95" ? 0.25 : template === "http_latency_p50" ? 0.02 : 12.5;
        return HttpResponse.json({
          template,
          unit,
          source: "prometheus",
          from: iso(now - 60 * 60 * 1000),
          to: iso(now),
          step: "30s",
          series: [
            {
              labels: {},
              points: [{ ts: iso(now), value }],
            },
          ],
          truncated: false,
          warnings: [],
        });
      }),
      http.get("*/prometheus/api/v1/query_range", () => {
        prometheusCalls += 1;
        return HttpResponse.json({ status: "success", data: { resultType: "matrix", result: [] } });
      }),
      http.get("*/prometheus/api/v1/query", () => {
        prometheusCalls += 1;
        return HttpResponse.json({ status: "success", data: { resultType: "vector", result: [] } });
      }),
    );
    return { queryUrls, prometheusCalls: () => prometheusCalls };
  }

  it("uses only the authenticated template proxy in Admin API mode", async () => {
    const capture = installAdmin("Operator");
    renderApp();

    await expectKpiValue("Realtime QPS", "12.5");
    expect(within(kpiCell("Peak QPS")).getByText("18.25")).toBeInTheDocument();
    expect(within(kpiCell("HTTP P99")).getByText("500")).toBeInTheDocument();
    expect(within(kpiCell("HTTP P99")).getByText("ms")).toBeInTheDocument();
    expect(
      screen.getAllByText(/数据源：Admin API 监控代理/).length,
    ).toBeGreaterThan(0);

    expect(capture.prometheusCalls()).toBe(0);
    expect(capture.queryUrls.length).toBeGreaterThanOrEqual(5);
    for (const url of capture.queryUrls) {
      const params = new URL(url).searchParams;
      expect(params.get("template")).toBeTruthy();
      expect(params.has("query")).toBe(false);
      expect(params.has("expr")).toBe(false);
      expect(params.has("promql")).toBe(false);
    }
  });

  it("shows the disabled reason when the monitoring proxy is unsupported", async () => {
    installAdmin();
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json({
          ...capabilityDocument,
          capabilities: { monitoring_proxy: { state: "unsupported" } },
        }),
      ),
    );
    renderApp();

    expect(
      (await screen.findAllByText(/后端未实现/)).length,
    ).toBeGreaterThan(0);
    expect(screen.getAllByText("能力未知").length).toBeGreaterThan(0);
    expect(within(kpiCell("Realtime QPS")).queryByText("12.5")).not.toBeInTheDocument();
  });

  it("requires login instead of falling back to direct Prometheus when unauthenticated", async () => {
    let prometheusCalls = 0;
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(capabilityDocument),
      ),
      http.get("*/api/v1/admin/auth/session", () =>
        HttpResponse.json(
          {
            error: {
              code: "unauthorized",
              message: "login required",
              details: {},
              request_id: "session-required",
            },
          },
          { status: 401 },
        ),
      ),
      http.get("*/prometheus/api/v1/query_range", () => {
        prometheusCalls += 1;
        return HttpResponse.json({ status: "success", data: { resultType: "matrix", result: [] } });
      }),
    );
    renderApp();

    expect((await screen.findAllByText(/需要登录/)).length).toBeGreaterThan(0);
    expect(prometheusCalls).toBe(0);
  });

  it("keeps monitoring visible for the Auditor role", async () => {
    installAdmin("Auditor");
    renderApp();

    expect(
      await screen.findByRole("heading", { name: "监控分析工作台" }),
    ).toBeInTheDocument();
    await expectKpiValue("Realtime QPS", "12.5");
  });

  it("never reads unauthenticated ops endpoints in Admin mode and shows explicit capability gaps", async () => {
    installAdmin("Operator");
    let opsRequested = false;
    server.use(
      http.get("*/status", ({ request }) => {
        if (new URL(request.url).pathname === "/status") {
          opsRequested = true;
        }
        return HttpResponse.json(apiStatus());
      }),
      http.get("*/readyz", () => {
        opsRequested = true;
        return HttpResponse.json({ ready: true, reason: "unexpected" });
      }),
      http.get("*/ops/*", () => {
        opsRequested = true;
        return HttpResponse.json({});
      }),
    );
    renderApp();

    expect(
      await screen.findByRole("heading", { name: "监控分析工作台" }),
    ).toBeInTheDocument();
    await expectKpiValue("Realtime QPS", "12.5");

    const serviceGap = await screen.findByTestId("service-instance-gap");
    expect(serviceGap).toHaveTextContent(
      "需要 Admin API 服务实例接口（未实现）",
    );
    expect(screen.getByTestId("feed-freshness-gap")).toHaveTextContent(
      "需要 Admin API 服务实例接口（未实现）",
    );
    expect(opsRequested).toBe(false);
  });
});
