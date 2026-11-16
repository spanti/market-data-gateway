import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import type { CollectorHealth } from "@/api/endpoints/ops";
import { MONITORING_QUERY_KEYS } from "@/pages/monitoring/use-monitoring-data";
import { renderApp } from "@/test/render-app";
import { server } from "@/test/server";
import { installResizeObserverStub } from "@/test/tooltip-env";
import { expectNoBannedUiSymbols } from "@/test/ui-copy";

installResizeObserverStub();

const iso = (ms: number) => new Date(ms).toISOString();

function metricCard(label: string): HTMLElement {
  return screen.getByRole("group", { name: label });
}

async function expectMetricValue(label: string, value: string) {
  await waitFor(() => {
    expect(within(metricCard(label)).getByText(value)).toBeInTheDocument();
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

function collectorHealth(overrides: Partial<CollectorHealth> = {}): CollectorHealth {
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
    ...overrides,
  };
  health.stale = health.tracked.filter((entry) => entry.stale);
  return health;
}

function collectorStatus(overrides: Record<string, unknown> = {}) {
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
    health: collectorHealth(),
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
  metric: Record<string, string> = {},
) {
  return {
    status: "success",
    data: {
      resultType: "matrix",
      result: [{ metric, values }],
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
        return HttpResponse.json(
          overrides.prometheusInstant(url.searchParams.get("query") ?? ""),
        );
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
  it("renders the D monitoring overview from real status, freshness and trend data", async () => {
    installSources();
    const { container } = renderApp();

    expect(
      await screen.findByRole("heading", { name: "监控总览", level: 1 }),
    ).toBeInTheDocument();

    // Four KPI cards, with QPS, Peak QPS, P99 and an independent Dataset age.
    for (const label of [
      "Realtime QPS",
      "Peak QPS",
      "HTTP P99",
      "Dataset Freshness",
    ]) {
      expect(metricCard(label)).toBeInTheDocument();
    }
    await expectMetricValue("Realtime QPS", "12.5");
    expect(within(metricCard("Realtime QPS")).getByText("req/s")).toBeInTheDocument();
    expect(within(metricCard("Realtime QPS")).getByText(/最近样本/)).toBeInTheDocument();
    await expectMetricValue("Peak QPS", "18.25");
    expect(within(metricCard("Peak QPS")).getByText(/最近 1 小时/)).toBeInTheDocument();
    await expectMetricValue("HTTP P99", "500");
    expect(within(metricCard("HTTP P99")).getByText("ms")).toBeInTheDocument();
    expect(
      within(metricCard("HTTP P99")).getByText(/P50 20 ms，P95 250 ms/),
    ).toBeInTheDocument();
    expect(
      within(metricCard("HTTP P99")).getByText(/含完整响应流耗时/),
    ).toBeInTheDocument();
    expect(
      within(metricCard("Dataset Freshness")).getByText("前"),
    ).toBeInTheDocument();
    expect(
      within(metricCard("Dataset Freshness")).getByText(/最近落盘/),
    ).toBeInTheDocument();
    expect(
      within(metricCard("Dataset Freshness")).queryByText("未知"),
    ).not.toBeInTheDocument();

    // QPS and HTTP latency are separate trend cards with data tables.
    for (const title of ["QPS 趋势", "HTTP 延迟趋势"]) {
      const chart = screen.getByRole("region", { name: title });
      expect(
        within(chart).getByRole("heading", { name: title }),
      ).toBeInTheDocument();
      expect(
        within(chart).getByText(/查看数据表（\d+ 个时间点）/),
      ).toBeInTheDocument();
    }

    // Feed Freshness keeps server age/threshold/stale semantics.
    const feed = screen.getByRole("region", { name: "Feed Freshness" });
    expect(within(feed).getByText(/剩余 100s/)).toBeInTheDocument();
    expect(within(feed).getAllByText("失活").length).toBeGreaterThan(0);
    expect(within(feed).getByText("正常")).toBeInTheDocument();
    expect(within(feed).queryByText("Collector /status 加载失败")).not.toBeInTheDocument();

    // Dataset Freshness is a separate region and row source.
    const dataset = screen.getByRole("region", { name: "Dataset Freshness" });
    expect(within(dataset).getByText(/独立于 Feed Freshness/)).toBeInTheDocument();
    expect(within(dataset).getByText("1m")).toBeInTheDocument();

    // Service Role and Service Instance are not conflated: Current Contract
    // exposes one process per Role, so the card names Service Roles.
    const service = screen.getByRole("region", { name: "Service Role" });
    expect(
      within(service).getByRole("heading", { name: "Service Role" }),
    ).toBeInTheDocument();
    expect(within(service).getByText("3 个 Service Role")).toBeInTheDocument();
    for (const role of ["collector", "api", "replay"] as const) {
      const item = within(service).getByRole("group", {
        name: `${role} Service Role`,
      });
      expect(item).toHaveTextContent(role);
      expect(item).toHaveTextContent("Service Role");
      expect(item).toHaveTextContent("Instance");
    }
    expect(service).not.toHaveTextContent(/master|worker|leader|failover/i);

    // Venues come from real instruments; connection capabilities stay unknown.
    const upstream = screen.getByRole("region", {
      name: "上游能力与 Freshness",
    });
    expect(within(upstream).getByText("venue-a")).toBeInTheDocument();
    expect(within(upstream).getByText("venue-b")).toBeInTheDocument();
    expect(within(upstream).getAllByText("连接 未暴露").length).toBeGreaterThan(0);
    expect(within(upstream).getAllByText("心跳 未暴露").length).toBeGreaterThan(0);
    expect(within(upstream).getAllByText("重连 未暴露").length).toBeGreaterThan(0);
    expect(within(upstream).getAllByText("网络 未暴露").length).toBeGreaterThan(0);
    expect(within(upstream).getByText(/不证明连接健康/)).toBeInTheDocument();
    expect(screen.queryByText(/binance|okx|fix/i)).not.toBeInTheDocument();

    const upstreamDetail = screen.getByRole("region", { name: "上游状态" });
    expect(within(upstreamDetail).getByText("venue-a")).toBeInTheDocument();
    expect(within(upstreamDetail).getByText("venue-b")).toBeInTheDocument();

    // Alerts are derived from the real stale entry, never fabricated.
    const alerts = screen.getByRole("region", { name: "页面内告警" });
    expect(within(alerts).getByText("Feed Freshness 失活")).toBeInTheDocument();
    expect(
      within(alerts).getByText("上游 venue-a 有 Feed 失活"),
    ).toBeInTheDocument();

    // Per-source metadata: source, window, last successful update.
    const sourcesPanel = screen.getByRole("region", { name: "数据源与刷新" });
    expect(within(sourcesPanel).getByText("Service /status")).toBeInTheDocument();
    expect(
      within(sourcesPanel).getByText("Dataset /api/v1/status"),
    ).toBeInTheDocument();
    expect(
      within(sourcesPanel).getByText(/Prometheus（\/prometheus）/),
    ).toBeInTheDocument();
    expect(screen.getAllByText(/最后成功/).length).toBeGreaterThanOrEqual(5);

    // No banned separators in UI-generated copy.
    expectNoBannedUiSymbols(container);
  });

  it("shows a stale entry as failed and never as healthy", async () => {
    installSources({
      collector: collectorStatus({
        health: collectorHealth({
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
          stale_count: 1,
        }),
      }),
    });
    renderApp();

    await waitFor(() => {
      const feed = screen.getByRole("region", { name: "Feed Freshness" });
      expect(within(feed).getByText("失活")).toBeInTheDocument();
      expect(within(feed).queryByText("正常")).not.toBeInTheDocument();
    });
    const alerts = screen.getByRole("region", { name: "页面内告警" });
    expect(within(alerts).getByText("严重")).toBeInTheDocument();
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
      (await screen.findAllByText(/Collector \/status 加载失败/)).length,
    ).toBeGreaterThan(0);
    expect(screen.queryByText(/剩余 100s/)).not.toBeInTheDocument();
    expect(screen.getAllByText(/最后成功/).length).toBeGreaterThan(0);

    const service = screen.getByRole("region", { name: "Service Role" });
    expect(within(service).getAllByText(/状态未知/).length).toBeGreaterThan(0);
    expect(within(service).getAllByText(/Uptime —/).length).toBeGreaterThan(0);

    const alerts = screen.getByRole("region", { name: "页面内告警" });
    expect(
      within(alerts).getByText("Collector /status 加载失败"),
    ).toBeInTheDocument();
    expect(
      screen.getAllByText("Ops endpoint returned HTTP 500").length,
    ).toBeGreaterThan(0);
  });

  it("marks trend KPIs unknown when Prometheus fails, with the last success", async () => {
    installSources();
    const { queryClient } = renderApp();

    await expectMetricValue("Realtime QPS", "12.5");

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

    await waitFor(() => {
      expect(
        within(metricCard("Realtime QPS")).getByText("未知"),
      ).toBeInTheDocument();
      expect(
        within(metricCard("Realtime QPS")).getByText("加载失败"),
      ).toBeInTheDocument();
      expect(
        within(metricCard("Realtime QPS")).queryByText("12.5"),
      ).not.toBeInTheDocument();
    });
    expect(screen.getAllByText(/最后成功/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/查询失败/).length).toBeGreaterThan(0);
    expect(screen.getAllByText("upstream down").length).toBeGreaterThan(0);
  });

  it("keeps cached last-success evidence without showing old KPIs as healthy", async () => {
    installSources();
    const { queryClient } = renderApp();
    await expectMetricValue("Realtime QPS", "12.5");

    server.use(
      http.get("*/prometheus/api/v1/query_range", () => HttpResponse.json({ status: "error", error: "down" })),
    );
    await queryClient.refetchQueries({
      queryKey: MONITORING_QUERY_KEYS.qpsTrend,
    });

    const card = metricCard("Realtime QPS");
    await waitFor(() => {
      expect(within(card).getByText("未知")).toBeInTheDocument();
      expect(within(card).queryByText("12.5")).not.toBeInTheDocument();
    });
    expect(within(card).getByText(/最后成功/)).toBeInTheDocument();
  });

  it("shows explicit empty states without generating health or alert records", async () => {
    installSources({
      collector: collectorStatus({
        health: collectorHealth({ tracked: [], stale: [], stale_count: 0 }),
      }),
      dataset: datasetStatus({ intervals: {}, last_flushed_ts: null }),
      instrumentList: { instruments: [] },
    });
    renderApp();

    expect(
      await screen.findByText("暂无 Feed Freshness 跟踪项"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("暂无 Dataset Freshness 数据"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("暂无 Instrument Reference 数据"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("当前没有页面内告警"),
    ).toBeInTheDocument();
    expect(
      within(metricCard("Dataset Freshness")).getByText("尚无 flush 记录"),
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
      screen.getByRole("radio", { name: "1 小时" }),
    ).toHaveAttribute("aria-checked", "true");

    await user.click(screen.getByRole("radio", { name: "6 小时" }));

    expect(
      screen.getByRole("radio", { name: "6 小时" }),
    ).toHaveAttribute("aria-checked", "true");
    expect(group).toBeInTheDocument();
    await waitFor(() => {
      expect(
        within(metricCard("Peak QPS")).getByText(/最近 6 小时/),
      ).toBeInTheDocument();
    });

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

  it("keeps KPI selection stable when a series display label includes a path", async () => {
    const now = Math.floor(Date.now() / 1000);
    installSources({
      prometheusRange: (query) => {
        if (query.includes("0.50")) {
          return prometheusMatrix([[now - 30, "0.02"]], { path: "/api/v1/bars" });
        }
        if (query.includes("0.95")) {
          return prometheusMatrix([[now - 30, "0.25"]], { path: "/api/v1/bars" });
        }
        if (query.includes("0.99")) {
          return prometheusMatrix([[now - 30, "0.5"]], { path: "/api/v1/bars" });
        }
        return prometheusMatrix([[now, "12.5"]], { path: "/api/v1/bars" });
      },
    });
    const { container } = renderApp();

    await expectMetricValue("HTTP P99", "500");
    expect(screen.getByText("P99 /api/v1/bars")).toBeInTheDocument();
    expect(screen.getAllByText("P99 /api/v1/bars").length).toBeGreaterThan(0);
    expectNoBannedUiSymbols(container);
  });

  it("keeps backend raw errors verbatim while UI copy stays clean", async () => {
    const raw = "upstream · failed：timeout";
    installSources();
    const errorResponse = {
      status: "error",
      errorType: "internal",
      error: raw,
    };
    server.use(
      http.get("*/prometheus/api/v1/query_range", () =>
        HttpResponse.json(errorResponse, { status: 500 }),
      ),
      http.get("*/prometheus/api/v1/query", () =>
        HttpResponse.json(errorResponse, { status: 500 }),
      ),
    );
    const { container } = renderApp();

    await waitFor(() => {
      expect(screen.getAllByText(raw).length).toBeGreaterThan(0);
    });
    expect(screen.getAllByText(/查询失败/).length).toBeGreaterThan(0);
    expectNoBannedUiSymbols(container);
  });

  it("opens card help by hover and exposes a readable tooltip", async () => {
    installSources();
    const user = userEvent.setup();
    renderApp();

    const trigger = await screen.findByRole("button", {
      name: "Realtime QPS 说明",
    });
    await user.hover(trigger);
    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent(/1 分钟滑动窗口的请求速率/);
    expect(tip).toHaveTextContent(/统计窗口 最近 1 小时/);
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

    await expectMetricValue("Realtime QPS", "12.5");
    await expectMetricValue("Peak QPS", "18.25");
    await expectMetricValue("HTTP P99", "500");
    expect(within(metricCard("HTTP P99")).getByText("ms")).toBeInTheDocument();
    expect(
      screen.getAllByText(/Admin API 监控代理/).length,
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
    expect(
      within(metricCard("Realtime QPS")).queryByText("12.5"),
    ).not.toBeInTheDocument();
    expect(
      within(metricCard("Realtime QPS")).getByText("未知"),
    ).toBeInTheDocument();
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
      await screen.findByRole("heading", { name: "监控总览" }),
    ).toBeInTheDocument();
    await expectMetricValue("Realtime QPS", "12.5");
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
      await screen.findByRole("heading", { name: "监控总览" }),
    ).toBeInTheDocument();
    await expectMetricValue("Realtime QPS", "12.5");

    const serviceGap = screen.getByRole("region", { name: "Service Instance" });
    expect(serviceGap).toHaveTextContent(
      "需要 Admin API 服务实例接口（未实现）",
    );
    const feedGap = screen.getByRole("region", { name: "Feed Freshness" });
    expect(feedGap).toHaveTextContent(
      "需要 Admin API 服务实例接口（未实现）",
    );
    expect(opsRequested).toBe(false);
  });
});
