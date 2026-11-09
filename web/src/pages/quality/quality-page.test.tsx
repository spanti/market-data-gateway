import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import { renderApp } from "@/test/render-app";
import { server } from "@/test/server";

/**
 * UI seam: the Quality & Backfill page rendered through the real app shell
 * with MSW at the network boundary. No component internals are reached.
 */

function gapEvent(overrides: Record<string, unknown> = {}) {
  return {
    instrument_id: "binance:BTC/USDT",
    interval: "1m",
    from_ts: "2026-11-08T04:24:52.850236000Z",
    to_ts: "2026-11-09T04:24:52.850236000Z",
    reason: "history_unsupported",
    recorded_at: "2026-11-09T04:24:52.850393000Z",
    ...overrides,
  };
}

function collectorStatus(gaps: unknown[]) {
  return {
    service: "collector",
    role: "collector",
    ready: true,
    uptime_s: 1.5,
    feed: "fake",
    last_ingest_ts: "2026-11-09T04:24:54.380612000Z",
    last_bar_ts: "2026-11-09T00:01:00.000000000Z",
    bars_ingested: 67,
    bars_written: 1,
    stale_count: 0,
    gap_unrecoverable_count: gaps.length,
    write_queue_depth: 0,
    write_dropped_bars: 0,
    write_lag_bars: 0,
    write_last_flush_ts: null,
    write_last_flush_duration_s: null,
    write_sink: {
      mode: "buffered",
      queue_depth: 0,
      queue_capacity: 10_000,
      pending_bytes: 0,
      dropped_bars: 0,
      lag_bars: 0,
      dropped_identities: 0,
      flushes: 0,
      flush_failures: 0,
      persisted_bars: 0,
      last_flush_ts: null,
      last_flush_duration_s: null,
    },
    quarantine: {},
    health: {
      stale: [],
      stale_count: 0,
      tracked: [],
      gap_unrecoverable: gaps,
      gap_unrecoverable_count: gaps.length,
    },
  };
}

describe("Quality & Backfill — Collector current snapshot", () => {
  it("labels gap_unrecoverable as a process-local snapshot lost on restart", async () => {
    server.use(
      http.get("*/ops/collector/status", () =>
        HttpResponse.json(collectorStatus([gapEvent()])),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });

    expect(
      await screen.findByText(/Collector 当前进程内快照，重启后丢失/),
    ).toBeInTheDocument();
    expect(await screen.findByText("binance:BTC/USDT")).toBeInTheDocument();
    expect(screen.getByText("history_unsupported")).toBeInTheDocument();
    // The server-reported count is shown as a snapshot count, not history.
    expect(screen.getByText(/gap_unrecoverable_count/)).toBeInTheDocument();
  });

  it("shows 未知 when the Collector is unreachable instead of an empty healthy state", async () => {
    server.use(
      http.get("*/ops/collector/status", () =>
        HttpResponse.json({ reason: "collector unavailable" }, { status: 503 }),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });

    expect(
      await screen.findByText(/无法读取 Collector/),
    ).toBeInTheDocument();
    expect(screen.getAllByText(/未知/).length).toBeGreaterThan(0);
    expect(screen.queryByText(/本次快照没有/)).not.toBeInTheDocument();
  });

  it("replaces the snapshot on refresh and never accumulates client-side history", async () => {
    const user = userEvent.setup();
    let current = collectorStatus([
      gapEvent({ instrument_id: "binance:AAA/USDT" }),
    ]);
    server.use(
      http.get("*/ops/collector/status", () => HttpResponse.json(current)),
    );

    renderApp({ initialEntries: ["/quality"] });

    expect(
      await screen.findByText("binance:AAA/USDT"),
    ).toBeInTheDocument();

    current = collectorStatus([
      gapEvent({ instrument_id: "binance:BBB/USDT" }),
    ]);
    await user.click(screen.getByRole("button", { name: "刷新快照" }));

    await waitFor(() =>
      expect(screen.getByText("binance:BBB/USDT")).toBeInTheDocument(),
    );
    expect(screen.queryByText("binance:AAA/USDT")).not.toBeInTheDocument();
    // One row, not two: the previous polling result is not kept.
    expect(screen.getAllByRole("row")).toHaveLength(2); // header + one body row
  });
});

describe("Quality & Backfill — Current Contract gating", () => {
  it("keeps the Backfill Job area visible but disabled with 需要 Admin API and no invented jobs", async () => {
    server.use(
      http.get("*/ops/collector/status", () =>
        HttpResponse.json(collectorStatus([])),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });

    const region = await screen.findByRole("region", { name: "Backfill Job" });
    expect(
      await within(region).findByText("需要 Admin API"),
    ).toBeInTheDocument();
    expect(
      within(region).getByRole("button", { name: "触发 Backfill Job" }),
    ).toBeDisabled();
    expect(within(region).queryByRole("table")).not.toBeInTheDocument();
    expect(within(region).queryByText("running")).not.toBeInTheDocument();
    expect(
      within(region).queryByRole("progressbar"),
    ).not.toBeInTheDocument();
  });

  it("marks persistent gaps as Admin API only and does not present the snapshot as persistent history", async () => {
    server.use(
      http.get("*/ops/collector/status", () =>
        HttpResponse.json(collectorStatus([gapEvent()])),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });

    const region = await screen.findByRole("region", {
      name: "持久 Unrecoverable Backfill Gap",
    });
    expect(
      await within(region).findByText("需要 Admin API"),
    ).toBeInTheDocument();
    expect(within(region).queryByRole("table")).not.toBeInTheDocument();

    // The process snapshot is clearly a different source, not persistent data.
    expect(
      screen.getByText(/Collector 当前进程内快照，重启后丢失/),
    ).toBeInTheDocument();
  });
});

const ADMIN_CAPABILITIES = {
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
    backfill_job: {
      state: "enabled",
      plan_modes: ["native_interval", "one_minute_plus_aggregation"],
    },
    persistent_backfill_gaps: { state: "enabled" },
  },
};

function useAdminMode(accessRole: "SuperAdmin" | "Operator" | "Auditor") {
  server.use(
    http.get("*/api/v1/admin/capabilities", () =>
      HttpResponse.json(ADMIN_CAPABILITIES),
    ),
    http.get("*/api/v1/admin/auth/session", () =>
      HttpResponse.json({
        authenticated: true,
        server_time: "2026-11-09T12:00:00.000000000Z",
        user: {
          subject: "oidc-subject-123",
          display_name: "Ada Operator",
          email: "ada@example.com",
        },
        access_role: accessRole,
        session: null,
        csrf: { header_name: "X-CSRF-Token", token: "csrf-test-token" },
      }),
    ),
    http.get("*/ops/collector/status", () =>
      HttpResponse.json(collectorStatus([])),
    ),
  );
}

function persistentGap(overrides: Record<string, unknown> = {}) {
  return {
    gap_id: "gap_open_1",
    status: "open",
    feed: "binance_spot",
    instrument_id: "binance:BTC/USDT",
    interval: "1m",
    start: "2026-09-01T00:00:00.000000000Z",
    end: "2026-09-01T01:00:00.000000000Z",
    reason: "history_unsupported",
    first_discovered_at: "2026-09-01T01:05:00.000000000Z",
    last_discovered_at: "2026-11-09T12:00:00.000000000Z",
    occurrence_count: 3,
    resolved_at: null,
    resolved_by_job_id: null,
    related_job_ids: [],
    created_at: "2026-09-01T01:05:00.000000000Z",
    updated_at: "2026-11-09T12:00:00.000000000Z",
    ...overrides,
  };
}

describe("Quality & Backfill — persistent gaps (Admin API contract target)", () => {
  it("shows first/last seen, reason, feed, InstrumentId, interval, range, status and linked jobs without a delete control", async () => {
    useAdminMode("Operator");
    const resolved = persistentGap({
      gap_id: "gap_resolved_1",
      status: "resolved",
      resolved_at: "2026-11-09T12:30:00.000000000Z",
      resolved_by_job_id: "bf_succeeded_1",
      related_job_ids: ["bf_succeeded_1"],
    });
    const open = persistentGap({ related_job_ids: ["bf_failed_1"] });
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({
          items: [open, resolved],
          page: { next_cursor: null, has_more: false },
        }),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });

    const region = await screen.findByRole("region", {
      name: "持久 Unrecoverable Backfill Gap",
    });
    expect(await within(region).findByText("gap_open_1")).toBeInTheDocument();
    expect(within(region).getByText("gap_resolved_1")).toBeInTheDocument();
    expect(within(region).getAllByText("binance_spot")).toHaveLength(2);
    expect(within(region).getAllByText("history_unsupported")).toHaveLength(2);
    expect(
      within(region).getAllByText(/2026-09-01 01:05:00 UTC/).length,
    ).toBeGreaterThan(0);
    expect(
      within(region).getAllByText(/2026-11-09 12:00:00 UTC/).length,
    ).toBeGreaterThan(0);
    expect(within(region).getAllByText("open").length).toBeGreaterThan(0);
    expect(within(region).getAllByText("resolved").length).toBeGreaterThan(0);
    expect(within(region).getByText(/关联：bf_failed_1/)).toBeInTheDocument();
    expect(within(region).getByText(/已由 bf_succeeded_1 解决/)).toBeInTheDocument();
    expect(within(region).getAllByText(/3 次/).length).toBeGreaterThan(0);
    expect(within(region).queryByRole("button", { name: /删除/ })).not.toBeInTheDocument();
  });

  it("sends gap filters to the server instead of filtering a fetched page locally", async () => {
    const user = userEvent.setup();
    useAdminMode("Operator");
    const seen: URLSearchParams[] = [];
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", ({ request }) => {
        const url = new URL(request.url);
        seen.push(url.searchParams);
        const items = url.searchParams.get("status") === "resolved"
          ? [persistentGap({ gap_id: "gap_only_resolved", status: "resolved" })]
          : [persistentGap({ gap_id: "gap_open_only" })];
        return HttpResponse.json({
          items,
          page: { next_cursor: null, has_more: false },
        });
      }),
    );

    renderApp({ initialEntries: ["/quality"] });

    const region = await screen.findByRole("region", {
      name: "持久 Unrecoverable Backfill Gap",
    });
    await within(region).findByText("gap_open_only");

    await user.selectOptions(
      within(region).getByLabelText("Gap 状态"),
      "resolved",
    );
    await user.type(within(region).getByLabelText("Feed"), "binance_spot");
    await user.type(
      within(region).getByLabelText("InstrumentId"),
      "binance:BTC/USDT",
    );
    await user.selectOptions(
      within(region).getByLabelText("Bar Interval"),
      "1m",
    );
    await user.selectOptions(
      within(region).getByLabelText("原因"),
      "history_unsupported",
    );
    await user.click(
      within(region).getByRole("button", { name: "应用 Gap 筛选" }),
    );

    await waitFor(() => {
      expect(seen[seen.length - 1]?.get("status")).toBe("resolved");
    });
    const last = seen[seen.length - 1];
    expect(last?.get("feed")).toBe("binance_spot");
    expect(last?.get("instrument_id")).toBe("binance:BTC/USDT");
    expect(last?.get("interval")).toBe("1m");
    expect(last?.get("reason")).toBe("history_unsupported");
    expect(await within(region).findByText("gap_only_resolved")).toBeInTheDocument();
    expect(within(region).queryByText("gap_open_only")).not.toBeInTheDocument();
  });
});

function backfillJob(overrides: Record<string, unknown> = {}) {
  const interval = typeof overrides.interval === "string" ? overrides.interval : "1h";
  return {
    job_id: "bf_job_1",
    status: "queued",
    instrument_id: "binance:BTC/USDT",
    interval,
    start: "2026-09-01T00:00:00.000000000Z",
    end: "2026-09-02T00:00:00.000000000Z",
    plan: {
      mode: interval === "1m" ? "native_interval" : "one_minute_plus_aggregation",
      source_intervals: ["1m"],
      target_interval: interval,
      description:
        interval === "1m" ? "原生 1m 补采" : "补采 1m + 内部 Bar Aggregation",
    },
    current_range: null,
    counters: {
      scanned_bars: 10,
      gap_bars: 2,
      backfilled_bars: 2,
      written_bars: 3,
      rejected_bars: 0,
    },
    requested_by: {
      subject: "oidc-subject-123",
      display_name: "Ada Operator",
      access_role: "Operator",
    },
    reason: "recover exchange outage",
    retry_of_job_id: null,
    created_at: "2026-11-09T12:00:00.000000000Z",
    started_at: null,
    finished_at: null,
    error: null,
    resolved_gap_ids: [],
    ...overrides,
  };
}

describe("Quality & Backfill — Backfill Job state machine (Admin API contract target)", () => {
  it("renders every contract status, counters, current range and indeterminate progress", async () => {
    useAdminMode("Operator");
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.get("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json({
          items: [
            backfillJob({ job_id: "bf_queued", status: "queued" }),
            backfillJob({
              job_id: "bf_running",
              status: "running",
              started_at: "2026-11-09T12:01:00.000000000Z",
              current_range: {
                start: "2026-09-01T06:00:00.000000000Z",
                end: "2026-09-01T12:00:00.000000000Z",
              },
            }),
            backfillJob({ job_id: "bf_succeeded", status: "succeeded" }),
            backfillJob({
              job_id: "bf_partial",
              status: "partially_succeeded",
              error: {
                code: "segment_failed",
                message: "one segment was rejected",
                details: {},
              },
            }),
            backfillJob({
              job_id: "bf_failed",
              status: "failed",
              error: { code: "interrupted", message: "restart", details: {} },
            }),
            backfillJob({
              job_id: "bf_cancelling",
              status: "cancelling",
              current_range: {
                start: "2026-09-01T06:00:00.000000000Z",
                end: "2026-09-01T07:00:00.000000000Z",
              },
            }),
            backfillJob({ job_id: "bf_cancelled", status: "cancelled" }),
          ],
          page: { next_cursor: null, has_more: false },
        }),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });

    const region = await screen.findByRole("region", { name: "Backfill Job" });
    await within(region).findByText("bf_queued");

    for (const status of [
      "queued",
      "running",
      "succeeded",
      "partially_succeeded",
      "failed",
      "cancelling",
      "cancelled",
    ]) {
      expect(
        within(region).getAllByText(status).length,
      ).toBeGreaterThan(0);
    }

    expect(within(region).getAllByText(/扫描 10 · 缺失 2 · 补采 2 · 写入 3 · 拒绝 0/).length).toBeGreaterThan(0);
    expect(within(region).getAllByText(/2026-09-01 06:00:00 UTC – 2026-09-01 12:00:00 UTC/).length).toBeGreaterThan(0);
    expect(within(region).getAllByText(/进度不确定/).length).toBeGreaterThan(0);
    expect(within(region).getAllByRole("progressbar").length).toBeGreaterThan(0);
    expect(
      within(region).getAllByText(/部分成功，不代表全部缺口已解决/).length,
    ).toBeGreaterThan(0);
    expect(within(region).getAllByText(/已中断/).length).toBeGreaterThan(0);
    expect(within(region).getAllByText(/written_bars 包含内部聚合生成的父 Bar/).length).toBeGreaterThan(0);
  });

  it("shows retry linkage and gap resolution only for succeeded jobs", async () => {
    useAdminMode("Operator");
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.get("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json({
          items: [
            backfillJob({
              job_id: "bf_retry",
              status: "queued",
              retry_of_job_id: "bf_original",
            }),
            backfillJob({
              job_id: "bf_done",
              status: "succeeded",
              resolved_gap_ids: ["gap_resolved_1", "gap_resolved_2"],
            }),
            backfillJob({
              job_id: "bf_partial",
              status: "partially_succeeded",
              resolved_gap_ids: [],
            }),
          ],
          page: { next_cursor: null, has_more: false },
        }),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });

    const region = await screen.findByRole("region", { name: "Backfill Job" });
    expect(await within(region).findByText(/重试自 bf_original/)).toBeInTheDocument();
    expect(
      within(region).getByText(/已解决 Gap：gap_resolved_1, gap_resolved_2/),
    ).toBeInTheDocument();
    expect(
      within(region).getAllByText(/未关闭全部 Gap/).length,
    ).toBeGreaterThan(0);
  });

  it("does not fabricate jobs when the admin list is empty", async () => {
    useAdminMode("Operator");
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.get("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });

    const region = await screen.findByRole("region", { name: "Backfill Job" });
    expect(await within(region).findByText("没有 Backfill Job")).toBeInTheDocument();
    expect(within(region).queryByRole("progressbar")).not.toBeInTheDocument();
  });
});

async function fillBackfillForm(
  user: ReturnType<typeof userEvent.setup>,
  region: HTMLElement,
  overrides: { interval?: string; end?: string } = {},
) {
  await user.type(
    within(region).getByLabelText("InstrumentId"),
    "binance:BTC/USDT",
  );
  await user.selectOptions(
    within(region).getByLabelText("目标 Bar Interval"),
    overrides.interval ?? "1h",
  );
  fireEvent.change(within(region).getByLabelText("开始时间（UTC，闭区间）"), {
    target: { value: "2026-09-01T00:00" },
  });
  fireEvent.change(within(region).getByLabelText("结束时间（UTC，开区间）"), {
    target: { value: overrides.end ?? "2026-09-02T00:00" },
  });
  await user.type(
    within(region).getByLabelText("操作原因"),
    "recover exchange outage",
  );
}

describe("Quality & Backfill — create Backfill Job (Admin API contract target)", () => {
  it("validates required fields, shows the 1m+aggregation plan, confirms, and submits the exact body", async () => {
    const user = userEvent.setup();
    useAdminMode("Operator");
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.get("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });
    const region = await screen.findByRole("region", { name: "Backfill Job" });
    await within(region).findByText("没有 Backfill Job");

    await user.click(
      within(region).getByRole("button", { name: "创建 Backfill Job" }),
    );
    expect(within(region).getByText("请输入 InstrumentId")).toBeInTheDocument();
    expect(within(region).getByText("请选择目标 Bar Interval")).toBeInTheDocument();
    expect(within(region).getByText("请输入开始时间")).toBeInTheDocument();
    expect(within(region).getByText("请输入结束时间")).toBeInTheDocument();
    expect(within(region).getByText("请输入操作原因")).toBeInTheDocument();

    await fillBackfillForm(user, region, { interval: "1h" });

    expect(
      within(region).getAllByText(/补采 1m \+ 内部 Bar Aggregation/).length,
    ).toBeGreaterThan(0);
    expect(
      within(region).getByText(/不是调用原生高周期接口/),
    ).toBeInTheDocument();

    let posted: unknown = null;
    let headers: Headers | null = null;
    server.use(
      http.post("*/api/v1/admin/backfill-jobs", async ({ request }) => {
        posted = await request.json();
        headers = request.headers;
        return HttpResponse.json(
          backfillJob({
            job_id: "bf_created",
            status: "queued",
            interval: "1h",
            reason: "recover exchange outage",
          }),
          { status: 202 },
        );
      }),
    );

    await user.click(
      within(region).getByRole("button", { name: "创建 Backfill Job" }),
    );

    const dialog = await screen.findByRole("dialog", {
      name: "确认触发 Backfill Job",
    });
    expect(
      within(dialog).getByText(/补采 1m \+ 内部 Bar Aggregation/),
    ).toBeInTheDocument();
    expect(
      within(dialog).getByText(/2026-09-01 00:00:00 UTC – 2026-09-02 00:00:00 UTC/),
    ).toBeInTheDocument();

    await user.click(
      within(dialog).getByRole("button", { name: "确认创建 Job" }),
    );

    expect(
      await within(region).findByText(/已创建 Backfill Job bf_created/),
    ).toBeInTheDocument();
    expect(posted).toEqual({
      instrument_id: "binance:BTC/USDT",
      interval: "1h",
      start: "2026-09-01T00:00:00.000Z",
      end: "2026-09-02T00:00:00.000Z",
      reason: "recover exchange outage",
      confirm: true,
    });
    expect((headers as unknown as Headers).get("x-csrf-token")).toBe(
      "csrf-test-token",
    );
    expect((headers as unknown as Headers).get("idempotency-key")).toBeTruthy();
  });

  it("shows the native 1m plan and rejects a non-positive range", async () => {
    const user = userEvent.setup();
    useAdminMode("Operator");
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.get("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });
    const region = await screen.findByRole("region", { name: "Backfill Job" });
    await within(region).findByText("没有 Backfill Job");

    await fillBackfillForm(user, region, {
      interval: "1m",
      end: "2026-09-01T00:00",
    });
    const plan = within(region).getByTestId("backfill-plan");
    expect(plan).toHaveTextContent("原生 1m 补采");
    expect(plan).not.toHaveTextContent("补采 1m + 内部 Bar Aggregation");

    await user.click(
      within(region).getByRole("button", { name: "创建 Backfill Job" }),
    );
    expect(
      within(region).getByText("结束时间必须晚于开始时间"),
    ).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("expresses a 409 active-job conflict without showing success", async () => {
    const user = userEvent.setup();
    useAdminMode("Operator");
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.get("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.post("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json(
          {
            error: {
              code: "conflict",
              message: "an active backfill job already exists",
              details: { reason: "active_job", active_job_id: "bf_active_1" },
              request_id: "req_conflict_ui",
            },
          },
          { status: 409 },
        ),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });
    const region = await screen.findByRole("region", { name: "Backfill Job" });
    await within(region).findByText("没有 Backfill Job");
    await fillBackfillForm(user, region);
    await user.click(
      within(region).getByRole("button", { name: "创建 Backfill Job" }),
    );
    const dialog = await screen.findByRole("dialog", {
      name: "确认触发 Backfill Job",
    });
    await user.click(
      within(dialog).getByRole("button", { name: "确认创建 Job" }),
    );

    expect(
      await within(region).findByText(/冲突：本次未创建 Job/),
    ).toBeInTheDocument();
    expect(
      within(region).getByText(/已有运行中的 Job：bf_active_1/),
    ).toBeInTheDocument();
    expect(within(region).getByText(/req_conflict_ui/)).toBeInTheDocument();
    expect(
      within(region).queryByText(/已创建 Backfill Job/),
    ).not.toBeInTheDocument();
  });

  it("treats a network failure as an unknown outcome and never auto-retries", async () => {
    const user = userEvent.setup();
    useAdminMode("Operator");
    let attempts = 0;
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.get("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.post("*/api/v1/admin/backfill-jobs", () => {
        attempts += 1;
        return HttpResponse.error();
      }),
    );

    renderApp({ initialEntries: ["/quality"] });
    const region = await screen.findByRole("region", { name: "Backfill Job" });
    await within(region).findByText("没有 Backfill Job");
    await fillBackfillForm(user, region);
    await user.click(
      within(region).getByRole("button", { name: "创建 Backfill Job" }),
    );
    const dialog = await screen.findByRole("dialog", {
      name: "确认触发 Backfill Job",
    });
    await user.click(
      within(dialog).getByRole("button", { name: "确认创建 Job" }),
    );

    expect(await within(region).findByText(/结果未知/)).toBeInTheDocument();
    expect(within(region).getByText(/未自动重试/)).toBeInTheDocument();
    expect(attempts).toBe(1);
    expect(
      within(region).queryByText(/已创建 Backfill Job/),
    ).not.toBeInTheDocument();
  });
});

describe("Quality & Backfill — cancel and retry (Admin API contract target)", () => {
  it("cancels cooperatively and states that written Revisions are not withdrawn", async () => {
    const user = userEvent.setup();
    useAdminMode("Operator");
    let cancelBody: unknown = null;
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.get("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json({
          items: [backfillJob({ job_id: "bf_run", status: "running" })],
          page: { next_cursor: null, has_more: false },
        }),
      ),
      http.post(
        "*/api/v1/admin/backfill-jobs/:jobId/cancel",
        async ({ request }) => {
          cancelBody = await request.json();
          return HttpResponse.json(
            { ...backfillJob({ job_id: "bf_run", status: "cancelling" }) },
            { status: 202 },
          );
        },
      ),
    );

    renderApp({ initialEntries: ["/quality"] });
    const region = await screen.findByRole("region", { name: "Backfill Job" });
    await within(region).findByText("bf_run");

    await user.click(
      within(region).getByRole("button", { name: "取消 Job bf_run" }),
    );
    const dialog = await screen.findByRole("dialog", {
      name: "确认取消 Backfill Job",
    });
    await user.type(within(dialog).getByLabelText("操作原因"), "stop backfill");
    await user.click(
      within(dialog).getByRole("button", { name: "确认取消 Job" }),
    );

    expect(
      await within(region).findByText(/取消请求已接受/),
    ).toBeInTheDocument();
    expect(
      within(region).getByText(/已写入的 Revision 不会撤回/),
    ).toBeInTheDocument();
    expect(within(region).queryByText(/已取消/)).not.toBeInTheDocument();
    expect(cancelBody).toEqual({ reason: "stop backfill" });
  });

  it("shows a 409 job_not_cancellable conflict instead of success", async () => {
    const user = userEvent.setup();
    useAdminMode("Operator");
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.get("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json({
          items: [backfillJob({ job_id: "bf_run", status: "running" })],
          page: { next_cursor: null, has_more: false },
        }),
      ),
      http.post("*/api/v1/admin/backfill-jobs/:jobId/cancel", () =>
        HttpResponse.json(
          {
            error: {
              code: "conflict",
              message: "job is not cancellable",
              details: { reason: "job_not_cancellable" },
              request_id: "req_not_cancellable",
            },
          },
          { status: 409 },
        ),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });
    const region = await screen.findByRole("region", { name: "Backfill Job" });
    await within(region).findByText("bf_run");
    await user.click(
      within(region).getByRole("button", { name: "取消 Job bf_run" }),
    );
    const dialog = await screen.findByRole("dialog", {
      name: "确认取消 Backfill Job",
    });
    await user.type(within(dialog).getByLabelText("操作原因"), "stop backfill");
    await user.click(
      within(dialog).getByRole("button", { name: "确认取消 Job" }),
    );

    expect(
      await within(region).findByText(/冲突：无法取消/),
    ).toBeInTheDocument();
    expect(within(region).getByText(/req_not_cancellable/)).toBeInTheDocument();
    expect(
      within(region).queryByText(/取消请求已接受/),
    ).not.toBeInTheDocument();
  });

  it("retries as a new linked Job without rewriting the old one", async () => {
    const user = userEvent.setup();
    useAdminMode("Operator");
    let retryPath: string | null = null;
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({ items: [], page: { next_cursor: null, has_more: false } }),
      ),
      http.get("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json({
          items: [
            backfillJob({
              job_id: "bf_failed",
              status: "failed",
              error: { code: "interrupted", message: "restart", details: {} },
            }),
          ],
          page: { next_cursor: null, has_more: false },
        }),
      ),
      http.post(
        "*/api/v1/admin/backfill-jobs/:jobId/retry",
        ({ request, params }) => {
          retryPath = `${String(params.jobId)}:${request.method}`;
          return HttpResponse.json(
            backfillJob({
              job_id: "bf_retry_new",
              status: "queued",
              retry_of_job_id: "bf_failed",
            }),
            { status: 202 },
          );
        },
      ),
    );

    renderApp({ initialEntries: ["/quality"] });
    const region = await screen.findByRole("region", { name: "Backfill Job" });
    await within(region).findByText("bf_failed");

    await user.click(
      within(region).getByRole("button", { name: "重试 Job bf_failed" }),
    );
    const dialog = await screen.findByRole("dialog", {
      name: "确认重试 Backfill Job",
    });
    await user.type(within(dialog).getByLabelText("操作原因"), "retry it");
    await user.click(
      within(dialog).getByRole("button", { name: "确认重试 Job" }),
    );

    expect(
      await within(region).findByText(/已创建关联的新 Job bf_retry_new/),
    ).toBeInTheDocument();
    expect(
      within(region).getByText(/重试自 bf_failed/),
    ).toBeInTheDocument();
    expect(within(region).getByText(/旧 Job 状态与计数不变/)).toBeInTheDocument();
    expect(retryPath).toBe("bf_failed:POST");
    // The old row still shows the original failed status.
    expect(within(region).getAllByText("failed").length).toBeGreaterThan(0);
  });
});

describe("Quality & Backfill — role and capability gating (Admin API contract target)", () => {
  it("keeps Auditor read-only: data visible, every write control disabled with a reason", async () => {
    useAdminMode("Auditor");
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", () =>
        HttpResponse.json({
          items: [persistentGap()],
          page: { next_cursor: null, has_more: false },
        }),
      ),
      http.get("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json({
          items: [
            backfillJob({ job_id: "bf_run", status: "running" }),
            backfillJob({ job_id: "bf_failed", status: "failed" }),
          ],
          page: { next_cursor: null, has_more: false },
        }),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });
    const region = await screen.findByRole("region", { name: "Backfill Job" });
    await within(region).findByText("bf_run");

    expect(
      within(region).getByRole("button", { name: "创建 Backfill Job" }),
    ).toBeDisabled();
    expect(
      within(region).getByRole("button", { name: "取消 Job bf_run" }),
    ).toBeDisabled();
    expect(
      within(region).getByRole("button", { name: "重试 Job bf_failed" }),
    ).toBeDisabled();
    expect(
      within(region).getAllByText("需要 SuperAdmin 或 Operator 角色").length,
    ).toBeGreaterThanOrEqual(2);
    expect(within(region).queryByLabelText("InstrumentId")).not.toBeInTheDocument();

    // Read-only data is still visible.
    await within(
      screen.getByRole("region", { name: "持久 Unrecoverable Backfill Gap" }),
    ).findByText("gap_open_1");
  });

  it("fails closed when the capability is unsupported", async () => {
    useAdminMode("Operator");
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json({
          ...ADMIN_CAPABILITIES,
          capabilities: {
            backfill_job: { state: "unsupported" },
            persistent_backfill_gaps: { state: "unsupported" },
          },
        }),
      ),
    );

    renderApp({ initialEntries: ["/quality"] });

    const jobs = await screen.findByRole("region", { name: "Backfill Job" });
    expect(await within(jobs).findByText("后端未实现")).toBeInTheDocument();
    expect(within(jobs).queryByRole("table")).not.toBeInTheDocument();
    expect(within(jobs).queryByRole("button", { name: "创建 Backfill Job" })).not.toBeInTheDocument();

    const gaps = screen.getByRole("region", {
      name: "持久 Unrecoverable Backfill Gap",
    });
    expect(within(gaps).getByText("后端未实现")).toBeInTheDocument();
    expect(within(gaps).queryByRole("table")).not.toBeInTheDocument();
  });
});
