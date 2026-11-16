import {
  expect,
  test,
  type Page,
  type Request,
  type Route,
} from "@playwright/test";
import {
  expectNoBannedUiSymbols,
  expectNoHorizontalOverflow,
} from "./support";

/**
 * CONTRACT-ONLY: the Admin API does not exist in the current backend.
 *
 * These flows are exercised in a real browser with Playwright request
 * interception. They verify the frontend contract for 401/403/409/429 and the
 * high-risk confirmation path; they are NOT real backend integration and must
 * not be reported as such.
 */

const ADMIN_GAP = "需要 Admin API 服务实例接口（未实现）";

function futureIso(offsetMs: number): string {
  return new Date(Date.now() + offsetMs).toISOString();
}

const capabilityDocument = {
  schema_version: 1,
  server_time: futureIso(0),
  version: { admin_api: "1.0.0", contract: 1, supported_contracts: [1] },
  auth: {
    required: true,
    mode: "oidc",
    state: "available",
    roles: ["SuperAdmin", "Operator", "Auditor"],
  },
  adapters: [
    {
      name: "supervisor",
      kind: "service_lifecycle",
      state: "available",
      reason: null,
    },
    {
      name: "prometheus",
      kind: "monitoring",
      state: "available",
      reason: null,
    },
  ],
  capabilities: {
    oidc_session: { state: "enabled" },
    service_lifecycle: {
      state: "enabled",
      actions: ["start", "stop", "reload"],
    },
    monitoring_proxy: { state: "enabled", arbitrary_promql: false },
    ws_admin_session: { state: "enabled", path: "/api/v1/admin/ws" },
    backfill_job: {
      state: "enabled",
      plan_modes: ["native_interval", "one_minute_plus_aggregation"],
    },
    persistent_backfill_gaps: { state: "enabled" },
  },
};

const serviceInstances = {
  items: [
    {
      instance_id: "collector-1",
      service_role: "collector",
      display_name: "Collector primary",
      desired_state: "running",
      observed_state: "running",
      health: "healthy",
      ready: true,
      ready_reason: "collector ready",
      pid: 1001,
      started_at: futureIso(-3_600_000),
      last_observed_at: futureIso(-1_000),
      last_error: null,
      available_actions: ["start", "stop", "reload"],
    },
    {
      instance_id: "api-1",
      service_role: "api",
      display_name: "API primary",
      desired_state: "running",
      observed_state: "running",
      health: "healthy",
      ready: true,
      ready_reason: "collector subscription ready",
      pid: 1002,
      started_at: futureIso(-3_600_000),
      last_observed_at: futureIso(-1_000),
      last_error: null,
      available_actions: ["start", "stop", "reload"],
    },
  ],
  page: { next_cursor: null, has_more: false },
};

function sessionBody(role: "SuperAdmin" | "Operator" | "Auditor") {
  return {
    authenticated: true,
    server_time: futureIso(0),
    user: {
      subject: "oidc-subject-123",
      display_name: "Ada Admin",
      email: "ada@example.com",
    },
    access_role: role,
    session: {
      issued_at: futureIso(-60_000),
      last_activity_at: futureIso(-1_000),
      idle_expires_at: futureIso(30 * 60_000),
      absolute_expires_at: futureIso(8 * 3_600_000),
    },
    csrf: { header_name: "X-CSRF-Token", token: "csrf-token" },
  };
}

async function json(route: Route, status: number, body: unknown, headers = {}) {
  await route.fulfill({
    status,
    contentType: "application/json",
    headers,
    body: JSON.stringify(body),
  });
}

async function installAdmin(
  page: Page,
  options: { role?: "SuperAdmin" | "Operator" | "Auditor" } = {},
): Promise<void> {
  await page.route("**/api/v1/admin/capabilities", (route) =>
    json(route, 200, capabilityDocument),
  );
  await page.route("**/api/v1/admin/auth/session", (route) =>
    json(route, 200, sessionBody(options.role ?? "SuperAdmin")),
  );
  await page.route("**/api/v1/admin/service-instances", (route) =>
    json(route, 200, serviceInstances),
  );
}

async function openServiceInstanceTab(page: Page): Promise<void> {
  await page.goto("/console");
  await page.getByRole("tab", { name: "Service Instance" }).click();
  await expect(page.getByTestId("service-instance-api-1")).toBeVisible();
}

async function submitStop(
  page: Page,
  instanceId = "api-1",
  reason = "drain before certificate rotation",
): Promise<void> {
  const row = page.getByTestId(`service-instance-${instanceId}`);
  await row.getByRole("button", { name: "Graceful Stop" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("操作原因").fill(reason);
  await dialog.getByLabel(/输入目标实例名/).fill(instanceId);
  await dialog.getByRole("button", { name: "确认执行" }).click();
}

const QUALITY_GAPS_PATTERN = /\/api\/v1\/admin\/backfill-gaps(\?.*)?$/;
const QUALITY_JOBS_PATTERN = /\/api\/v1\/admin\/backfill-jobs(\?.*)?$/;

function qualityGap(overrides: Record<string, unknown> = {}) {
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
    related_job_ids: ["bf_failed_1"],
    created_at: "2026-09-01T01:05:00.000000000Z",
    updated_at: "2026-11-09T12:00:00.000000000Z",
    ...overrides,
  };
}

function qualityJob(overrides: Record<string, unknown> = {}) {
  const interval =
    typeof overrides.interval === "string" ? overrides.interval : "1h";
  return {
    job_id: "bf_job_1",
    status: "running",
    instrument_id: "binance:BTC/USDT",
    interval,
    start: "2026-09-01T00:00:00.000000000Z",
    end: "2026-09-02T00:00:00.000000000Z",
    plan: {
      mode:
        interval === "1m"
          ? "native_interval"
          : "one_minute_plus_aggregation",
      source_intervals: ["1m"],
      target_interval: interval,
      description:
        interval === "1m" ? "原生 1m 补采" : "补采 1m + 内部 Bar Aggregation",
    },
    current_range: {
      start: "2026-09-01T06:00:00.000000000Z",
      end: "2026-09-01T12:00:00.000000000Z",
    },
    counters: {
      scanned_bars: 10,
      gap_bars: 2,
      backfilled_bars: 2,
      written_bars: 3,
      rejected_bars: 0,
    },
    requested_by: {
      subject: "oidc-subject-123",
      display_name: "Ada Admin",
      access_role: "SuperAdmin",
    },
    reason: "recover exchange outage",
    retry_of_job_id: null,
    created_at: "2026-09-01T00:00:00.000000000Z",
    started_at: null,
    finished_at: null,
    error: null,
    resolved_gap_ids: [],
    ...overrides,
  };
}

async function installQualityAdmin(
  page: Page,
  options: { gaps?: unknown[]; jobs?: unknown[] } = {},
): Promise<void> {
  await installAdmin(page);
  await page.route(QUALITY_GAPS_PATTERN, (route) =>
    json(route, 200, {
      items: options.gaps ?? [],
      page: { next_cursor: null, has_more: false },
    }),
  );
  await page.route(QUALITY_JOBS_PATTERN, (route) =>
    json(route, 200, {
      items: options.jobs ?? [],
      page: { next_cursor: null, has_more: false },
    }),
  );
}

async function fillBackfillJobForm(page: Page): Promise<void> {
  const jobs = page.getByRole("region", { name: "Backfill Job" });
  await jobs.getByLabel("InstrumentId").fill("binance:BTC/USDT");
  await jobs.getByLabel("目标 Bar Interval").selectOption("1h");
  await jobs
    .getByLabel("开始时间（UTC，闭区间）")
    .fill("2026-09-01T00:00");
  await jobs
    .getByLabel("结束时间（UTC，开区间）")
    .fill("2026-09-02T00:00");
  await jobs.getByLabel("操作原因").fill("recover exchange outage");
}

function trackUntrustedOps(page: Page): string[] {
  const requested: string[] = [];
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (
      path === "/status" ||
      path === "/metrics" ||
      path.startsWith("/ops/") ||
      path.startsWith("/prometheus/")
    ) {
      requested.push(path);
    }
  });
  return requested;
}

test.describe("CONTRACT-ONLY Admin API mode (backend absent)", () => {
  test("uses the service-instance seam, confirms high-risk actions and sends CSRF + idempotency headers", async ({
    page,
  }) => {
    const opsPaths = trackUntrustedOps(page);
    await installAdmin(page);

    let postCount = 0;
    const captured: {
      request: Request | null;
      body: Record<string, unknown> | null;
    } = { request: null, body: null };
    await page.route(
      "**/api/v1/admin/service-instances/*/stop",
      async (route) => {
        postCount += 1;
        captured.request = route.request();
        captured.body = (await route.request().postDataJSON()) as Record<
          string,
          unknown
        >;
        await json(
          route,
          202,
          { operation_id: "op_1", request_id: "req_1", status: "accepted" },
          { "X-Request-ID": "req_1" },
        );
      },
    );
    await page.route("**/api/v1/admin/operations/op_1", (route) =>
      json(route, 200, {
        operation_id: "op_1",
        kind: "service_instance.stop",
        target: { instance_id: "api-1", service_role: "api" },
        requested_by: {
          subject: "oidc-subject-123",
          display_name: "Ada Admin",
          access_role: "SuperAdmin",
        },
        reason: "drain before certificate rotation",
        status: "succeeded",
        requested_at: futureIso(-1_000),
        started_at: futureIso(-900),
        finished_at: futureIso(-500),
        result: {},
        error: null,
        request_id: "req_1",
      }),
    );

    await openServiceInstanceTab(page);

    // Real Admin instance identity, not a Current Contract /status fallback.
    const row = page.getByTestId("service-instance-api-1");
    await expect(row).toContainText("API primary");
    await expect(row.getByText("api-1", { exact: true }).first()).toBeVisible();
    expect(opsPaths).toEqual([]);

    await row.getByRole("button", { name: "Graceful Stop" }).click();
    const dialog = page.getByRole("dialog");
    const confirm = dialog.getByRole("button", { name: "确认执行" });
    await expect(confirm).toBeDisabled();

    await dialog.getByLabel("操作原因").fill("drain before certificate rotation");
    await expect(confirm).toBeDisabled();

    await dialog.getByLabel(/输入目标实例名/).fill("api");
    await expect(confirm).toBeDisabled();
    await expect(dialog.getByText("实例名不匹配，无法提交。")).toBeVisible();

    await dialog.getByLabel(/输入目标实例名/).fill("api-1");
    await expect(confirm).toBeEnabled();
    await confirm.click();

    await expect(row.getByText("成功")).toBeVisible();
    expect(postCount).toBe(1);
    expect(captured.body).toMatchObject({
      reason: "drain before certificate rotation",
      confirm: true,
      confirm_instance_name: "api-1",
    });
    expect(captured.request?.headers()["idempotency-key"]).toBeTruthy();
    expect(captured.request?.headers()["x-csrf-token"]).toBe("csrf-token");

    // No automatic replay of a non-idempotent write.
    await page.waitForTimeout(1_000);
    expect(postCount).toBe(1);
  });

  test("401 stops writes, clears the session and returns to the login entry", async ({
    page,
  }) => {
    await installAdmin(page);
    await page.route(
      "**/api/v1/admin/service-instances/*/stop",
      (route) =>
        json(
          route,
          401,
          {
            error: {
              code: "unauthenticated",
              message: "session expired",
              details: { reason: "idle_timeout" },
              request_id: "req-401",
            },
          },
          { "X-Request-ID": "req-401" },
        ),
    );

    await openServiceInstanceTab(page);
    await submitStop(page);

    await expect(page.getByTestId("global-http-notice")).toContainText(
      "会话已失效或未登录，写操作已停止。请回到登录入口。",
    );
    const banner = page.getByTestId("runtime-mode-banner");
    await expect(banner.getByText("会话已失效")).toBeVisible();
    await expect(banner.getByRole("link", { name: "登录" })).toBeVisible();
    await expect(page.getByTestId("service-instance-gap")).toContainText(
      "需要登录",
    );
  });

  test("403 names the missing role without logging out", async ({ page }) => {
    await installAdmin(page);
    await page.route(
      "**/api/v1/admin/service-instances/*/stop",
      (route) =>
        json(
          route,
          403,
          {
            error: {
              code: "forbidden",
              message: "role denied",
              details: { reason: "role", required_role: "SuperAdmin" },
              request_id: "req-403",
            },
          },
          { "X-Request-ID": "req-403" },
        ),
    );

    await openServiceInstanceTab(page);
    await submitStop(page);

    await expect(page.getByTestId("global-http-notice")).toContainText(
      "缺少权限，需要 SuperAdmin 角色。",
    );
    await expect(page.getByTestId("service-instance-api-1")).toContainText(
      "forbidden: role denied",
    );
    await expect(
      page.getByTestId("runtime-mode-banner").getByText("SuperAdmin"),
    ).toBeVisible();
  });

  test("409 surfaces a manual-merge conflict", async ({ page }) => {
    await installAdmin(page);
    await page.route(
      "**/api/v1/admin/service-instances/*/stop",
      (route) =>
        json(
          route,
          409,
          {
            error: {
              code: "conflict",
              message: "version conflict",
              details: {
                reason: "operation_in_progress",
                current_version: 3,
              },
              request_id: "req-409",
            },
          },
          { "X-Request-ID": "req-409" },
        ),
    );

    await openServiceInstanceTab(page);
    await submitStop(page);

    await expect(page.getByTestId("global-http-notice")).toContainText(
      "数据已被他人修改（409 冲突）",
    );
    await expect(page.getByTestId("service-instance-api-1")).toContainText(
      "conflict: version conflict",
    );
  });

  test("429 honours Retry-After", async ({ page }) => {
    await installAdmin(page);
    await page.route(
      "**/api/v1/admin/service-instances/*/stop",
      (route) =>
        json(
          route,
          429,
          {
            error: {
              code: "rate_limited",
              message: "too many requests",
              details: { retry_after_s: 7 },
              request_id: "req-429",
            },
          },
          { "Retry-After": "7", "X-Request-ID": "req-429" },
        ),
    );

    await openServiceInstanceTab(page);
    await submitStop(page);

    await expect(page.getByTestId("global-http-notice")).toContainText(
      "请求过于频繁，请在 7 秒后重试。",
    );
    await expect(page.getByTestId("service-instance-api-1")).toContainText(
      "rate_limited: too many requests",
    );
  });

  test("shows the explicit Admin service-instance gap when the capability is unsupported", async ({
    page,
  }) => {
    await installAdmin(page);
    await page.route("**/api/v1/admin/capabilities", (route) =>
      json(route, 200, {
        ...capabilityDocument,
        capabilities: {
          ...capabilityDocument.capabilities,
          service_lifecycle: { state: "unsupported" },
        },
      }),
    );

    await page.goto("/console");
    await page.getByRole("tab", { name: "Service Instance" }).click();
    await expect(page.getByTestId("service-instance-gap")).toContainText(
      ADMIN_GAP,
    );
    await expect(page.getByTestId("service-instance-api-1")).toHaveCount(0);
  });
});

/**
 * CONTRACT-ONLY: the Admin API does not exist in the current backend. These
 * Quality flows use Playwright request interception. They verify the frontend
 * contract only and are NOT real OIDC or Admin API integration.
 */
test.describe("CONTRACT-ONLY Admin API Quality flows (backend absent)", () => {
  test("renders persistent gaps, lifecycle states, truthful counters and raw errors", async ({
    page,
  }) => {
    await installQualityAdmin(page, {
      gaps: [qualityGap()],
      jobs: [
        qualityJob({ job_id: "bf_running", status: "running" }),
        qualityJob({
          job_id: "bf_failed",
          status: "failed",
          current_range: null,
          error: {
            code: "interrupted",
            message: "upstream · failed：timeout",
            details: {},
          },
        }),
      ],
    });

    // Narrow desktop: the wide gap and job tables scroll locally instead of
    // making the whole page horizontally scrollable.
    await page.setViewportSize({ width: 1024, height: 900 });
    await page.goto("/quality");

    const gaps = page.getByRole("region", {
      name: "持久 Unrecoverable Backfill Gap",
    });
    await expect(gaps.getByText("gap_open_1")).toBeVisible();
    await expect(gaps.getByText(/首次 2026-09-01 01:05:00 UTC/)).toBeVisible();
    await expect(
      gaps.getByText(/最近 2026-11-09 12:00:00 UTC/),
    ).toBeVisible();
    await expect(gaps.getByText(/关联 Job bf_failed_1/)).toBeVisible();
    await expect(
      gaps.getByText("history_unsupported", { exact: true }),
    ).toBeVisible();

    const jobs = page.getByRole("region", { name: "Backfill Job" });
    await expect(
      jobs.getByText("bf_running", { exact: true }),
    ).toBeVisible();
    await expect(
      jobs.getByText("2026-09-01 06:00:00 UTC – 2026-09-01 12:00:00 UTC"),
    ).toBeVisible();
    await expect(
      jobs.getByText("进度不确定，契约未提供百分比").first(),
    ).toBeVisible();
    await expect(jobs.getByRole("progressbar")).toHaveAttribute(
      "aria-valuetext",
      "进度不确定",
    );
    await expect(jobs.getByText("扫描").first()).toBeVisible();
    await expect(jobs.getByText("3").first()).toBeVisible();

    const raw = jobs.locator('[data-raw-evidence="true"]', {
      hasText: "upstream · failed：timeout",
    });
    await expect(raw).toHaveText("upstream · failed：timeout");
    await expect(jobs.getByText(/已中断/)).toBeVisible();

    for (const region of [gaps, jobs]) {
      const scroller = region
        .locator('[data-slot="table-container"]')
        .first();
      const widths = await scroller.evaluate((element) => ({
        scrollWidth: element.scrollWidth,
        clientWidth: element.clientWidth,
      }));
      expect(widths.scrollWidth).toBeGreaterThan(widths.clientWidth);
    }
    await expectNoHorizontalOverflow(page);
    await expectNoBannedUiSymbols(page);
  });

  test("creating a Job requires reason and confirmation and posts the exact body", async ({
    page,
  }) => {
    await installQualityAdmin(page, { gaps: [], jobs: [] });

    let posted: Record<string, unknown> | null = null;
    let headers: Record<string, string> = {};
    await page.route("**/api/v1/admin/backfill-jobs", async (route) => {
      posted = (await route.request().postDataJSON()) as Record<
        string,
        unknown
      >;
      headers = route.request().headers();
      await json(
        route,
        202,
        qualityJob({
          job_id: "bf_created",
          status: "queued",
          interval: "1h",
          reason: "recover exchange outage",
        }),
      );
    });

    await page.setViewportSize({ width: 1024, height: 900 });
    await page.goto("/quality");
    const jobs = page.getByRole("region", { name: "Backfill Job" });
    await expect(jobs.getByText("没有 Backfill Job")).toBeVisible();
    await fillBackfillJobForm(page);

    // Validation is inline; the confirmation dialog is a separate deliberate step.
    await jobs.getByRole("button", { name: "创建 Backfill Job" }).click();
    const dialog = page.getByRole("dialog", {
      name: "确认触发 Backfill Job",
    });
    await expect(dialog).toBeVisible();
    await expectNoHorizontalOverflow(page);
    await expect(dialog).toContainText("补采 1m + 内部 Bar Aggregation");
    await expect(dialog).toContainText("不是调用原生高周期接口");
    await expect(
      dialog.getByRole("button", { name: "确认创建 Job" }),
    ).toBeEnabled();
    await dialog.getByRole("button", { name: "确认创建 Job" }).click();

    await expect(
      jobs.getByText(/已创建 Backfill Job bf_created/),
    ).toBeVisible();
    expect(posted).toMatchObject({
      instrument_id: "binance:BTC/USDT",
      interval: "1h",
      start: "2026-09-01T00:00:00.000Z",
      end: "2026-09-02T00:00:00.000Z",
      reason: "recover exchange outage",
      confirm: true,
    });
    expect(headers["x-csrf-token"]).toBe("csrf-token");
    expect(headers["idempotency-key"]).toBeTruthy();
    await expectNoBannedUiSymbols(page);
  });

  test("cancel keeps written Revisions and retry creates a linked Job without rewriting the old row", async ({
    page,
  }) => {
    await installQualityAdmin(page, {
      gaps: [],
      jobs: [
        qualityJob({ job_id: "bf_run", status: "running" }),
        qualityJob({
          job_id: "bf_failed",
          status: "failed",
          current_range: null,
          error: { code: "interrupted", message: "restart", details: {} },
        }),
      ],
    });

    let cancelBody: unknown = null;
    let retried = false;
    await page.route(
      "**/api/v1/admin/backfill-jobs/bf_run/cancel",
      async (route) => {
        cancelBody = await route.request().postDataJSON();
        await json(
          route,
          202,
          qualityJob({ job_id: "bf_run", status: "cancelling" }),
        );
      },
    );
    await page.route(
      "**/api/v1/admin/backfill-jobs/bf_failed/retry",
      async (route) => {
        retried = true;
        await json(
          route,
          202,
          qualityJob({
            job_id: "bf_retry_new",
            status: "queued",
            retry_of_job_id: "bf_failed",
          }),
        );
      },
    );

    await page.goto("/quality");
    const jobs = page.getByRole("region", { name: "Backfill Job" });
    await expect(jobs.getByText("bf_run", { exact: true })).toBeVisible();

    await jobs.getByRole("button", { name: "取消 Job bf_run" }).click();
    const cancelDialog = page.getByRole("dialog", {
      name: "确认取消 Backfill Job",
    });
    await cancelDialog.getByLabel("操作原因").fill("stop backfill");
    await cancelDialog.getByRole("button", { name: "确认取消 Job" }).click();
    await expect(jobs.getByText("取消请求已接受")).toBeVisible();
    await expect(
      jobs.getByText(/已写入的 Revision 不会撤回/),
    ).toBeVisible();
    expect(cancelBody).toEqual({ reason: "stop backfill" });

    await jobs.getByRole("button", { name: "重试 Job bf_failed" }).click();
    const retryDialog = page.getByRole("dialog", {
      name: "确认重试 Backfill Job",
    });
    await retryDialog.getByLabel("操作原因").fill("retry it");
    await retryDialog.getByRole("button", { name: "确认重试 Job" }).click();
    await expect(
      jobs.getByText(/已创建关联的新 Job bf_retry_new/),
    ).toBeVisible();
    await expect(jobs.getByText(/旧 Job 状态与计数不变/)).toBeVisible();
    expect(retried).toBe(true);
    // The old row still reports its original state.
    await expect(jobs.getByText("failed", { exact: true })).toBeVisible();
  });

  test("a 409 active-job conflict stays inline and never shows success", async ({
    page,
  }) => {
    await installQualityAdmin(page, { gaps: [], jobs: [] });
    let createAttempts = 0;
    await page.route("**/api/v1/admin/backfill-jobs", async (route) => {
      createAttempts += 1;
      await json(
        route,
        409,
        {
          error: {
            code: "conflict",
            message: "an active backfill job already exists",
            details: { reason: "active_job", active_job_id: "bf_active_1" },
            request_id: "req_conflict_ui",
          },
        },
        { "X-Request-ID": "req_conflict_ui" },
      );
    });

    await page.goto("/quality");
    const jobs = page.getByRole("region", { name: "Backfill Job" });
    await expect(jobs.getByText("没有 Backfill Job")).toBeVisible();
    await fillBackfillJobForm(page);
    await jobs.getByRole("button", { name: "创建 Backfill Job" }).click();
    const dialog = page.getByRole("dialog", {
      name: "确认触发 Backfill Job",
    });
    await dialog.getByRole("button", { name: "确认创建 Job" }).click();

    await expect(jobs.getByText("冲突，本次未创建 Job")).toBeVisible();
    await expect(
      jobs.getByText(/已有运行中的 Job bf_active_1/),
    ).toBeVisible();
    await expect(jobs.getByText(/req_conflict_ui/)).toBeVisible();
    await expect(jobs.getByText(/已创建 Backfill Job/)).toHaveCount(0);
    expect(createAttempts).toBe(1);
    await expectNoBannedUiSymbols(page);
  });
});
