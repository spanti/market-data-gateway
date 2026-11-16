import { expect, test, type Page, type Route } from "@playwright/test";
import { expectNoBannedUiSymbols, expectNoHorizontalOverflow } from "./support";

/**
 * CONTRACT-ONLY: the Admin Export Job API does not exist in the current
 * backend. These flows run in a real browser against Playwright request
 * interception, not against the local FastAPI/Admin service. They must not be
 * reported as real Admin/OIDC end-to-end evidence.
 *
 * Real-stack coverage for the same panel (Current Contract disabled state,
 * copy and help interaction) lives in bars-csv.spec.ts and
 * bars-chart-cursor.spec.ts.
 */

const INSTRUMENT = "binance:BTC/USDT";
const START = "2024-01-01T00:00:00.000Z";
const END_45_DAYS = "2024-02-15T00:00:00.000Z";
const SPLIT_START = "2022-01-01T00:00:00.000Z";
const SPLIT_END_800_DAYS = "2024-03-11T00:00:00.000Z";

const EXPORT_JOBS_PATH = /\/api\/v1\/admin\/export-jobs(?:\?.*)?$/;
const EXPORT_JOB_DOWNLOAD_PATH =
  /\/api\/v1\/admin\/export-jobs\/[^/]+\/download$/;

function futureIso(offsetMs: number): string {
  return new Date(Date.now() + offsetMs).toISOString();
}

function barsUrl(start: string, end: string): string {
  const params = new URLSearchParams({
    instrument_id: INSTRUMENT,
    interval: "1m",
    start,
    end,
  });
  return `/bars?${params.toString()}`;
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
  adapters: [],
  capabilities: {
    oidc_session: { state: "enabled" },
    export_job: {
      state: "enabled",
      formats: ["csv"],
      min_days: 31,
      max_days: 366,
      retention_s: 86400,
      max_running_per_user: 1,
    },
  },
};

function sessionBody() {
  return {
    authenticated: true,
    server_time: futureIso(0),
    user: {
      subject: "oidc-subject-123",
      display_name: "Ada Operator",
      email: "ada@example.com",
    },
    access_role: "Operator",
    session: {
      issued_at: futureIso(-60_000),
      last_activity_at: futureIso(-1_000),
      idle_expires_at: futureIso(30 * 60_000),
      absolute_expires_at: futureIso(8 * 3_600_000),
    },
    csrf: { header_name: "X-CSRF-Token", token: "csrf-token" },
  };
}

async function json(route: Route, status: number, body: unknown) {
  await route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify(body),
  });
}

async function installAdmin(page: Page): Promise<void> {
  await page.route("**/api/v1/admin/capabilities", (route) =>
    json(route, 200, capabilityDocument),
  );
  await page.route("**/api/v1/admin/auth/session", (route) =>
    json(route, 200, sessionBody()),
  );
}

function queuedJob() {
  return {
    job_id: "exp_e2e_01",
    status: "queued",
    instrument_id: INSTRUMENT,
    interval: "1m",
    start: START,
    end: END_45_DAYS,
    range_days: 45,
    format: "csv",
    created_at: futureIso(-1_000),
    created_by: {
      subject: "oidc-subject-123",
      display_name: "Ada Operator",
    },
    progress: {
      processed_rows: 0,
      processed_bytes: 0,
      total_rows: null,
      percent: null,
      indeterminate: true,
    },
    result: null,
    error: null,
    cancelled_at: null,
    expires_at: null,
    download_url: null,
  };
}

function succeededJob() {
  return {
    ...queuedJob(),
    status: "succeeded",
    progress: {
      processed_rows: 450000,
      processed_bytes: 12_345_678,
      total_rows: 450000,
      percent: 100,
      indeterminate: false,
    },
    result: {
      file_name: "binance_BTCUSDT_1m.csv",
      content_type: "text/csv; charset=utf-8",
      size_bytes: 12_345_678,
      sha256: "e3b0c44298fc1c149afbf4c8996fb924abc",
      row_count: 450000,
    },
    expires_at: futureIso(3_600_000),
    download_url: "/api/v1/admin/export-jobs/exp_e2e_01/download",
  };
}

function failedJobWithRawError() {
  return {
    ...queuedJob(),
    status: "failed",
    error: {
      code: "internal_error",
      message: "upstream · failed：timeout",
      details: {},
    },
  };
}

function exportPanel(page: Page) {
  return page.getByRole("region", { name: "Export Job" });
}

test.describe("CONTRACT-ONLY Export Job lifecycle (Admin API intercepted)", () => {
  test("creates, tracks and downloads a job with the keyboard", async ({
    page,
  }) => {
    await installAdmin(page);

    let jobs: unknown[] = [];
    let postBody: Record<string, unknown> | null = null;
    let postCount = 0;
    await page.route(EXPORT_JOBS_PATH, async (route) => {
      if (route.request().method() === "POST") {
        postCount += 1;
        postBody = route.request().postDataJSON() as Record<string, unknown>;
        jobs = [queuedJob()];
        await json(route, 202, queuedJob());
        return;
      }
      await json(route, 200, {
        items: jobs,
        page: { next_cursor: null, has_more: false },
      });
    });
    await page.route(EXPORT_JOB_DOWNLOAD_PATH, (route) =>
      route.fulfill({
        status: 200,
        headers: {
          "Content-Type": "text/csv; charset=utf-8",
          "Content-Disposition": 'attachment; filename="binance_BTCUSDT_1m.csv"',
        },
        body: "open,close\n65000.1,65005.5\n",
      }),
    );

    await page.goto(barsUrl(START, END_45_DAYS));
    const panel = exportPanel(page);
    await expect(panel).toBeVisible();
    await expect(
      panel.getByText("当前没有可见的 Export Job"),
    ).toBeVisible();

    const create = page.getByRole("button", { name: "创建 Export Job" });
    await expect(create).toBeEnabled();
    await create.focus();
    await page.keyboard.press("Enter");

    await expect(panel.getByText("排队中")).toBeVisible();
    expect(postCount).toBe(1);
    expect(postBody).toEqual({
      instrument_id: INSTRUMENT,
      interval: "1m",
      start: START,
      end: END_45_DAYS,
      format: "csv",
    });
    await expect(create).toBeDisabled();
    await expect(panel.getByText(/已有运行中的 Export Job/)).toBeVisible();

    jobs = [succeededJob()];
    const refresh = page.getByRole("button", { name: "刷新任务列表" });
    await expect(refresh).toBeEnabled();
    await refresh.focus();
    await page.keyboard.press("Enter");

    await expect(panel.getByText("已完成")).toBeVisible();
    await expect(panel.getByText("binance_BTCUSDT_1m.csv")).toBeVisible();
    await expect(
      panel.getByText("e3b0c44298fc1c149afbf4c8996fb924abc"),
    ).toBeVisible();
    await expect(panel).toContainText(INSTRUMENT);
    await expect(panel).toContainText("45 天");
    await expect(panel).toContainText("Ada Operator");
    await expect(panel).toContainText(START);
    await expect(panel).toContainText(END_45_DAYS);

    const link = panel.getByRole("link", { name: /原生下载（支持 Range）/ });
    expect(await link.evaluate((element) => element.tagName)).toBe("A");
    const downloadPromise = page.waitForEvent("download");
    await link.focus();
    await page.keyboard.press("Enter");
    const download = await downloadPromise;
    expect(download.suggestedFilename()).toBe("binance_BTCUSDT_1m.csv");
    expect(postCount).toBe(1);
    await expectNoBannedUiSymbols(
      page,
      '[role="region"][aria-label="Export Job"]',
    );

    // A backend raw error is rendered verbatim and exempt from the UI symbol
    // ban; the surrounding panel copy remains separator-free.
    jobs = [failedJobWithRawError()];
    await refresh.focus();
    await page.keyboard.press("Enter");
    await expect(panel.getByText("失败", { exact: true })).toBeVisible();
    await expect(
      panel.locator('[data-raw-evidence="true"]', {
        hasText: "upstream · failed：timeout",
      }),
    ).toBeVisible();
    await expectNoBannedUiSymbols(
      page,
      '[role="region"][aria-label="Export Job"]',
    );
  });

  test("confirms a split chunk with the keyboard at 1024 without page overflow", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1024, height: 900 });
    await installAdmin(page);

    let jobs: unknown[] = [];
    let postBody: Record<string, unknown> | null = null;
    let postCount = 0;
    await page.route(EXPORT_JOBS_PATH, async (route) => {
      if (route.request().method() === "POST") {
        postCount += 1;
        postBody = route.request().postDataJSON() as Record<string, unknown>;
        jobs = [queuedJob()];
        await json(route, 202, queuedJob());
        return;
      }
      await json(route, 200, {
        items: jobs,
        page: { next_cursor: null, has_more: false },
      });
    });

    await page.goto(barsUrl(SPLIT_START, SPLIT_END_800_DAYS));
    const panel = exportPanel(page);
    await expect(panel).toBeVisible();
    await expect(
      panel.getByText(/需要拆分为 3 个 Export Job/),
    ).toBeVisible();
    await expect(
      panel.getByText(/不会合并为多 Instrument ZIP/),
    ).toBeVisible();

    const firstChunk = page.getByRole("button", {
      name: "创建第 1 段 Export Job",
    });
    await firstChunk.focus();
    await page.keyboard.press("Enter");

    const dialog = page.getByRole("dialog");
    await expect(dialog).toBeVisible();
    await expect(dialog).toContainText("确认创建单独分段 Export Job");
    await expect(dialog).toContainText("不会合并为多 Instrument ZIP");
    await expectNoBannedUiSymbols(page, '[data-slot="dialog-content"]');

    const confirmed = dialog.getByLabel(/我确认只创建所选分段/);
    await confirmed.focus();
    await page.keyboard.press("Space");
    const confirm = dialog.getByRole("button", { name: "确认创建该段" });
    await expect(confirm).toBeEnabled();
    await confirm.focus();
    await page.keyboard.press("Enter");

    await expect(panel.getByText("排队中")).toBeVisible();
    expect(postCount).toBe(1);
    expect(postBody).toEqual({
      instrument_id: INSTRUMENT,
      interval: "1m",
      start: SPLIT_START,
      end: "2023-01-02T00:00:00.000Z",
      format: "csv",
    });
    await expect(
      page.getByRole("button", { name: "创建第 2 段 Export Job" }),
    ).toBeDisabled();
    await expect(
      page.getByRole("button", { name: "创建第 3 段 Export Job" }),
    ).toBeDisabled();
    await expectNoHorizontalOverflow(page);
    await expectNoBannedUiSymbols(
      page,
      '[role="region"][aria-label="Export Job"]',
    );
  });

  test("keeps a raw backend create error verbatim with a visible Request ID", async ({
    page,
  }) => {
    await installAdmin(page);
    await page.route(EXPORT_JOBS_PATH, async (route) => {
      if (route.request().method() === "POST") {
        await json(route, 500, {
          error: {
            code: "internal_error",
            message: "upstream · failed：timeout",
            details: {},
            request_id: "raw-e2e-request-id",
          },
        });
        return;
      }
      await json(route, 200, {
        items: [],
        page: { next_cursor: null, has_more: false },
      });
    });

    await page.goto(barsUrl(START, END_45_DAYS));
    const panel = exportPanel(page);
    await expect(
      panel.getByText("当前没有可见的 Export Job"),
    ).toBeVisible();

    const create = page.getByRole("button", { name: "创建 Export Job" });
    await create.focus();
    await page.keyboard.press("Enter");

    await expect(panel.getByText("Export Job 创建失败")).toBeVisible();
    await expect(
      panel.locator('[data-raw-evidence="true"]', {
        hasText: "upstream · failed：timeout",
      }),
    ).toBeVisible();
    await expect(panel).toContainText("raw-e2e-request-id");
    await expectNoBannedUiSymbols(
      page,
      '[role="region"][aria-label="Export Job"]',
    );
  });
});
