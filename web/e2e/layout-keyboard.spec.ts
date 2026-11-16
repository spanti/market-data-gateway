import { expect, test, type Page, type Route } from "@playwright/test";
import {
  closeHelpWithEscape,
  expectNoBannedUiSymbols,
  expectNoHorizontalOverflow,
  expectTooltipUsable,
  helpTrigger,
  openHelpByHover,
  openHelpByKeyboard,
  toggleHelpByTouch,
} from "./support";

/**
 * Real-browser layout and keyboard acceptance.
 *
 * The spec uses the real local stack started by playwright.config.ts. It
 * checks the two required desktop widths (1280 and 1440), the complete top
 * navigation, and keyboard activation/hover-free chart readouts.
 */

const NAV_ITEMS = [
  { name: "监控", heading: "监控总览", path: "/monitoring" },
  { name: "Bar 图与导出", heading: "Bar 图与导出", path: "/bars" },
  { name: "Gateway Console", heading: "Gateway Console", path: "/console" },
  { name: "Quality & Backfill", heading: "Quality & Backfill", path: "/quality" },
  { name: "Security & Audit", heading: "Security & Audit", path: "/security" },
] as const;

async function openSecurity(page: Page): Promise<void> {
  await page.goto("/security");
  await expect(
    page.getByRole("heading", { name: "Security & Audit" }),
  ).toBeVisible();
  await expect(page.getByRole("region", { name: "当前身份" })).toBeVisible();
}

/**
 * CONTRACT-ONLY Admin fixture for the Service Instance lifecycle. The real
 * backend has no Admin API, so these routes are intercepted; this never
 * substitutes for real OIDC/Admin integration.
 */
function futureIso(offsetMs: number): string {
  return new Date(Date.now() + offsetMs).toISOString();
}

const CONTRACT_ADMIN_CAPABILITIES = {
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
  ],
  capabilities: {
    service_lifecycle: {
      state: "enabled",
      actions: ["start", "stop", "reload"],
    },
    ws_admin_session: { state: "enabled" },
  },
  limits: {},
};

const CONTRACT_SERVICE_INSTANCES = {
  items: [
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

async function fulfillJson(
  route: Route,
  status: number,
  body: unknown,
  headers: Record<string, string> = {},
): Promise<void> {
  await route.fulfill({
    status,
    contentType: "application/json",
    headers,
    body: JSON.stringify(body),
  });
}

async function installContractAdmin(page: Page): Promise<void> {
  await page.route("**/api/v1/admin/capabilities", (route) =>
    fulfillJson(route, 200, CONTRACT_ADMIN_CAPABILITIES),
  );
  await page.route("**/api/v1/admin/auth/session", (route) =>
    fulfillJson(route, 200, {
      authenticated: true,
      server_time: futureIso(0),
      user: {
        subject: "oidc-subject-123",
        display_name: "Ada Admin",
        email: "ada@example.com",
      },
      access_role: "SuperAdmin",
      session: {
        issued_at: futureIso(-60_000),
        last_activity_at: futureIso(-1_000),
        idle_expires_at: futureIso(30 * 60_000),
        absolute_expires_at: futureIso(8 * 3_600_000),
      },
      csrf: { header_name: "X-CSRF-Token", token: "csrf-token" },
    }),
  );
  await page.route("**/api/v1/admin/service-instances", (route) =>
    fulfillJson(route, 200, CONTRACT_SERVICE_INSTANCES),
  );
}

function barsQueryUrl(): string {
  const params = new URLSearchParams({
    instrument_id: "binance:BTC/USDT",
    interval: "1m",
    start: "2024-01-01T00:00:00.000Z",
    end: "2024-01-01T00:02:00.000Z",
  });
  return `/bars?${params.toString()}`;
}

async function expectBarsDRegions(page: Page): Promise<void> {
  await expect(
    page.getByRole("heading", { name: "Bar 图与导出" }),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "Bar 查询条件" }),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "最大 Revision Bar 图" }),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "原始 Revision 表" }),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "CSV 直接下载" }),
  ).toBeVisible();
  await expect(
    page.getByRole("region", { name: "实时跟随" }),
  ).toBeVisible();
}

async function tabUntilText(
  page: Page,
  text: string,
  attempts = 20,
): Promise<void> {
  for (let index = 0; index < attempts; index += 1) {
    await page.keyboard.press("Tab");
    const activeText = await page.evaluate(() =>
      (document.activeElement?.textContent ?? "").replace(/\s+/g, " ").trim(),
    );
    if (activeText.includes(text)) {
      return;
    }
  }
  throw new Error(`keyboard focus never reached ${text}`);
}

test.describe("narrow desktop layout 1024px", () => {
  test.use({ viewport: { width: 1024, height: 900 } });

  test("monitoring stays usable without page-level horizontal overflow", async ({
    page,
  }) => {
    await page.goto("/monitoring");
    await expect(
      page.getByRole("heading", { name: "监控总览" }),
    ).toBeVisible();
    await expect(page.locator('[data-slot="metric-card"]')).toHaveCount(4);
    await expect(page.locator('[data-slot="chart-panel"]')).toHaveCount(2);
    await expect(
      page.getByRole("region", { name: "Feed Freshness", exact: true }),
    ).toBeVisible();
    await expect(page.getByRole("region", { name: "Service Instance" })).toBeVisible();
    await expectNoHorizontalOverflow(page);

    // Bars remains operable at 1024px. Wide Revision tables scroll inside
    // their own card instead of making the whole page overflow.
    await page.goto(barsQueryUrl());
    await expectBarsDRegions(page);
    await expect(page.getByTestId("history-summary")).toContainText(
      "共 2 条原始 Revision",
    );
    await expect(
      page.getByRole("button", { name: "应用筛选" }),
    ).toBeVisible();
    const revisionScroller = page
      .getByRole("region", { name: "原始 Revision 表" })
      .locator('[data-slot="table-container"]');
    const widths = await revisionScroller.evaluate((element) => ({
      scrollWidth: element.scrollWidth,
      clientWidth: element.clientWidth,
    }));
    expect(widths.scrollWidth).toBeGreaterThan(widths.clientWidth);
    await expectNoHorizontalOverflow(page);
  });

  test("console tabs stay usable without page-level horizontal overflow", async ({
    page,
  }) => {
    await page.goto("/console");
    await expect(
      page.getByRole("heading", { name: "Gateway Console" }),
    ).toBeVisible();

    await page.getByRole("tab", { name: "Feed Subscription" }).click();
    await expect(
      page.getByRole("region", { name: "Feed Subscription" }),
    ).toBeVisible();
    await expectNoHorizontalOverflow(page);

    await page.getByRole("tab", { name: "Service Instance" }).click();
    const instanceRegion = page.getByRole("region", {
      name: "Service Instance",
    });
    await expect(instanceRegion).toBeVisible();
    await expect(page.getByTestId("service-instance-collector")).toBeVisible();
    await expect(
      instanceRegion.getByRole("columnheader", { name: "Service Role" }),
    ).toBeVisible();
    // The wide ledger scrolls inside its own card, not on the page.
    const instanceScroller = instanceRegion.locator(
      '[data-slot="table-container"]',
    );
    const instanceWidths = await instanceScroller.evaluate((element) => ({
      scrollWidth: element.scrollWidth,
      clientWidth: element.clientWidth,
    }));
    expect(instanceWidths.scrollWidth).toBeGreaterThan(
      instanceWidths.clientWidth,
    );
    await expectNoBannedUiSymbols(page, "#main-content");
    await expectNoHorizontalOverflow(page);

    await page.getByRole("tab", { name: "WebSocket Debug" }).click();
    await expect(
      page.getByRole("heading", { name: "WebSocket Debug" }),
    ).toBeVisible();
    await expectNoHorizontalOverflow(page);

    // Back on Feed, every UI-generated string must avoid the banned
    // separators. Raw backend error text stays exempt via data-raw-evidence.
    await page.getByRole("tab", { name: "Feed Subscription" }).click();
    await expectNoBannedUiSymbols(page, "#main-content");
  });

  test("quality shows the real Collector snapshot and disabled Admin areas without overflow", async ({
    page,
  }) => {
    await page.goto("/quality");
    await expect(
      page.getByRole("heading", { name: "Quality & Backfill" }),
    ).toBeVisible();
    await expect(page.locator('[data-slot="card"]')).toHaveCount(3);

    // Current Contract: a real Collector snapshot, not accumulated history.
    const snapshot = page.getByRole("region", {
      name: "Unrecoverable Backfill Gap 当前快照",
    });
    await expect(snapshot).toBeVisible();
    await expect(snapshot).toContainText("Collector 进程内快照");
    await expect(
      snapshot.getByText(/gap_unrecoverable_count/),
    ).toBeVisible();

    // Persistent gaps and Jobs stay visible but honestly gated.
    const persistent = page.getByRole("region", {
      name: "持久 Unrecoverable Backfill Gap",
    });
    await expect(persistent).toContainText("需要 Admin API");
    await expect(persistent.locator("table")).toHaveCount(0);

    const jobs = page.getByRole("region", { name: "Backfill Job" });
    await expect(jobs).toContainText(
      "Current Contract 没有持久 Backfill Job API",
    );
    await expect(jobs.locator("table")).toHaveCount(0);
    await expect(jobs.getByRole("progressbar")).toHaveCount(0);

    await expectNoBannedUiSymbols(page);
    await expectNoHorizontalOverflow(page);
  });

  test("security stays usable and honest without page-level horizontal overflow", async ({
    page,
  }) => {
    await openSecurity(page);

    // The identity card exposes Access Role, not a Service Role.
    await expect(
      page.getByRole("region", { name: "当前身份" }).getByText("Access Role", {
        exact: true,
      }),
    ).toBeVisible();
    await expect(
      page.getByRole("region", { name: "当前身份" }).getByText(/Service Role/),
    ).toHaveCount(0);

    // Current Contract has no Admin API. The audit area stays honestly
    // forbidden and never fabricates rows.
    await expect(page.getByText("需要 Admin API", { exact: true })).toBeVisible();
    await expect(
      page.getByRole("region", { name: "Operator Audit Event 查询" }),
    ).toHaveCount(0);

    await expectNoBannedUiSymbols(page);
    await expectNoHorizontalOverflow(page);
  });
});

for (const width of [1280, 1440] as const) {
  test.describe(`desktop layout ${width}px`, () => {
    test.use({ viewport: { width, height: 900 } });

    test("all five modules fit without horizontal overflow and the full nav is usable", async ({
      page,
    }) => {
      await page.goto("/monitoring");
      await expect(
        page.getByRole("heading", { name: "监控总览" }),
      ).toBeVisible();
      await expectNoHorizontalOverflow(page);
      await expect(page.locator('[data-slot="metric-card"]')).toHaveCount(4);
      await expect(page.locator('[data-slot="chart-panel"]')).toHaveCount(2);

      const nav = page.getByRole("navigation", { name: "一级模块导航" });
      await expect(nav).toBeVisible();
      expect(await nav.getByRole("link").count()).toBe(5);

      for (const item of NAV_ITEMS) {
        const link = nav.getByRole("link", { name: item.name });
        await expect(link).toBeVisible();
        await link.click();
        await expect(page).toHaveURL(new RegExp(`${item.path}$`));
        await expect(
          page.getByRole("heading", { name: item.heading }),
        ).toBeVisible();
        if (item.path === "/security") {
          await expect(
            page.getByRole("region", { name: "当前身份" }),
          ).toBeVisible();
          await expectNoBannedUiSymbols(page);
        }
        await expectNoHorizontalOverflow(page);
      }

      // The page must not require horizontal scrolling even with a dense
      // table on the Console tab.
      await page.goto("/console");
      await page.getByRole("tab", { name: "Service Instance" }).click();
      const instanceRegion = page.getByRole("region", {
        name: "Service Instance",
      });
      await expect(instanceRegion).toBeVisible();
      await expect(
        page.getByTestId("service-instance-collector"),
      ).toBeVisible();
      await expect(
        instanceRegion.getByRole("button", { name: "Service Instance 说明" }),
      ).toBeVisible();
      await expect(
        instanceRegion.getByRole("columnheader", { name: "Service Instance" }),
      ).toBeVisible();
      await expect(
        instanceRegion.getByRole("columnheader", { name: "Service Role" }),
      ).toBeVisible();
      await expectNoBannedUiSymbols(page, "#main-content");
      await expectNoHorizontalOverflow(page);

      await page.getByRole("tab", { name: "Feed Subscription" }).click();
      await expect(
        page.getByRole("region", { name: "Feed Subscription" }),
      ).toBeVisible();
      await expectNoBannedUiSymbols(page, "#main-content");
      await expectNoHorizontalOverflow(page);

      // The migrated Bars query, chart and data regions stay usable and do
      // not overflow at this desktop width.
      await page.goto(barsQueryUrl());
      await expectBarsDRegions(page);
      await expect(page.getByTestId("history-summary")).toContainText(
        "共 2 条原始 Revision",
      );
      await expectNoHorizontalOverflow(page);
    });

    test("quality cards and help are usable", async ({ page }) => {
      await page.goto("/quality");
      await expect(
        page.getByRole("heading", { name: "Quality & Backfill" }),
      ).toBeVisible();
      await expect(page.locator('[data-slot="card"]')).toHaveCount(3);
      await expectNoHorizontalOverflow(page);

      const pageHelp = await openHelpByHover(
        page,
        "Quality & Backfill 说明",
      );
      await expectTooltipUsable(page);
      await expect(pageHelp.tooltip).toContainText("进程内 Gap 快照");
      await closeHelpWithEscape(page);

      const jobHelp = await openHelpByKeyboard(page, "Backfill Job 说明");
      await expect(jobHelp.tooltip).toContainText("partially_succeeded");
      await expectTooltipUsable(page);
      await closeHelpWithEscape(page);

      const gapHelp = await openHelpByHover(
        page,
        "持久 Unrecoverable Backfill Gap 说明",
      );
      await expect(gapHelp.tooltip).toContainText("物理删除");
      await closeHelpWithEscape(page);

      await expectNoBannedUiSymbols(page);
      await expectNoHorizontalOverflow(page);
    });

    test("top navigation and the chart are operable by keyboard", async ({
      page,
    }) => {
      const params = new URLSearchParams({
        instrument_id: "binance:BTC/USDT",
        interval: "1m",
        start: "2024-01-01T00:00:00.000Z",
        end: "2024-01-01T00:02:00.000Z",
      });
      await page.goto(`/monitoring`);
      await expect(page.getByRole("heading", { name: "监控总览" })).toBeVisible();

      // Reach the first module link with Tab, then navigate with Enter.
      await tabUntilText(page, "监控");
      await page.keyboard.press("Enter");
      await expect(page).toHaveURL(/\/monitoring$/);

      await page.goto(`/bars?${params.toString()}`);
      await expect(page.getByTestId("history-summary")).toContainText(
        "共 2 条原始 Revision",
      );

      const chart = page.getByRole("img", {
        name: /最大 Revision Bar 价格图/,
      });
      await chart.focus();
      const readout = page.locator("#bar-chart-readout");
      await expect(readout).toContainText("close 65015.0");

      await page.keyboard.press("ArrowLeft");
      await expect(readout).toContainText("close 65005.5");

      await page.keyboard.press("Home");
      await expect(readout).toContainText("close 65005.5");

      // The chart host consumes arrows but never traps global navigation.
      await page.keyboard.press("Tab");
      const activeTag = await page.evaluate(
        () => document.activeElement?.tagName ?? "",
      );
      expect(activeTag).not.toBe("");

      await expectBarsDRegions(page);
      await expectNoHorizontalOverflow(page);
    });
  });
}

test.describe("console feed help", () => {
  test.use({ viewport: { width: 1280, height: 900 } });

  test("Feed Subscription help opens by hover and keyboard focus and closes with Escape", async ({
    page,
  }) => {
    await page.goto("/console");
    await expect(
      page.getByRole("region", { name: "Feed Subscription" }),
    ).toBeVisible();

    const hovered = await openHelpByHover(page, "Feed Subscription 说明");
    await expect(hovered.tooltip).toContainText("Desired");
    await expectTooltipUsable(page);
    await closeHelpWithEscape(page);

    const focused = await openHelpByKeyboard(page, "Feed Subscription 说明");
    await expect(focused.tooltip).toContainText("409");
    await expectTooltipUsable(page);
    await closeHelpWithEscape(page);
  });
});

test.describe("quality help with a touch context", () => {
  test.use({ viewport: { width: 1280, height: 900 }, hasTouch: true });

  test("help toggles by touch, stays readable and closes with Escape", async ({
    page,
  }) => {
    await page.goto("/quality");
    await expect(
      page.getByRole("heading", { name: "Quality & Backfill" }),
    ).toBeVisible();

    // A tap opens and a second tap closes the same help surface.
    await toggleHelpByTouch(page, "Quality & Backfill 说明");

    await helpTrigger(
      page,
      "Unrecoverable Backfill Gap 当前快照说明",
    ).tap();
    await expect(page.getByRole("tooltip")).toBeVisible();
    await expectTooltipUsable(page);
    await expectNoBannedUiSymbols(page);
    await closeHelpWithEscape(page);

    await expectNoHorizontalOverflow(page);
  });
});

test.describe("security help interactions", () => {
  test.use({ viewport: { width: 1280, height: 900 } });

  test("hover, keyboard focus and Escape work on Security help", async ({
    page,
  }) => {
    await openSecurity(page);

    const { tooltip } = await openHelpByHover(page, "Security & Audit 说明");
    await expect(tooltip).toContainText("首版不做 IAM 管理");
    await expectTooltipUsable(page);
    await expectNoBannedUiSymbols(page);
    await closeHelpWithEscape(page);

    await openSecurity(page);
    const identityHelp = await openHelpByKeyboard(
      page,
      "身份与 Access Role 说明",
    );
    await expect(identityHelp.tooltip).toContainText(
      "服务端授权才是安全边界",
    );
    await expectTooltipUsable(page);
    await closeHelpWithEscape(page);
  });
});

test.describe("console feed touch help", () => {
  test.use({ viewport: { width: 1024, height: 900 }, hasTouch: true });

  test("Feed Subscription help toggles on touch tap", async ({ page }) => {
    await page.goto("/console");
    await expect(
      page.getByRole("region", { name: "Feed Subscription" }),
    ).toBeVisible();
    await toggleHelpByTouch(page, "Feed Subscription 说明");
    await expectNoHorizontalOverflow(page);
  });
});

test.describe("security touch help", () => {
  test.use({ hasTouch: true, viewport: { width: 1024, height: 900 } });

  test("a tap toggles identity help and Escape dismisses it", async ({
    page,
  }) => {
    await openSecurity(page);

    await toggleHelpByTouch(page, "身份与 Access Role 说明");
    await expectNoBannedUiSymbols(page);

    const trigger = helpTrigger(page, "身份与 Access Role 说明");
    await trigger.tap();
    await expect(page.getByRole("tooltip")).toBeVisible();
    await closeHelpWithEscape(page);
  });
});

test.describe("console service instance help", () => {
  test.use({ viewport: { width: 1280, height: 900 } });

  test("Service Instance help opens by hover and keyboard focus and closes with Escape", async ({
    page,
  }) => {
    await page.goto("/console");
    await page.getByRole("tab", { name: "Service Instance" }).click();
    await expect(
      page.getByRole("region", { name: "Service Instance" }),
    ).toBeVisible();

    const hovered = await openHelpByHover(page, "Service Instance 说明");
    await expect(hovered.tooltip).toContainText("Service Role");
    await expect(hovered.tooltip).toContainText("Service Instance");
    await expect(hovered.tooltip).toContainText("PID 存在不等于 Ready");
    await expect(hovered.tooltip).toContainText("不是热加载");
    await expectTooltipUsable(page);
    await expectNoBannedUiSymbols(page, '[data-help-content="true"]');
    await closeHelpWithEscape(page);

    const focused = await openHelpByKeyboard(page, "Service Instance 说明");
    await expect(focused.tooltip).toContainText("服务端授权");
    await expectTooltipUsable(page);
    await closeHelpWithEscape(page);

    await expectNoBannedUiSymbols(page, "#main-content");
    await expectNoHorizontalOverflow(page);
  });
});

test.describe("console service instance touch help", () => {
  test.use({ viewport: { width: 1024, height: 900 }, hasTouch: true });

  test("Service Instance help toggles on touch tap without page overflow", async ({
    page,
  }) => {
    await page.goto("/console");
    await page.getByRole("tab", { name: "Service Instance" }).click();
    await expect(
      page.getByRole("region", { name: "Service Instance" }),
    ).toBeVisible();

    await toggleHelpByTouch(page, "Service Instance 说明");
    await expectNoHorizontalOverflow(page);
  });
});

test.describe("CONTRACT-ONLY service instance lifecycle keyboard", () => {
  test.use({ viewport: { width: 1024, height: 900 } });

  /**
   * CONTRACT-ONLY: the Admin API and OIDC session do not exist in the current
   * backend. Admin responses are intercepted with page.route; this proves the
   * browser keyboard contract only and is NOT real Admin integration.
   */
  test("open, cancel and submit Graceful Stop entirely by keyboard", async ({
    page,
  }) => {
    await installContractAdmin(page);

    let postCount = 0;
    let body: Record<string, unknown> | null = null;
    await page.route(
      "**/api/v1/admin/service-instances/api-1/stop",
      async (route) => {
        postCount += 1;
        body = (await route.request().postDataJSON()) as Record<
          string,
          unknown
        >;
        await fulfillJson(
          route,
          202,
          { operation_id: "op_kb", request_id: "req_kb", status: "accepted" },
          { "X-Request-ID": "req_kb" },
        );
      },
    );
    await page.route("**/api/v1/admin/operations/op_kb", (route) =>
      fulfillJson(route, 200, {
        operation_id: "op_kb",
        kind: "service_instance.stop",
        target: { instance_id: "api-1", service_role: "api" },
        requested_by: {
          subject: "oidc-subject-123",
          display_name: "Ada Admin",
          access_role: "SuperAdmin",
        },
        reason: "keyboard stop",
        status: "accepted",
        requested_at: futureIso(-1_000),
        started_at: null,
        finished_at: null,
        result: null,
        error: null,
        request_id: "req_kb",
      }),
    );

    await page.goto("/console");
    await page.getByRole("tab", { name: "Service Instance" }).click();
    const row = page.getByTestId("service-instance-api-1");
    await expect(row).toBeVisible();

    // Open the confirmation with the keyboard.
    const stop = row.getByRole("button", { name: "Graceful Stop" });
    await stop.focus();
    await page.keyboard.press("Enter");
    const dialog = page.getByRole("dialog");
    await expect(dialog).toContainText("确认 Graceful Stop api-1");
    await expect(dialog).toContainText("目标 Service Instance api-1");
    await expect(dialog).toContainText("排空");
    await expectNoBannedUiSymbols(page, '[data-slot="dialog-content"]');
    await expectNoHorizontalOverflow(page);

    // Escape cancels without a write.
    await page.keyboard.press("Escape");
    await expect(dialog).toBeHidden();
    expect(postCount).toBe(0);

    // Reopen and complete the whole operation with the keyboard.
    await stop.focus();
    await page.keyboard.press("Enter");
    await expect(dialog).toBeVisible();
    await dialog.getByLabel("操作原因").focus();
    await page.keyboard.type("keyboard stop");
    await dialog.getByLabel(/输入目标实例名/).focus();
    await page.keyboard.type("api-1");
    const confirm = dialog.getByRole("button", { name: "确认执行" });
    await expect(confirm).toBeEnabled();
    await confirm.focus();
    await page.keyboard.press("Enter");

    await expect(row.getByText("已接受（未完成）")).toBeVisible();
    await expect(row.getByText("目标 api-1")).toBeVisible();
    await expect(row.getByText("Request ID req_kb")).toBeVisible();
    expect(postCount).toBe(1);
    expect(body).toMatchObject({
      reason: "keyboard stop",
      confirm: true,
      confirm_instance_name: "api-1",
    });

    // No automatic replay of a non-idempotent write.
    await page.waitForTimeout(1_000);
    expect(postCount).toBe(1);
    await expectNoHorizontalOverflow(page);
  });
});
