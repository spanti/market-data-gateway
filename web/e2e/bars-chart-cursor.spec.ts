import { expect, test, type Page } from "@playwright/test";
import {
  barsQuery,
  BTC_INSTRUMENT,
  closeHelpWithEscape,
  ETH_INSTRUMENT,
  expectNoBannedUiSymbols,
  expectTooltipUsable,
  helpTrigger,
  openHelpByHover,
  openHelpByKeyboard,
  toggleHelpByTouch,
} from "./support";

/**
 * Real Bars acceptance against the local API and test Dataset.
 *
 * - The chart's HTML data table exposes the exact server decimal strings.
 * - The chart host supports keyboard point readout.
 * - The revision-safe v2 cursor pages the 5001-revision corpus exactly once,
 *   passes the opaque cursor back unchanged and never falls back to the old
 *   Time Cursor defect path.
 * - Migrated Bars areas use D cards and `?` help without banned separators.
 */

const HELP_NAMES = [
  "查询条件说明",
  "最大 Revision Bar 图说明",
  "原始 Revision 表说明",
  "小文件 CSV 直接下载说明",
  "Export Job 说明",
  "Export Job 状态说明",
  "实时跟随说明",
] as const;

const MIGRATED_REGION_SELECTORS = [
  '[role="region"][aria-label="Bar 查询条件"]',
  '[data-slot="chart-panel"]',
  '[role="region"][aria-label="原始 Revision 表"]',
  '[role="region"][aria-label="CSV 直接下载"]',
  '[role="region"][aria-label="Export Job"]',
  '[role="region"][aria-label="实时跟随"]',
] as const;

interface BarsPageBody {
  bars: Array<{
    instrument_id: string;
    ingest_ts: string;
    payload: {
      interval: string;
      open_ts: string;
      revision: number;
    };
  }>;
  page: { has_more: boolean; next_cursor: string | null };
}

function collectV2Pages(page: Page) {
  const requestUrls = new Set<string>();
  const pagesByUrl = new Map<string, BarsPageBody>();
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname === "/api/v2/bars") {
      requestUrls.add(request.url());
    }
  });
  page.on("response", (response) => {
    const url = new URL(response.url());
    if (url.pathname !== "/api/v2/bars") {
      return;
    }
    void response
      .json()
      .then((body) => pagesByUrl.set(response.url(), body as BarsPageBody))
      .catch(() => {});
  });
  return { requestUrls, pagesByUrl };
}

function barIdentity(bar: BarsPageBody["bars"][number]): string {
  return `${bar.payload.open_ts}\u0000${bar.payload.revision}\u0000${bar.ingest_ts}`;
}

test("chart data table renders exact server strings and keyboard readout works", async ({
  page,
}) => {
  const params = barsQuery({
    instrument: BTC_INSTRUMENT,
    start: "2024-01-01T00:00:00.000Z",
    end: "2024-01-01T00:02:00.000Z",
  });
  await page.goto(`/bars?${params.toString()}`);

  await expect(page.getByTestId("history-summary")).toContainText(
    "共 2 条原始 Revision",
  );
  await expect(
    page.getByRole("region", { name: "最大 Revision Bar 图" }),
  ).toBeVisible();

  const summary = page.getByText("查看数据表（2 个时间点）");
  await summary.click();
  const chartPanel = page.locator('[data-slot="chart-panel"]', {
    has: page.getByRole("heading", { name: "最大 Revision Bar 图" }),
  });
  const table = chartPanel.getByRole("table");
  await expect(table).toBeVisible();
  await expect(table).toContainText("2024-01-01T00:00:00.000000000Z");
  await expect(table).toContainText("2024-01-01T00:01:00.000000000Z");
  await expect(table).toContainText("65000.1");
  await expect(table).toContainText("65010.0");
  await expect(table).toContainText("64990.0");
  await expect(table).toContainText("65005.5");
  await expect(table).toContainText("65015.0");
  await expect(table).toContainText("12.34");

  const chart = page.getByRole("img", {
    name: /最大 Revision Bar 价格图/,
  });
  await chart.focus();
  const readout = page.locator("#bar-chart-readout");
  await expect(readout).toContainText("close 65015.0");
  await page.keyboard.press("ArrowLeft");
  await expect(readout).toContainText("close 65005.5");
  await page.keyboard.press("End");
  await expect(readout).toContainText("close 65015.0");

  // D help triggers exist on every migrated region.
  for (const name of HELP_NAMES) {
    await expect(helpTrigger(page, name)).toBeVisible();
  }

  // Raw Revision JSON is raw evidence: expand it and verify it is marked and
  // still readable, while UI-generated copy in the migrated regions (including
  // the Export Job area) stays free of `·` and `：`.
  await page
    .locator('[role="region"][aria-label="原始 Revision 表"] summary', {
      hasText: "原始 JSON",
    })
    .first()
    .click();
  await expect(
    page
      .locator('[role="region"][aria-label="原始 Revision 表"]')
      .locator('[data-raw-evidence="true"]')
      .first(),
  ).toBeVisible();

  for (const selector of MIGRATED_REGION_SELECTORS) {
    await expect(page.locator(selector)).toBeVisible();
    await expectNoBannedUiSymbols(page, selector);
  }
});

test("opaque revision-safe cursor pages the 5001-revision corpus once", async ({
  page,
}) => {
  const { requestUrls, pagesByUrl } = collectV2Pages(page);

  const params = barsQuery({
    instrument: ETH_INSTRUMENT,
    start: "2024-01-02T00:00:00.000Z",
    end: "2024-01-03T00:00:00.000Z",
  });
  await page.goto(`/bars?${params.toString()}`);

  const summary = page.getByTestId("history-summary");
  await expect(summary).toContainText("共 5001 条原始 Revision", {
    timeout: 60_000,
  });
  await expect(summary).toContainText("1 个最大 Revision 点");
  await expect(summary).toContainText("2 页");
  await expect(summary).toContainText("分页完整");
  await expect(page.getByText(/分页已停止/)).toHaveCount(0);

  // Exactly two opaque-cursor pages; no Time Cursor fallback or loop.
  await expect.poll(() => pagesByUrl.size).toBe(2);
  await expect.poll(() => requestUrls.size).toBe(2);

  const pages = [...pagesByUrl.values()];
  const firstPage = pages.find((item) => item.page.has_more);
  const lastPage = pages.find((item) => !item.page.has_more);
  expect(firstPage).toBeTruthy();
  expect(lastPage).toBeTruthy();
  expect(firstPage!.page.next_cursor).toBeTruthy();

  const bars = pages.flatMap((item) => item.bars);
  expect(bars).toHaveLength(5001);
  expect(new Set(bars.map(barIdentity)).size).toBe(5001);

  const cursorRequests = [...requestUrls].filter(
    (url) => new URL(url).searchParams.get("cursor") !== null,
  );
  expect(cursorRequests).toHaveLength(1);
  expect(new URL(cursorRequests[0]!).searchParams.get("cursor")).toBe(
    firstPage!.page.next_cursor,
  );
  const firstRequest = [...requestUrls].find(
    (url) => new URL(url).searchParams.get("cursor") === null,
  );
  expect(firstRequest).toBeTruthy();
  expect(new URL(firstRequest!).searchParams.get("limit")).toBe("5000");
});

test("migrated Bars help opens by hover and keyboard and closes with Escape", async ({
  page,
}) => {
  const params = barsQuery({
    instrument: BTC_INSTRUMENT,
    start: "2024-01-01T00:00:00.000Z",
    end: "2024-01-01T00:02:00.000Z",
  });
  await page.goto(`/bars?${params.toString()}`);
  await expect(page.getByTestId("history-summary")).toContainText(
    "共 2 条原始 Revision",
  );

  for (const name of HELP_NAMES) {
    const hovered = await openHelpByHover(page, name);
    await expectTooltipUsable(page);
    expect(
      ((await hovered.tooltip.textContent()) ?? "").trim().length,
    ).toBeGreaterThan(0);
    await expectNoBannedUiSymbols(page, '[data-help-content="true"]');
    await closeHelpWithEscape(page);

    const focused = await openHelpByKeyboard(page, name);
    await expectTooltipUsable(page);
    expect(
      ((await focused.tooltip.textContent()) ?? "").trim().length,
    ).toBeGreaterThan(0);
    await closeHelpWithEscape(page);
  }
});

test("migrated Bars help toggles on a real touch context", async ({
  browser,
}) => {
  const baseURL = test.info().project.use.baseURL as string;
  const context = await browser.newContext({
    baseURL,
    hasTouch: true,
    viewport: { width: 1280, height: 900 },
  });
  const touchPage = await context.newPage();
  try {
    const params = barsQuery({
      instrument: BTC_INSTRUMENT,
      start: "2024-01-01T00:00:00.000Z",
      end: "2024-01-01T00:02:00.000Z",
    });
    await touchPage.goto(`/bars?${params.toString()}`);
    await expect(touchPage.getByTestId("history-summary")).toContainText(
      "共 2 条原始 Revision",
    );

    for (const name of HELP_NAMES) {
      await toggleHelpByTouch(touchPage, name);
    }
  } finally {
    await context.close();
  }
});
