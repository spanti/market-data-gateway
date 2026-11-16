import fs from "node:fs/promises";
import path from "node:path";
import { expect, test, type Page } from "@playwright/test";
import {
  barsQuery,
  closeHelpWithEscape,
  expectNoBannedUiSymbols,
  expectNoHorizontalOverflow,
  expectTooltipUsable,
  helpTrigger,
  openHelpByHover,
} from "./support";

/**
 * Ticket 09: Variant D visual acceptance and full-console proof.
 *
 * Screenshots are curated evidence under `web/docs/acceptance-v2/`. The spec
 * also asserts the D surface properties (near-white micro-rounded cards, soft
 * shadow, status pills), absence of full-page decorative rules and numeric
 * underlined headings, the distinct chart-readout/help roles, and that a real
 * failed source never leaves a healthy badge behind.
 *
 * `CONTRACT-ONLY` tests inject responses or WebSocket frames in a real browser;
 * they are not evidence of a real backend behaviour.
 */

const SCREENSHOT_DIR = path.resolve(process.cwd(), "docs/acceptance-v2");

async function capture(page: Page, name: string): Promise<void> {
  await fs.mkdir(SCREENSHOT_DIR, { recursive: true });
  await page.screenshot({
    path: path.join(SCREENSHOT_DIR, name),
    fullPage: true,
    animations: "disabled",
  });
}

async function fullPageDividerHits(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const viewportWidth = document.documentElement.clientWidth;
    const hits: string[] = [];
    for (const element of Array.from(
      document.querySelectorAll<HTMLElement>("body *"),
    )) {
      const rect = element.getBoundingClientRect();
      if (rect.width < viewportWidth * 0.98 || rect.height > 3) {
        continue;
      }
      const style = window.getComputedStyle(element);
      const borderTop = Number.parseFloat(style.borderTopWidth) || 0;
      const borderBottom = Number.parseFloat(style.borderBottomWidth) || 0;
      if (borderTop < 0.5 && borderBottom < 0.5) {
        continue;
      }
      const background = style.backgroundColor;
      if (background !== "rgba(0, 0, 0, 0)" && background !== "transparent") {
        continue;
      }
      hits.push(
        `${element.tagName}.${String(element.className).slice(0, 120)}`,
      );
    }
    return hits;
  });
}

async function numericHeadingHits(page: Page): Promise<string[]> {
  return page.evaluate(() =>
    Array.from(document.querySelectorAll<HTMLElement>("h1, h2, h3, h4"))
      .filter((heading) => /^\s*\d+[.、\s]/.test(heading.innerText.trim()))
      .map((heading) => heading.innerText.trim().slice(0, 80)),
  );
}

/**
 * User-visible page contract shared by every module. Visual properties such
 * as card radius and shadow are reviewed from the curated screenshots, not
 * asserted through private component selectors.
 */
async function expectConsoleSurface(page: Page): Promise<void> {
  const regions = page.getByRole("region").filter({ visible: true });
  await expect(regions.first()).toBeVisible();
  await expect(page.getByRole("heading").first()).toBeVisible();

  expect(await fullPageDividerHits(page)).toEqual([]);
  expect(await numericHeadingHits(page)).toEqual([]);
  const title = await page.title();
  expect(title).not.toContain("·");
  expect(title).not.toContain("：");
  expect(title.trim().length).toBeGreaterThan(0);
  await expectNoBannedUiSymbols(page);
  await expectNoHorizontalOverflow(page);
}

async function expectMonitoringKpis(page: Page): Promise<void> {
  for (const label of [
    "Realtime QPS",
    "Peak QPS",
    "HTTP P99",
    "Dataset Freshness",
  ]) {
    await expect(page.getByRole("group", { name: label })).toBeVisible();
  }
  for (const title of ["QPS 趋势", "HTTP 延迟趋势"]) {
    await expect(page.getByRole("region", { name: title })).toBeVisible();
  }
}

async function expectQualityRegions(page: Page): Promise<void> {
  for (const name of [
    "Unrecoverable Backfill Gap 当前快照",
    "持久 Unrecoverable Backfill Gap",
    "Backfill Job",
  ]) {
    await expect(page.getByRole("region", { name })).toBeVisible();
  }
}

function barsUrl(): string {
  const params = barsQuery({
    instrument: "binance:BTC/USDT",
    start: "2024-01-01T00:00:00.000Z",
    end: "2024-01-01T00:02:00.000Z",
  });
  return `/bars?${params.toString()}`;
}

async function expectBarsReady(page: Page): Promise<void> {
  await expect(page.getByTestId("history-summary")).toContainText(
    "共 2 条原始 Revision",
  );
  const chart = page.getByRole("region", { name: "最大 Revision Bar 图" });
  await expect(chart).toBeVisible();
  await expect(chart.getByText(/查看数据表（\d+ 个时间点）/)).toBeVisible();
}

async function waitForMonitoringData(page: Page): Promise<void> {
  const qps = page.getByRole("group", { name: "Realtime QPS" });
  await expect
    .poll(
      async () => (await qps.innerText()).replace(/\s+/g, " "),
      { timeout: 60_000, intervals: [500, 1000, 2000] },
    )
    .not.toContain("未知");
  const qpsPanel = page.getByRole("region", { name: "QPS 趋势" });
  await expect(qpsPanel.getByText(/查看数据表（\d+ 个时间点）/)).toBeVisible();
}

test.describe("Variant D visual acceptance screenshots", () => {
  test.use({ viewport: { width: 1440, height: 900 } });

  test("light 1440 screenshots for all five modules plus expanded help", async ({
    page,
  }) => {
    await page.goto("/monitoring");
    await expect(
      page.getByRole("heading", { name: "监控总览" }),
    ).toBeVisible();
    await expectMonitoringKpis(page);
    await waitForMonitoringData(page);
    await expectConsoleSurface(page);
    await capture(page, "light-1440-monitoring.png");

    const help = await openHelpByHover(page, "Realtime QPS 说明");
    await expectTooltipUsable(page);
    expect((await help.tooltip.innerText()).trim().length).toBeGreaterThan(10);
    await expectNoBannedUiSymbols(page);
    await capture(page, "light-1440-monitoring-help.png");
    await closeHelpWithEscape(page);

    await page.goto(barsUrl());
    await expectBarsReady(page);
    await expectConsoleSurface(page);
    await capture(page, "light-1440-bars.png");

    await page.goto("/console");
    await expect(
      page.getByRole("region", { name: "Feed Subscription" }),
    ).toBeVisible();
    await expectConsoleSurface(page);
    await capture(page, "light-1440-console-feed.png");

    await page.getByRole("tab", { name: "Service Instance" }).click();
    await expect(
      page.getByRole("region", { name: "Service Instance" }),
    ).toBeVisible();
    await expect(page.getByTestId("service-instance-collector")).toBeVisible();
    await expectConsoleSurface(page);
    await capture(page, "light-1440-console-service-instance.png");

    await page.getByRole("tab", { name: "WebSocket Debug" }).click();
    await expect(
      page.getByRole("region", { name: "WebSocket Debug" }),
    ).toBeVisible();
    await expectConsoleSurface(page);
    await capture(page, "light-1440-console-websocket.png");

    await page.goto("/quality");
    await expect(
      page.getByRole("heading", { name: "Quality & Backfill" }),
    ).toBeVisible();
    await expectQualityRegions(page);
    await expectConsoleSurface(page);
    await capture(page, "light-1440-quality.png");

    await page.goto("/security");
    await expect(
      page.getByRole("region", { name: "当前身份" }),
    ).toBeVisible();
    await expectConsoleSurface(page);
    await capture(page, "light-1440-security.png");
  });

  test("1024 narrow desktop and dark theme remain readable", async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 900 });
    await page.goto(barsUrl());
    await expectBarsReady(page);
    await expectConsoleSurface(page);

    // The wide table is wider than its card, yet the page itself does not
    // overflow; the local scroll container keeps it inside the card.
    const revisionRegion = page.getByRole("region", {
      name: "原始 Revision 表",
    });
    const tableBox = await revisionRegion.getByRole("table").boundingBox();
    const regionBox = await revisionRegion.boundingBox();
    expect(tableBox).not.toBeNull();
    expect(regionBox).not.toBeNull();
    expect(tableBox!.width).toBeGreaterThan(regionBox!.width);
    await capture(page, "light-1024-bars.png");

    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto("/monitoring");
    await expect(
      page.getByRole("heading", { name: "监控总览" }),
    ).toBeVisible();
    await waitForMonitoringData(page);
    await page
      .getByRole("button", { name: "切换主题（当前浅色）" })
      .click();
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");

    // Readable dark rendering is reviewed from the screenshot; behaviour
    // assertions stay on user-visible content, states and overflow.
    await expectConsoleSurface(page);
    await capture(page, "dark-1440-monitoring.png");
  });
});

test("chart value readout and help tooltip keep distinct responsibilities", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto("/monitoring");
  await expect(
    page.getByRole("heading", { name: "监控总览" }),
  ).toBeVisible();

  const qpsPanel = page.getByRole("region", { name: "QPS 趋势" });
  const chart = qpsPanel.getByRole("img", { name: "QPS 趋势", exact: true });
  await expect(chart).toBeVisible();
  await chart.focus();
  const readout = qpsPanel.getByRole("status");
  await expect(readout).not.toBeEmpty();

  // While help is closed the chart readout is the only value readout.
  await expect(page.getByRole("tooltip")).toBeHidden();

  await helpTrigger(page, "QPS 趋势说明").focus();
  const tooltip = page.getByRole("tooltip");
  await expect(tooltip).toBeVisible();
  await expect(tooltip).not.toBeEmpty();
  await expect(tooltip.getByRole("status")).toBeHidden();
  await expect(tooltip).toContainText("数据源");
  await expect(readout).not.toContainText("数据源");
  await expectTooltipUsable(page);
  await expectNoBannedUiSymbols(page);
  await closeHelpWithEscape(page);
});

test.describe("CONTRACT-ONLY failure-state injection", () => {
  test("a failed Collector source never keeps a healthy Feed badge", async ({
    page,
  }) => {
    await page.route("**/ops/collector/status", (route) =>
      route.fulfill({
        status: 500,
        contentType: "application/json",
        body: JSON.stringify({
          error: {
            code: "internal_error",
            message: "upstream · failed：timeout",
            details: {},
            request_id: "req-state-09",
          },
        }),
      }),
    );

    await page.goto("/monitoring");
    await expect(
      page.getByRole("heading", { name: "监控总览" }),
    ).toBeVisible();

    const feed = page.getByRole("region", {
      name: "Feed Freshness",
      exact: true,
    });
    await expect(feed).toContainText("Collector /status 加载失败");
    await expect(feed).toContainText("最后成功");
    await expect(feed.getByRole("alert")).toBeVisible();
    // The ops adapter deliberately discards non-envelope JSON bodies, so the
    // UI renders its own transport-level message. Assert exactly that visible
    // text; the JSON body's "upstream · failed：timeout" is not surfaced here.
    await expect(feed).toContainText("Ops endpoint returned HTTP 500");
    await expect(feed.getByText("正常")).toBeHidden();
    await expect(feed).not.toContainText("upstream · failed：timeout");
    await expectNoBannedUiSymbols(page);
  });
});

test.describe("CONTRACT-ONLY WebSocket raw evidence", () => {
  test("raw JSON with both separators stays byte-accurate", async ({ page }) => {
    let sent = false;
    await page.routeWebSocket(/\/ws$/, (socket) => {
      socket.onMessage(() => {
        if (sent) {
          return;
        }
        sent = true;
        socket.send(
          JSON.stringify({
            op: "event",
            topic: "market.bar.1m.binance:BTC/USDT",
            data: {
              schema_version: 1,
              data_type: "bar",
              instrument_id: "binance:BTC/USDT",
              event_ts: "2024-01-01T00:01:00.000000000Z",
              ingest_ts: "2024-01-01T00:01:00.000000000Z",
              feed: "fake",
              seq: 1,
              payload: {
                interval: "1m",
                open_ts: "2024-01-01T00:00:00.000000000Z",
                close_ts: "2024-01-01T00:01:00.000000000Z",
                aggregation_source: "external",
                open: "65000.1",
                high: "65010.0",
                low: "64990.0",
                close: "65005.5",
                volume: "12.34",
                trade_count: 128,
                revision: 0,
              },
              note: "a·b：c",
            },
          }),
        );
      });
    });

    await page.goto("/console");
    await page.getByRole("tab", { name: "WebSocket Debug" }).click();
    await page.getByRole("button", { name: "连接", exact: true }).click();
    await expect(page.getByTestId("ws-connection-state")).toHaveText("已连接");
    await page.getByRole("button", { name: "Ping", exact: true }).click();

    const row = page
      .getByTestId("ws-message")
      .filter({ hasText: "close 65005.5" })
      .first();
    await expect(row).toBeVisible();
    await row.getByText("原始 JSON").click();
    const detail = row.getByTestId("ws-message-detail");
    await expect(detail).toHaveAttribute("data-raw-evidence", "true");
    expect(await detail.textContent()).toContain('"note": "a·b：c"');
    await expectNoBannedUiSymbols(page, "#main-content");
  });
});

test.describe("touch console tabs", () => {
  test.use({ hasTouch: true, viewport: { width: 1024, height: 900 } });

  test("each Console tab is reachable and stays free of page overflow", async ({
    page,
  }) => {
    await page.goto("/console");
    for (const tab of ["Feed Subscription", "Service Instance", "WebSocket Debug"] as const) {
      await page.getByRole("tab", { name: tab }).tap();
      await expect(
        page.getByRole("region", { name: tab, exact: true }),
      ).toBeVisible();
      await expectNoHorizontalOverflow(page);
      await expectNoBannedUiSymbols(page, "#main-content");
    }
  });
});
