import fs from "node:fs/promises";
import path from "node:path";
import { expect, test, type Locator, type Page } from "@playwright/test";
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

/**
 * D card surfaces. `Card` receives a component-specific `data-slot` from its
 * caller (for example `metric-card` or `chart-panel`), so the generic
 * `data-slot="card"` selector alone is not enough on the monitoring pages.
 */
const CARD_SURFACE_SELECTOR = [
  '[data-slot="card"]',
  '[data-slot="metric-card"]',
  '[data-slot="chart-panel"]',
].join(", ");

async function capture(page: Page, name: string): Promise<void> {
  await fs.mkdir(SCREENSHOT_DIR, { recursive: true });
  await page.screenshot({
    path: path.join(SCREENSHOT_DIR, name),
    fullPage: true,
    animations: "disabled",
  });
}

async function resolvedRgb(
  locator: Locator,
  property: "backgroundColor" | "color" = "backgroundColor",
): Promise<[number, number, number, number]> {
  return locator.evaluate((element, cssProperty) => {
    const style = window.getComputedStyle(element);
    const canvas = document.createElement("canvas");
    canvas.width = 1;
    canvas.height = 1;
    const context = canvas.getContext("2d");
    if (!context) {
      return [0, 0, 0, 0] as [number, number, number, number];
    }
    context.fillStyle = style[cssProperty];
    context.fillRect(0, 0, 1, 1);
    const data = context.getImageData(0, 0, 1, 1).data;
    return [data[0]!, data[1]!, data[2]!, data[3]!] as [
      number,
      number,
      number,
      number,
    ];
  }, property);
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

async function expectVariantDSurface(
  page: Page,
  options: { dark?: boolean } = {},
): Promise<void> {
  const cards = page
    .locator(CARD_SURFACE_SELECTOR)
    .filter({ visible: true });
  await expect(cards.first()).toBeVisible();
  expect(await cards.count()).toBeGreaterThan(0);

  const card = cards.first();
  const cardStyle = await card.evaluate((element) => {
    const style = window.getComputedStyle(element);
    return {
      radius: style.borderRadius,
      shadow: style.boxShadow,
      borderWidth: style.borderTopWidth,
    };
  });
  expect(Number.parseFloat(cardStyle.radius)).toBeGreaterThanOrEqual(8);
  expect(cardStyle.shadow).not.toBe("none");
  expect(Number.parseFloat(cardStyle.borderWidth)).toBeGreaterThan(0);

  const [red, green, blue, alpha] = await resolvedRgb(card);
  expect(alpha).toBeGreaterThan(0);
  if (options.dark) {
    expect(Math.max(red, green, blue)).toBeLessThan(100);
  } else {
    expect(Math.min(red, green, blue)).toBeGreaterThanOrEqual(245);
  }

  const badges = page
    .locator('[data-slot="status-badge"]')
    .filter({ visible: true });
  const badgeCount = await badges.count();
  if (badgeCount > 0) {
    const badge = badges.first();
    const badgeStyle = await badge.evaluate((element) => {
      const style = window.getComputedStyle(element);
      return {
        radius: style.borderRadius,
        background: style.backgroundColor,
      };
    });
    expect(Number.parseFloat(badgeStyle.radius)).toBeGreaterThanOrEqual(8);
    expect(badgeStyle.background).not.toBe("rgba(0, 0, 0, 0)");
    await expect(badge.locator("svg").first()).toBeVisible();
    expect((await badge.innerText()).trim().length).toBeGreaterThan(0);
  }

  expect(await fullPageDividerHits(page)).toEqual([]);
  expect(await numericHeadingHits(page)).toEqual([]);
  const title = await page.title();
  expect(title).not.toContain("·");
  expect(title).not.toContain("：");
  expect(title.trim().length).toBeGreaterThan(0);
  await expectNoBannedUiSymbols(page);
  await expectNoHorizontalOverflow(page);
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
  await expect(
    page.getByRole("region", { name: "最大 Revision Bar 图" }),
  ).toBeVisible();
  await expect(
    page.locator('[data-slot="chart-panel"][data-state="ready"]'),
  ).toBeVisible();
}

async function waitForMonitoringData(page: Page): Promise<void> {
  const qps = page.locator(
    '[data-slot="metric-card"][data-metric-label="Realtime QPS"]',
  );
  await expect
    .poll(
      async () => (await qps.innerText()).replace(/\s+/g, " "),
      { timeout: 60_000, intervals: [500, 1000, 2000] },
    )
    .not.toContain("未知");
  const qpsPanel = page.locator('[data-slot="chart-panel"]', {
    has: page.getByRole("heading", { name: "QPS 趋势" }),
  });
  await expect(qpsPanel).toHaveAttribute("data-state", "ready");
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
    await expect(page.locator('[data-slot="metric-card"]')).toHaveCount(4);
    await expect(page.locator('[data-slot="chart-panel"]')).toHaveCount(2);
    await waitForMonitoringData(page);
    await expectVariantDSurface(page);
    await capture(page, "light-1440-monitoring.png");

    const help = await openHelpByHover(page, "Realtime QPS 说明");
    await expectTooltipUsable(page);
    expect((await help.tooltip.innerText()).trim().length).toBeGreaterThan(10);
    await expectNoBannedUiSymbols(page);
    await capture(page, "light-1440-monitoring-help.png");
    await closeHelpWithEscape(page);

    await page.goto(barsUrl());
    await expectBarsReady(page);
    await expectVariantDSurface(page);
    await capture(page, "light-1440-bars.png");

    await page.goto("/console");
    await expect(
      page.getByRole("region", { name: "Feed Subscription" }),
    ).toBeVisible();
    await expectVariantDSurface(page);
    await capture(page, "light-1440-console-feed.png");

    await page.getByRole("tab", { name: "Service Instance" }).click();
    await expect(
      page.getByRole("region", { name: "Service Instance" }),
    ).toBeVisible();
    await expect(page.getByTestId("service-instance-collector")).toBeVisible();
    await expectVariantDSurface(page);
    await capture(page, "light-1440-console-service-instance.png");

    await page.getByRole("tab", { name: "WebSocket Debug" }).click();
    await expect(
      page.getByRole("region", { name: "WebSocket Debug" }),
    ).toBeVisible();
    await expectVariantDSurface(page);
    await capture(page, "light-1440-console-websocket.png");

    await page.goto("/quality");
    await expect(
      page.getByRole("heading", { name: "Quality & Backfill" }),
    ).toBeVisible();
    await expect(page.locator('[data-slot="card"]')).toHaveCount(3);
    await expectVariantDSurface(page);
    await capture(page, "light-1440-quality.png");

    await page.goto("/security");
    await expect(
      page.getByRole("region", { name: "当前身份" }),
    ).toBeVisible();
    await expectVariantDSurface(page);
    await capture(page, "light-1440-security.png");
  });

  test("1024 narrow desktop and dark theme remain readable", async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 900 });
    await page.goto(barsUrl());
    await expectBarsReady(page);
    await expectVariantDSurface(page);

    const revisionScroller = page
      .getByRole("region", { name: "原始 Revision 表" })
      .locator('[data-slot="table-container"]');
    const widths = await revisionScroller.evaluate((element) => ({
      scrollWidth: element.scrollWidth,
      clientWidth: element.clientWidth,
    }));
    expect(widths.scrollWidth).toBeGreaterThan(widths.clientWidth);
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

    const [bodyRed, bodyGreen, bodyBlue] = await resolvedRgb(
      page.locator("body"),
    );
    const [textRed, textGreen, textBlue] = await resolvedRgb(
      page.locator("body"),
      "color",
    );
    expect(Math.max(bodyRed, bodyGreen, bodyBlue)).toBeLessThan(64);
    expect(Math.min(textRed, textGreen, textBlue)).toBeGreaterThan(128);
    await expectVariantDSurface(page, { dark: true });
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

  const qpsPanel = page.locator('[data-slot="chart-panel"]', {
    has: page.getByRole("heading", { name: "QPS 趋势" }),
  });
  await expect(qpsPanel).toHaveAttribute("data-state", "ready");
  const chart = qpsPanel.getByRole("img", { name: "QPS 趋势", exact: true });
  await expect(chart).toBeVisible();
  await chart.focus();
  const readout = qpsPanel.getByRole("status");
  await expect(readout).not.toBeEmpty();

  // While help is closed the chart readout is the only value readout.
  await expect(page.getByRole("tooltip")).toHaveCount(0);

  await helpTrigger(page, "QPS 趋势说明").focus();
  const tooltip = page.getByRole("tooltip");
  await expect(tooltip).toBeVisible();
  await expect(tooltip.locator('[data-help-content="true"]')).toBeVisible();
  await expect(tooltip.locator('[role="status"]')).toHaveCount(0);
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
    await expect(
      feed.locator('[data-slot="status-badge"][data-tone="good"]'),
    ).toHaveCount(0);
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
