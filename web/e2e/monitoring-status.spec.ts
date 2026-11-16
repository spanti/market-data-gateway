import { expect, test } from "@playwright/test";
import {
  closeHelpWithEscape,
  expectNoBannedUiSymbols,
  expectTooltipUsable,
  helpTrigger,
  openHelpByHover,
  openHelpByKeyboard,
  toggleHelpByTouch,
} from "./support";

/**
 * Real monitoring acceptance against the local collector/API/replay stack and
 * a real Prometheus container. No status or Prometheus response is mocked.
 */

const ALLOWED_TONES = new Set([
  "good",
  "warning",
  "serious",
  "critical",
  "unknown",
  "info",
]);

const KPI_LABELS = [
  "Realtime QPS",
  "Peak QPS",
  "HTTP P99",
  "Dataset Freshness",
] as const;

test("status is encoded with icon + text, never color alone", async ({
  page,
}) => {
  await page.goto("/monitoring");
  await expect(
    page.getByRole("heading", { name: "监控总览" }),
  ).toBeVisible();

  const badges = page.locator('[data-slot="status-badge"]');
  await expect(badges.first()).toBeVisible();
  const count = await badges.count();
  expect(count).toBeGreaterThan(3);

  for (let index = 0; index < count; index += 1) {
    const badge = badges.nth(index);
    if (!(await badge.isVisible())) {
      continue;
    }
    const text = (await badge.innerText()).trim();
    expect(text.length, `status badge #${index} has no text`).toBeGreaterThan(0);
    await expect(badge.locator("svg").first()).toBeVisible();
    const tone = await badge.getAttribute("data-tone");
    expect(tone).not.toBeNull();
    expect(ALLOWED_TONES.has(tone as string)).toBe(true);
  }

  // The real collector status is surfaced as Ready / not-ready text.
  const collector = page.locator(
    '[data-slot="service-instance"][data-role="collector"]',
  );
  await expect(collector).toBeVisible();
  await expect(
    collector.locator('[data-slot="status-badge"]').first(),
  ).toContainText(/Ready|未就绪|未知/);
});

test("D foundation exposes four KPI cards and separated freshness areas", async ({
  page,
}) => {
  await page.goto("/monitoring");
  await expect(
    page.getByRole("heading", { name: "监控总览" }),
  ).toBeVisible();

  for (const label of KPI_LABELS) {
    await expect(
      page.locator(`[data-slot="metric-card"][data-metric-label="${label}"]`),
    ).toBeVisible();
  }
  await expect(page.locator('[data-slot="metric-card"]')).toHaveCount(4);

  // Feed Freshness and Dataset Freshness remain separate signals.
  const feed = page.getByRole("region", {
    name: "Feed Freshness",
    exact: true,
  });
  await expect(feed).toBeVisible();
  await expect(feed).toContainText("binance:BTC/USDT");
  await expect(feed).toContainText("正常");
  const dataset = page.getByRole("region", {
    name: "Dataset Freshness",
    exact: true,
  });
  await expect(dataset).toBeVisible();
  await expect(dataset).toContainText("独立于 Feed Freshness");

  // Service Role and Service Instance are labelled as distinct concepts.
  const service = page.getByRole("region", {
    name: "Service Instance",
    exact: true,
  });
  await expect(service).toBeVisible();
  await expect(service).toContainText("Service Role");
  await expect(service).toContainText("Instance");
  for (const role of ["collector", "api", "replay"] as const) {
    await expect(
      page.locator(`[data-slot="service-instance"][data-role="${role}"]`),
    ).toBeVisible();
  }

  // Upstream connection capabilities stay explicitly unknown.
  const upstream = page.getByRole("region", {
    name: "上游能力与 Freshness",
  });
  await expect(upstream).toBeVisible();
  await expect(upstream).toContainText("连接 未暴露");
  await expect(upstream).toContainText("心跳 未暴露");
  await expect(upstream).toContainText("重连 未暴露");
  await expect(upstream).toContainText("网络 未暴露");
  await expect(upstream).toContainText("不证明连接健康");

  // In-page alerts are a real region, not a fabricated mock timeline.
  await expect(page.getByRole("region", { name: "页面内告警" })).toBeVisible();

  await expectNoBannedUiSymbols(page);
});

test("real Prometheus trends expose a keyboard readout and data table", async ({
  page,
}) => {
  await page.goto("/monitoring");
  await expect(
    page.getByRole("heading", { name: "监控总览" }),
  ).toBeVisible();

  const qpsCell = page.locator(
    '[data-slot="metric-card"][data-metric-label="Realtime QPS"]',
  );
  await expect(qpsCell).toBeVisible();

  // Wait until the real Prometheus query has a sample; never accept unknown.
  await expect
    .poll(async () => (await qpsCell.innerText()).replace(/\s+/g, " "), {
      timeout: 60_000,
      intervals: [500, 1000, 2000],
    })
    .not.toContain("未知");

  await expect(page.getByText(/Prometheus（\/prometheus）/).first()).toBeVisible();

  const qpsPanel = page.locator('[data-slot="chart-panel"]', {
    has: page.getByRole("heading", { name: "QPS 趋势" }),
  });
  await expect(qpsPanel).toHaveAttribute("data-state", "ready");

  // A freshly started Prometheus initially has a single sample. Keyboard
  // navigation is only observable once the window has at least two points.
  const summary = qpsPanel.getByText(/查看数据表（\d+ 个时间点）/);
  await expect(summary).toBeVisible();
  await expect
    .poll(
      async () => {
        const text = await summary.innerText();
        const match = text.match(/（(\d+) 个时间点）/);
        return match ? Number(match[1]) : 0;
      },
      { timeout: 60_000, intervals: [1000, 2000] },
    )
    .toBeGreaterThanOrEqual(2);

  const chart = qpsPanel.getByRole("img", { name: "QPS 趋势", exact: true });
  await chart.focus();
  const readout = qpsPanel.getByRole("status");
  await expect(readout).not.toBeEmpty();
  const before = await readout.innerText();
  await page.keyboard.press("ArrowLeft");
  await expect
    .poll(async () => readout.innerText())
    .not.toBe(before);

  await summary.click();

  const table = qpsPanel.getByRole("table");
  await expect(table).toBeVisible();
  await expect(table.getByRole("columnheader", { name: "时间（UTC）" })).toBeVisible();
  expect(await table.getByRole("row").count()).toBeGreaterThan(1);
});

test("help opens on hover, is readable, stays off its trigger and closes with Escape", async ({
  page,
}) => {
  await page.goto("/monitoring");
  await expect(
    page.getByRole("heading", { name: "监控总览" }),
  ).toBeVisible();

  const trigger = helpTrigger(page, "Realtime QPS 说明");
  await expect(trigger).toBeVisible();
  const { tooltip } = await openHelpByHover(page, "Realtime QPS 说明");
  expect((await tooltip.innerText()).trim().length).toBeGreaterThan(10);
  await expectTooltipUsable(page);
  await expectNoBannedUiSymbols(page);
  await closeHelpWithEscape(page);
});

test("keyboard focus opens help without a pointer", async ({ page }) => {
  await page.goto("/monitoring");
  await expect(
    page.getByRole("heading", { name: "监控总览" }),
  ).toBeVisible();

  await openHelpByKeyboard(page, "Peak QPS 说明");
  await expectTooltipUsable(page);
  await closeHelpWithEscape(page);
});

test.describe("touch help", () => {
  test.use({ hasTouch: true, viewport: { width: 1024, height: 900 } });

  test("a tap toggles help and a second tap closes it", async ({ page }) => {
    await page.goto("/monitoring");
    await expect(
      page.getByRole("heading", { name: "监控总览" }),
    ).toBeVisible();

    await toggleHelpByTouch(page, "Dataset Freshness 指标说明");
    await expectNoBannedUiSymbols(page);
  });
});
