import { expect, test } from "@playwright/test";

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

test("status is encoded with icon + text, never color alone", async ({
  page,
}) => {
  await page.goto("/monitoring");
  await expect(
    page.getByRole("heading", { name: "监控分析工作台" }),
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

test("real Feed Freshness and Service Instance data are shown", async ({
  page,
}) => {
  await page.goto("/monitoring");
  await expect(
    page.getByRole("heading", { name: "监控分析工作台" }),
  ).toBeVisible();

  // Service ledger: three real Service Roles from the live role stack.
  for (const role of ["collector", "api", "replay"] as const) {
    await expect(
      page.locator(`[data-slot="service-instance"][data-role="${role}"]`),
    ).toBeVisible();
  }

  // Feed Freshness comes from the real collector /status.health.tracked.
  const matrix = page.locator('[data-slot="freshness-matrix"]');
  await expect(matrix).toBeVisible();
  await expect(matrix).toContainText("binance:BTC/USDT");
  await expect(matrix).toContainText("binance:ETH/USDT");
  await expect(matrix).toContainText("正常");

  // Dataset Freshness is a separate real source.
  await expect(page.getByText("Dataset Freshness", { exact: true })).toBeVisible();
});

test("real Prometheus trends expose an exact data-table readout", async ({
  page,
}) => {
  await page.goto("/monitoring");
  await expect(
    page.getByRole("heading", { name: "监控分析工作台" }),
  ).toBeVisible();

  const qpsCell = page.locator(
    '[data-slot="kpi-cell"][data-kpi-label="Realtime QPS"]',
  );
  await expect(qpsCell).toBeVisible();

  // Wait until the real Prometheus query has a sample; never accept unknown.
  await expect
    .poll(async () => (await qpsCell.innerText()).replace(/\s+/g, " "), {
      timeout: 60_000,
      intervals: [500, 1000, 2000],
    })
    .not.toContain("未知");

  await expect(page.getByText(/数据源：Prometheus（\/prometheus）/).first()).toBeVisible();

  const qpsPanel = page.locator('[data-slot="chart-panel"]', {
    has: page.getByRole("heading", { name: "HTTP QPS" }),
  });
  await expect(qpsPanel).toHaveAttribute("data-state", "ready");
  const summary = qpsPanel.getByText(/查看数据表（\d+ 个时间点）/);
  await expect(summary).toBeVisible();
  await summary.click();

  const table = qpsPanel.getByRole("table");
  await expect(table).toBeVisible();
  await expect(table.getByRole("columnheader", { name: "时间（UTC）" })).toBeVisible();
  expect(await table.getByRole("row").count()).toBeGreaterThan(1);
});
