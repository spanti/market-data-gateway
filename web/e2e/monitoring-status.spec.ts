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

  // Every live status is announced as readable text; color and the decorative
  // icon are redundant encodings, never the only one.
  const liveStatuses = page.getByRole("status");
  await expect(liveStatuses.first()).toBeVisible();
  const count = await liveStatuses.count();
  expect(count).toBeGreaterThan(0);
  for (let index = 0; index < count; index += 1) {
    const status = liveStatuses.nth(index);
    if (!(await status.isVisible())) {
      continue;
    }
    expect((await status.innerText()).trim().length).toBeGreaterThan(0);
  }

  // Key static statuses also remain readable text.
  await expect(page.getByText("正常").first()).toBeVisible();
  await expect(page.getByText("3 个 Service Role")).toBeVisible();

  // The real collector status is surfaced as Ready / not-ready text.
  const collector = page.getByRole("group", {
    name: "collector Service Role",
  });
  await expect(collector).toBeVisible();
  await expect(collector.getByRole("status")).toContainText(
    /Ready|未就绪|未知/,
  );
});

test("D foundation exposes four KPI cards and separated freshness areas", async ({
  page,
}) => {
  await page.goto("/monitoring");
  await expect(
    page.getByRole("heading", { name: "监控总览" }),
  ).toBeVisible();

  for (const label of KPI_LABELS) {
    await expect(page.getByRole("group", { name: label })).toBeVisible();
  }

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

  // Current Contract exposes one process per Service Role, and the card says
  // so; Service Instance is not claimed for per-Role rows.
  const service = page.getByRole("region", {
    name: "Service Role",
    exact: true,
  });
  await expect(service).toBeVisible();
  await expect(service).toContainText("3 个 Service Role");
  for (const role of ["collector", "api", "replay"] as const) {
    await expect(
      service.getByRole("group", { name: `${role} Service Role` }),
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

  const qpsCell = page.getByRole("group", { name: "Realtime QPS" });
  await expect(qpsCell).toBeVisible();

  // Wait until the real Prometheus query has a sample; never accept unknown.
  await expect
    .poll(async () => (await qpsCell.innerText()).replace(/\s+/g, " "), {
      timeout: 60_000,
      intervals: [500, 1000, 2000],
    })
    .not.toContain("未知");

  await expect(page.getByText(/Prometheus（\/prometheus）/).first()).toBeVisible();

  const qpsPanel = page.getByRole("region", { name: "QPS 趋势" });
  await expect(qpsPanel).toBeVisible();

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
