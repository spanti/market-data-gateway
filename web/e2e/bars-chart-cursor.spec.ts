import { expect, test } from "@playwright/test";
import { barsQuery, BTC_INSTRUMENT, ETH_INSTRUMENT } from "./support";

/**
 * Real Bars acceptance against the local API and test Dataset.
 *
 * - The chart's HTML data table exposes the exact server decimal strings.
 * - The chart host supports keyboard point readout.
 * - The known Revision-heavy Time Cursor defect is detected and stops.
 */

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
});

test("non-advancing Time Cursor stops with an explicit error instead of looping", async ({
  page,
}) => {
  const barRequests: string[] = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname === "/api/v1/bars" && url.searchParams.get("format") === "json") {
      barRequests.push(url.toString());
    }
  });

  const params = barsQuery({
    instrument: ETH_INSTRUMENT,
    start: "2024-01-02T00:00:00.000Z",
    end: "2024-01-03T00:00:00.000Z",
  });
  await page.goto(`/bars?${params.toString()}`);

  const summary = page.getByTestId("history-summary");
  await expect(summary).toContainText("共 5000 条原始 Revision", {
    timeout: 60_000,
  });
  await expect(summary).toContainText("分页已停止（部分结果）");
  await expect(
    page.getByText(/分页已停止：next_start 未严格前进/),
  ).toBeVisible();
  await expect(page.getByText(/这是已知的 Revision-heavy Time Cursor 缺陷/)).toBeVisible();

  // One page is enough to detect the non-advancing cursor. React StrictMode
  // may mount twice in development, but the walk must never approach the
  // 20-page safety cap.
  expect(barRequests.length).toBeGreaterThan(0);
  expect(barRequests.length).toBeLessThanOrEqual(4);
});
