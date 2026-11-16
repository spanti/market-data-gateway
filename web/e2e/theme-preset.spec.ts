import { expect, test } from "@playwright/test";
import { barsQuery, expectNoHorizontalOverflow } from "./support";

const MODULES = [
  { path: "/monitoring", heading: "监控总览", slug: "monitoring" },
  { path: "/bars", heading: "Bar 图与导出", slug: "bars" },
  { path: "/console", heading: "Gateway Console", slug: "console" },
  { path: "/quality", heading: "Quality & Backfill", slug: "quality" },
  { path: "/security", heading: "Security & Audit", slug: "security" },
] as const;

test("all modules remain usable in light and dark themes", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  const pageErrors: string[] = [];
  const consoleErrors: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("console", (message) => {
    if (message.type() !== "error") {
      return;
    }
    const url = message.location().url;
    // The real Current Contract stack intentionally has no Admin API. Its
    // capability probe returns 404; every other browser error remains fatal.
    const expectedCapabilityGap =
      url.endsWith("/api/v1/admin/capabilities") &&
      message.text().includes("404 (Not Found)");
    if (!expectedCapabilityGap) {
      consoleErrors.push(`${message.text()} (${url})`);
    }
  });

  await page.goto("/monitoring");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");

  for (const theme of ["light", "dark"] as const) {
    if (theme === "dark") {
      await page
        .getByRole("button", { name: "切换主题（当前浅色）" })
        .click();
      await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
      await expect(page.locator("html")).toHaveClass(/dark/);
      expect(
        await page.evaluate(() => localStorage.getItem("mdgw.theme")),
      ).toBe("dark");
    }

    for (const module of MODULES) {
      await page.goto(module.path);
      await expect(
        page.getByRole("heading", { name: module.heading }),
      ).toBeVisible();
      await expect(page.locator("html")).toHaveAttribute("data-theme", theme);
      await expectNoHorizontalOverflow(page);
      await page.screenshot({
        path: testInfo.outputPath(`${theme}-${module.slug}.png`),
        fullPage: true,
        animations: "disabled",
      });
    }
  }

  await testInfo.attach("browser-console-errors", {
    body: JSON.stringify(consoleErrors, null, 2),
    contentType: "application/json",
  });
  expect(pageErrors).toEqual([]);
  expect(consoleErrors).toEqual([]);
});

test("chart legends and readouts stay usable across theme switches", async ({
  page,
}) => {
  await page.goto("/monitoring");
  const trendPanel = page.getByRole("region", { name: "QPS 趋势" });
  await expect(trendPanel).toBeVisible();
  await expect(trendPanel.getByText(/查看数据表/)).toBeVisible();
  await expect(
    trendPanel.getByRole("img", { name: "QPS 趋势", exact: true }),
  ).toBeVisible();

  await page.getByRole("button", { name: "切换主题（当前浅色）" }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(trendPanel).toBeVisible();
  await expect(trendPanel.getByText(/查看数据表/)).toBeVisible();
  await expect(
    trendPanel.getByRole("img", { name: "QPS 趋势", exact: true }),
  ).toBeVisible();

  const query = barsQuery({
    instrument: "binance:BTC/USDT",
    start: "2024-01-01T00:00:00.000Z",
    end: "2024-01-01T00:02:00.000Z",
  });
  await page.goto(`/bars?${query.toString()}`);
  await expect(page.getByTestId("history-summary")).toContainText(
    "共 2 条原始 Revision",
  );
  const barPanel = page.getByRole("region", { name: "最大 Revision Bar 图" });
  await expect(barPanel).toBeVisible();
  await expect(barPanel.getByText(/查看数据表/)).toBeVisible();
  await expect(
    barPanel.getByRole("img", { name: /最大 Revision Bar 价格图/ }),
  ).toBeVisible();

  await page.getByRole("button", { name: "切换主题（当前深色）" }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await expect(barPanel).toBeVisible();
  await expect(barPanel.getByText(/查看数据表/)).toBeVisible();
  await expect(
    barPanel.getByRole("img", { name: /最大 Revision Bar 价格图/ }),
  ).toBeVisible();
});
