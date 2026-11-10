import { expect, test, type Page } from "@playwright/test";
import { barsQuery, expectNoHorizontalOverflow } from "./support";

const MODULES = [
  { path: "/monitoring", heading: "监控分析工作台", slug: "monitoring" },
  { path: "/bars", heading: "Bar 图与导出", slug: "bars" },
  { path: "/console", heading: "Gateway Console", slug: "console" },
  { path: "/quality", heading: "Quality & Backfill", slug: "quality" },
  { path: "/security", heading: "Security & Audit", slug: "security" },
] as const;

async function resolvedCssColor(page: Page, variable: string): Promise<string> {
  return page.evaluate((name) => {
    const probe = document.createElement("span");
    probe.style.color = `var(${name})`;
    probe.style.position = "absolute";
    probe.style.visibility = "hidden";
    document.body.append(probe);
    const color = getComputedStyle(probe).color;
    probe.remove();
    return color;
  }, variable);
}

function legendSwatch(page: Page, chartTitle: string) {
  return page
    .locator('[data-slot="chart-panel"]', { hasText: chartTitle })
    .locator('span[aria-hidden="true"][style]')
    .first();
}

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

test("chart legends follow the active CSS palette after theme switches", async ({
  page,
}) => {
  await page.goto("/monitoring");
  const trendPanel = page.locator('[data-slot="chart-panel"]', {
    has: page.getByRole("heading", { name: "HTTP QPS" }),
  });
  await expect(trendPanel).toHaveAttribute("data-state", "ready");
  const trendSwatch = legendSwatch(page, "HTTP QPS");
  await expect(trendSwatch).toBeVisible();
  await expect(trendSwatch).toHaveCSS(
    "background-color",
    await resolvedCssColor(page, "--chart-series-1"),
  );

  await page.getByRole("button", { name: "切换主题（当前浅色）" }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(trendSwatch).toHaveCSS(
    "background-color",
    await resolvedCssColor(page, "--chart-series-1"),
  );

  const query = barsQuery({
    instrument: "binance:BTC/USDT",
    start: "2024-01-01T00:00:00.000Z",
    end: "2024-01-01T00:02:00.000Z",
  });
  await page.goto(`/bars?${query.toString()}`);
  await expect(page.getByTestId("history-summary")).toContainText(
    "共 2 条原始 Revision",
  );
  const barSwatch = legendSwatch(page, "最大 Revision Bar 图");
  await expect(barSwatch).toBeVisible();
  await expect(barSwatch).toHaveCSS(
    "background-color",
    await resolvedCssColor(page, "--chart-series-1"),
  );

  await page.getByRole("button", { name: "切换主题（当前深色）" }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  await expect(barSwatch).toHaveCSS(
    "background-color",
    await resolvedCssColor(page, "--chart-series-1"),
  );
});
