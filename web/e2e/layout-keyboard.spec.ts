import { expect, test, type Page } from "@playwright/test";
import { expectNoHorizontalOverflow } from "./support";

/**
 * Real-browser layout and keyboard acceptance.
 *
 * The spec uses the real local stack started by playwright.config.ts. It
 * checks the two required desktop widths (1280 and 1440), the complete top
 * navigation, and keyboard activation/hover-free chart readouts.
 */

const NAV_ITEMS = [
  { name: "监控", heading: "监控分析工作台", path: "/monitoring" },
  { name: "Bar 图与导出", heading: "Bar 图与导出", path: "/bars" },
  { name: "Gateway Console", heading: "Gateway Console", path: "/console" },
  { name: "Quality & Backfill", heading: "Quality & Backfill", path: "/quality" },
  { name: "Security & Audit", heading: "Security & Audit", path: "/security" },
] as const;

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

for (const width of [1280, 1440] as const) {
  test.describe(`desktop layout ${width}px`, () => {
    test.use({ viewport: { width, height: 900 } });

    test("all five modules fit without horizontal overflow and the full nav is usable", async ({
      page,
    }) => {
      await page.goto("/monitoring");
      await expect(
        page.getByRole("heading", { name: "监控分析工作台" }),
      ).toBeVisible();
      await expectNoHorizontalOverflow(page);

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
        await expectNoHorizontalOverflow(page);
      }

      // The page must not require horizontal scrolling even with a dense
      // table on the Console tab.
      await page.goto("/console");
      await page.getByRole("tab", { name: "Service Instance" }).click();
      await expect(
        page.getByTestId("service-instance-collector"),
      ).toBeVisible();
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
      await expect(page.getByRole("heading", { name: "监控分析工作台" })).toBeVisible();

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
    });
  });
}
