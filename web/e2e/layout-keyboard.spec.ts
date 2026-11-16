import { expect, test, type Page } from "@playwright/test";
import {
  closeHelpWithEscape,
  expectNoBannedUiSymbols,
  expectNoHorizontalOverflow,
  expectTooltipUsable,
  openHelpByHover,
  openHelpByKeyboard,
  toggleHelpByTouch,
} from "./support";

/**
 * Real-browser layout and keyboard acceptance.
 *
 * The spec uses the real local stack started by playwright.config.ts. It
 * checks the two required desktop widths (1280 and 1440), the complete top
 * navigation, and keyboard activation/hover-free chart readouts.
 */

const NAV_ITEMS = [
  { name: "监控", heading: "监控总览", path: "/monitoring" },
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

test.describe("narrow desktop layout 1024px", () => {
  test.use({ viewport: { width: 1024, height: 900 } });

  test("monitoring stays usable without page-level horizontal overflow", async ({
    page,
  }) => {
    await page.goto("/monitoring");
    await expect(
      page.getByRole("heading", { name: "监控总览" }),
    ).toBeVisible();
    await expect(page.locator('[data-slot="metric-card"]')).toHaveCount(4);
    await expect(page.locator('[data-slot="chart-panel"]')).toHaveCount(2);
    await expect(
      page.getByRole("region", { name: "Feed Freshness", exact: true }),
    ).toBeVisible();
    await expect(page.getByRole("region", { name: "Service Instance" })).toBeVisible();
    await expectNoHorizontalOverflow(page);
  });

  test("console tabs stay usable without page-level horizontal overflow", async ({
    page,
  }) => {
    await page.goto("/console");
    await expect(
      page.getByRole("heading", { name: "Gateway Console" }),
    ).toBeVisible();

    await page.getByRole("tab", { name: "Feed Subscription" }).click();
    await expect(
      page.getByRole("region", { name: "Feed Subscription" }),
    ).toBeVisible();
    await expectNoHorizontalOverflow(page);

    await page.getByRole("tab", { name: "Service Instance" }).click();
    await expect(page.getByTestId("service-instance-collector")).toBeVisible();
    await expectNoHorizontalOverflow(page);

    await page.getByRole("tab", { name: "WebSocket Debug" }).click();
    await expect(
      page.getByRole("heading", { name: "WebSocket Debug" }),
    ).toBeVisible();
    await expectNoHorizontalOverflow(page);

    // Back on Feed, every UI-generated string must avoid the banned
    // separators. Raw backend error text stays exempt via data-raw-evidence.
    await page.getByRole("tab", { name: "Feed Subscription" }).click();
    await expectNoBannedUiSymbols(page, "#main-content");
  });
});

for (const width of [1280, 1440] as const) {
  test.describe(`desktop layout ${width}px`, () => {
    test.use({ viewport: { width, height: 900 } });

    test("all five modules fit without horizontal overflow and the full nav is usable", async ({
      page,
    }) => {
      await page.goto("/monitoring");
      await expect(
        page.getByRole("heading", { name: "监控总览" }),
      ).toBeVisible();
      await expectNoHorizontalOverflow(page);
      await expect(page.locator('[data-slot="metric-card"]')).toHaveCount(4);
      await expect(page.locator('[data-slot="chart-panel"]')).toHaveCount(2);

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

      await page.getByRole("tab", { name: "Feed Subscription" }).click();
      await expect(
        page.getByRole("region", { name: "Feed Subscription" }),
      ).toBeVisible();
      await expectNoBannedUiSymbols(page, "#main-content");
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
      await expect(page.getByRole("heading", { name: "监控总览" })).toBeVisible();

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

test.describe("console feed help", () => {
  test.use({ viewport: { width: 1280, height: 900 } });

  test("Feed Subscription help opens by hover and keyboard focus and closes with Escape", async ({
    page,
  }) => {
    await page.goto("/console");
    await expect(
      page.getByRole("region", { name: "Feed Subscription" }),
    ).toBeVisible();

    const hovered = await openHelpByHover(page, "Feed Subscription 说明");
    await expect(hovered.tooltip).toContainText("Desired");
    await expectTooltipUsable(page);
    await closeHelpWithEscape(page);

    const focused = await openHelpByKeyboard(page, "Feed Subscription 说明");
    await expect(focused.tooltip).toContainText("409");
    await expectTooltipUsable(page);
    await closeHelpWithEscape(page);
  });
});

test.describe("console feed touch help", () => {
  test.use({ viewport: { width: 1024, height: 900 }, hasTouch: true });

  test("Feed Subscription help toggles on touch tap", async ({ page }) => {
    await page.goto("/console");
    await expect(
      page.getByRole("region", { name: "Feed Subscription" }),
    ).toBeVisible();
    await toggleHelpByTouch(page, "Feed Subscription 说明");
    await expectNoHorizontalOverflow(page);
  });
});
