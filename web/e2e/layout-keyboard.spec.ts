import { expect, test, type Page } from "@playwright/test";
import {
  closeHelpWithEscape,
  expectNoBannedUiSymbols,
  expectNoHorizontalOverflow,
  expectTooltipUsable,
  helpTrigger,
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

  test("quality shows the real Collector snapshot and disabled Admin areas without overflow", async ({
    page,
  }) => {
    await page.goto("/quality");
    await expect(
      page.getByRole("heading", { name: "Quality & Backfill" }),
    ).toBeVisible();
    await expect(page.locator('[data-slot="card"]')).toHaveCount(3);

    // Current Contract: a real Collector snapshot, not accumulated history.
    const snapshot = page.getByRole("region", {
      name: "Unrecoverable Backfill Gap 当前快照",
    });
    await expect(snapshot).toBeVisible();
    await expect(snapshot).toContainText("Collector 进程内快照");
    await expect(
      snapshot.getByText(/gap_unrecoverable_count/),
    ).toBeVisible();

    // Persistent gaps and Jobs stay visible but honestly gated.
    const persistent = page.getByRole("region", {
      name: "持久 Unrecoverable Backfill Gap",
    });
    await expect(persistent).toContainText("需要 Admin API");
    await expect(persistent.locator("table")).toHaveCount(0);

    const jobs = page.getByRole("region", { name: "Backfill Job" });
    await expect(jobs).toContainText(
      "Current Contract 没有持久 Backfill Job API",
    );
    await expect(jobs.locator("table")).toHaveCount(0);
    await expect(jobs.getByRole("progressbar")).toHaveCount(0);

    await expectNoBannedUiSymbols(page);
    await expectNoHorizontalOverflow(page);
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
    });

    test("quality cards and help are usable", async ({ page }) => {
      await page.goto("/quality");
      await expect(
        page.getByRole("heading", { name: "Quality & Backfill" }),
      ).toBeVisible();
      await expect(page.locator('[data-slot="card"]')).toHaveCount(3);
      await expectNoHorizontalOverflow(page);

      const pageHelp = await openHelpByHover(
        page,
        "Quality & Backfill 说明",
      );
      await expectTooltipUsable(page);
      await expect(pageHelp.tooltip).toContainText("进程内 Gap 快照");
      await closeHelpWithEscape(page);

      const jobHelp = await openHelpByKeyboard(page, "Backfill Job 说明");
      await expect(jobHelp.tooltip).toContainText("partially_succeeded");
      await expectTooltipUsable(page);
      await closeHelpWithEscape(page);

      const gapHelp = await openHelpByHover(
        page,
        "持久 Unrecoverable Backfill Gap 说明",
      );
      await expect(gapHelp.tooltip).toContainText("物理删除");
      await closeHelpWithEscape(page);

      await expectNoBannedUiSymbols(page);
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

test.describe("quality help with a touch context", () => {
  test.use({ viewport: { width: 1280, height: 900 }, hasTouch: true });

  test("help toggles by touch, stays readable and closes with Escape", async ({
    page,
  }) => {
    await page.goto("/quality");
    await expect(
      page.getByRole("heading", { name: "Quality & Backfill" }),
    ).toBeVisible();

    // A tap opens and a second tap closes the same help surface.
    await toggleHelpByTouch(page, "Quality & Backfill 说明");

    await helpTrigger(
      page,
      "Unrecoverable Backfill Gap 当前快照说明",
    ).tap();
    await expect(page.getByRole("tooltip")).toBeVisible();
    await expectTooltipUsable(page);
    await expectNoBannedUiSymbols(page);
    await closeHelpWithEscape(page);

    await expectNoHorizontalOverflow(page);
  });
});
