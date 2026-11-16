import { expect, type APIRequestContext, type Page } from "@playwright/test";

/** Separators that UI-generated copy must never contain. */
export const BANNED_UI_SYMBOLS = ["·", "："] as const;

export interface BannedSymbolHit {
  symbol: string;
  text: string;
}

/**
 * Collects visible-DOM text containing banned UI separators. Raw evidence
 * regions (`data-raw-evidence="true"`) and visually hidden text are exempt.
 */
export async function bannedUiSymbolHits(
  page: Page,
  scopeSelector = "body",
): Promise<BannedSymbolHit[]> {
  return page.evaluate(
    ({ selector, symbols }) => {
      const root = document.querySelector(selector);
      if (!root) {
        return [{ symbol: "", text: `missing scan scope ${selector}` }];
      }
      const hits: Array<{ symbol: string; text: string }> = [];
      const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
      const skipped = new Set(["SCRIPT", "STYLE", "NOSCRIPT", "TEMPLATE"]);
      while (walker.nextNode()) {
        const node = walker.currentNode;
        const element = node.parentElement;
        if (!element || skipped.has(element.tagName)) {
          continue;
        }
        if (element.closest('[data-raw-evidence="true"]')) {
          continue;
        }
        if (element.closest(".sr-only")) {
          continue;
        }
        const style = window.getComputedStyle(element);
        if (
          style.display === "none" ||
          style.visibility === "hidden" ||
          style.opacity === "0"
        ) {
          continue;
        }
        const text = node.textContent ?? "";
        for (const symbol of symbols) {
          if (text.includes(symbol)) {
            hits.push({ symbol, text: text.trim().slice(0, 160) });
          }
        }
      }
      return hits;
    },
    { selector: scopeSelector, symbols: [...BANNED_UI_SYMBOLS] },
  );
}

export async function expectNoBannedUiSymbols(
  page: Page,
  scopeSelector = "body",
): Promise<void> {
  const hits = await bannedUiSymbolHits(page, scopeSelector);
  expect(hits, `banned UI symbols: ${JSON.stringify(hits)}`).toEqual([]);
}

export function helpTrigger(page: Page, name: string) {
  return page.getByRole("button", { name, exact: true });
}

/** Hover opens help and keeps the trigger accessible by name. */
export async function openHelpByHover(page: Page, name: string) {
  const trigger = helpTrigger(page, name);
  await expect(trigger).toBeVisible();
  await trigger.hover();
  const tooltip = page.getByRole("tooltip");
  await expect(tooltip).toBeVisible();
  return { trigger, tooltip };
}

/** Keyboard focus opens the same help without a pointer. */
export async function openHelpByKeyboard(page: Page, name: string) {
  const trigger = helpTrigger(page, name);
  await trigger.focus();
  const tooltip = page.getByRole("tooltip");
  await expect(tooltip).toBeVisible();
  return { trigger, tooltip };
}

/** A touch tap toggles help; a second tap closes it. */
export async function toggleHelpByTouch(page: Page, name: string) {
  const trigger = helpTrigger(page, name);
  await trigger.tap();
  const tooltip = page.getByRole("tooltip");
  await expect(tooltip).toBeVisible();
  await trigger.tap();
  await expect(tooltip).toBeHidden();
}

/** Escape closes help opened by hover or focus. */
export async function closeHelpWithEscape(page: Page) {
  await page.keyboard.press("Escape");
  await expect(page.getByRole("tooltip")).toBeHidden();
}

/**
 * The help surface must stay readable (bounded width, wrapped) and must not
 * sit on top of its own trigger.
 */
export async function expectTooltipUsable(page: Page) {
  const tooltip = page.getByRole("tooltip");
  const trigger = page.locator(
    '[data-slot="help-trigger"][aria-expanded="true"]',
  );
  const tipBox = await tooltip.boundingBox();
  const triggerBox = await trigger.first().boundingBox();
  expect(tipBox).not.toBeNull();
  expect(triggerBox).not.toBeNull();
  if (!tipBox || !triggerBox) {
    return;
  }
  expect(tipBox.width).toBeLessThanOrEqual(340);
  const overlapWidth = Math.max(
    0,
    Math.min(tipBox.x + tipBox.width, triggerBox.x + triggerBox.width) -
      Math.max(tipBox.x, triggerBox.x),
  );
  const overlapHeight = Math.max(
    0,
    Math.min(tipBox.y + tipBox.height, triggerBox.y + triggerBox.height) -
      Math.max(tipBox.y, triggerBox.y),
  );
  expect(
    overlapWidth * overlapHeight,
    "help must not cover its trigger",
  ).toBe(0);
}

/** Local control endpoint of the real role stack (see playwright.config.ts). */
export const E2E_CONTROL_URL =
  process.env.MDGW_E2E_CONTROL_URL ??
  `http://127.0.0.1:${process.env.MDGW_E2E_CONTROL_PORT ?? "8096"}`;

export const BTC_INSTRUMENT = "binance:BTC/USDT";
export const ETH_INSTRUMENT = "binance:ETH/USDT";

export function barsQuery(params: {
  instrument: string;
  interval?: string;
  start: string;
  end: string;
}): URLSearchParams {
  return new URLSearchParams({
    instrument_id: params.instrument,
    interval: params.interval ?? "1m",
    start: params.start,
    end: params.end,
  });
}

/** Enqueue closed Bars into the real collector feed and wait for acceptance. */
export async function emitBars(
  request: APIRequestContext,
  count: number,
): Promise<void> {
  const response = await request.post(`${E2E_CONTROL_URL}/emit?count=${count}`);
  if (!response.ok()) {
    throw new Error(
      `control /emit failed: HTTP ${response.status()} ${await response.text()}`,
    );
  }
}

/** Fails when an element makes the page horizontally scrollable. */
export async function expectNoHorizontalOverflow(page: Page): Promise<void> {
  const overflow = await page.evaluate(() => {
    const root = document.documentElement;
    return {
      scrollWidth: root.scrollWidth,
      clientWidth: root.clientWidth,
      bodyScrollWidth: document.body.scrollWidth,
    };
  });
  if (
    overflow.scrollWidth > overflow.clientWidth + 1 ||
    overflow.bodyScrollWidth > overflow.clientWidth + 1
  ) {
    throw new Error(
      `horizontal overflow: ${JSON.stringify(overflow)} at ${page.viewportSize()?.width}px`,
    );
  }
}
