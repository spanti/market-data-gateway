import type { APIRequestContext, Page } from "@playwright/test";

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
