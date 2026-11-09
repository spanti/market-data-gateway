import fs from "node:fs/promises";
import { expect, test } from "@playwright/test";

/**
 * Real small-file CSV download against a real local gateway API and a
 * temporary Dataset created by playwright.config.ts. No response is mocked.
 */
test("downloads the real small CSV with server bytes and filename", async ({
  page,
}) => {
  const params = new URLSearchParams({
    instrument_id: "binance:BTC/USDT",
    interval: "1m",
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

  const downloadPromise = page.waitForEvent("download");
  await page
    .getByRole("button", { name: "直接下载 CSV（≤31 天）" })
    .click();
  const download = await downloadPromise;

  // The gateway sends `Content-Disposition: inline; filename="1m-bars.csv"`.
  // The console must still trigger a download with that filename.
  expect(download.suggestedFilename()).toBe("1m-bars.csv");

  const downloadedPath = await download.path();
  expect(downloadedPath).toBeTruthy();
  const text = await fs.readFile(downloadedPath!, "utf8");
  const lines = text.trimEnd().split("\n");

  expect(lines[0]).toBe(
    "schema_version,data_type,instrument_id,interval,open_ts,close_ts,event_ts,ingest_ts,feed,seq,aggregation_source,open,high,low,close,volume,trade_count,revision",
  );
  expect(lines).toHaveLength(3);
  expect(lines[1]).toContain(
    "1,bar,binance:BTC/USDT,1m,2024-01-01T00:00:00.000000000Z,2024-01-01T00:01:00.000000000Z,2024-01-01T00:01:00.000000000Z,",
  );
  expect(lines[1]).toContain(
    ",fake,,external,65000.1,65010.0,64990.0,65005.5,12.34,128,0",
  );
  expect(lines[2]).toContain(
    "1,bar,binance:BTC/USDT,1m,2024-01-01T00:01:00.000000000Z,2024-01-01T00:02:00.000000000Z,2024-01-01T00:02:00.000000000Z,",
  );
  expect(lines[2]).toContain(
    ",fake,,external,65005.5,65020.0,65000.0,65015.0,10.5,64,0",
  );
});
