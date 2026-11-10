import { delay, http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import {
  CsvDownloadCancelledError,
  computeCsvProgress,
  defaultCsvFilename,
  fetchBarsCsv,
} from "@/api/endpoints/bars";
import { HttpError } from "@/api/http/types";
import { server } from "@/test/server";

const CSV_TEXT = [
  "schema_version,data_type,instrument_id,interval,open_ts,close_ts,event_ts,ingest_ts,feed,seq,aggregation_source,open,high,low,close,volume,trade_count,revision",
  "1,bar,binance:BTC/USDT,1m,2024-01-01T00:00:00.000000000Z,2024-01-01T00:01:00.000000000Z,2024-01-01T00:01:00.000000000Z,2026-11-09T04:24:42.946489000Z,fake,,external,65000.1,65010.0,64990.0,65005.5,12.34,128,0",
  "1,bar,binance:BTC/USDT,1m,2024-01-01T00:01:00.000000000Z,2024-01-01T00:02:00.000000000Z,2024-01-01T00:02:00.000000000Z,2026-11-09T04:24:42.946489000Z,fake,,external,65005.5,65020.0,65000.0,65015.0,10.5,64,0",
  "",
].join("\n");

function readBlobText(blob: Blob): Promise<string> {
  if (typeof blob.text === "function") {
    return blob.text();
  }
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result ?? ""));
    reader.onerror = () => reject(reader.error);
    reader.readAsText(blob);
  });
}

describe("CSV download endpoint", () => {
  it("returns the exact Blob bytes, inline filename and trusted length", async () => {
    server.use(
      http.get("*/api/v1/bars", ({ request }) => {
        const params = new URL(request.url).searchParams;
        expect(params.get("format")).toBe("csv");
        expect(params.get("instrument")).toBe("binance:BTC/USDT");
        return HttpResponse.text(CSV_TEXT, {
          headers: {
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": 'inline; filename="1m-bars.csv"',
            "Content-Length": String(new TextEncoder().encode(CSV_TEXT).length),
            "X-Request-ID": "csv-request-1",
          },
        });
      }),
    );

    const result = await fetchBarsCsv({
      instrument: "binance:BTC/USDT",
      interval: "1m",
      start: "2024-01-01T00:00:00.000Z",
      end: "2024-01-01T00:02:00.000Z",
    });

    expect(result.filename).toBe("1m-bars.csv");
    expect(result.requestId).toBe("csv-request-1");
    expect(result.contentLength).toBe(
      new TextEncoder().encode(CSV_TEXT).length,
    );
    expect(await readBlobText(result.blob)).toBe(CSV_TEXT);
  });

  it("prefers filename* and falls back to <interval>-bars.csv", async () => {
    server.use(
      http.get("*/api/v1/bars", () =>
        HttpResponse.text(CSV_TEXT, {
          headers: {
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition":
              "attachment; filename=\"fallback.csv\"; filename*=UTF-8''%E6%9F%B1%E5%9B%BE.csv",
          },
        }),
      ),
    );

    const withExtended = await fetchBarsCsv({ interval: "5m" });
    expect(withExtended.filename).toBe("柱图.csv");

    server.use(
      http.get("*/api/v1/bars", () =>
        HttpResponse.text(CSV_TEXT, {
          headers: { "Content-Type": "text/csv; charset=utf-8" },
        }),
      ),
    );
    const withoutDisposition = await fetchBarsCsv({ interval: "5m" });
    expect(withoutDisposition.filename).toBe("5m-bars.csv");
    expect(defaultCsvFilename("1h")).toBe("1h-bars.csv");
  });

  it("does not mutate or re-serialize the CSV bytes", async () => {
    const original = new Blob([CSV_TEXT], { type: "text/csv" });
    server.use(
      http.get("*/api/v1/bars", () =>
        HttpResponse.text(CSV_TEXT, {
          headers: { "Content-Type": "text/csv; charset=utf-8" },
        }),
      ),
    );

    const result = await fetchBarsCsv({ interval: "1m" });

    expect(await readBlobText(result.blob)).toBe(await readBlobText(original));
    expect(result.blob.type).toContain("text/csv");
  });

  it("parses Blob error responses with the unified envelope and Request ID", async () => {
    server.use(
      http.get("*/api/v1/bars", () =>
        new HttpResponse(
          JSON.stringify({
            error: {
              code: "range_too_large",
              message:
                "JSON output is limited to 100000 Bars; use format=arrow or format=csv for larger ranges",
              details: { row_count: 100000, limit: 100000 },
              request_id: "blob-error-request",
            },
          }),
          {
            status: 400,
            headers: { "Content-Type": "application/json" },
          },
        ),
      ),
    );

    await expect(fetchBarsCsv({ interval: "1m" })).rejects.toMatchObject({
      code: "range_too_large",
      requestId: "blob-error-request",
      status: 400,
    });
  });

  it("supports AbortController cancellation", async () => {
    server.use(
      http.get("*/api/v1/bars", async () => {
        await delay(500);
        return HttpResponse.text(CSV_TEXT);
      }),
    );

    const controller = new AbortController();
    const pending = fetchBarsCsv(
      { interval: "1m" },
      { signal: controller.signal },
    );
    controller.abort();

    await expect(pending).rejects.toBeInstanceOf(CsvDownloadCancelledError);
  });
});

describe("CSV progress", () => {
  it("shows a percentage only for a trusted total", () => {
    expect(computeCsvProgress(512, 1024)).toEqual({
      loadedBytes: 512,
      totalBytes: 1024,
      percent: 50,
    });
  });

  it("reports bytes with an unknown total instead of inventing a percentage", () => {
    expect(computeCsvProgress(4096, null)).toEqual({
      loadedBytes: 4096,
      totalBytes: null,
      percent: null,
    });
    expect(computeCsvProgress(4096, 0).totalBytes).toBeNull();
    expect(computeCsvProgress(4096, Number.NaN).percent).toBeNull();
  });

  it("never reports more than 100 percent", () => {
    expect(computeCsvProgress(2048, 1024).percent).toBe(100);
  });
});

describe("Blob error adapter", () => {
  it("exposes HttpError for unexpected binary failures", async () => {
    server.use(
      http.get("*/api/v1/bars", () =>
        new HttpResponse("not-json", {
          status: 500,
          headers: { "Content-Type": "text/plain" },
        }),
      ),
    );

    await expect(fetchBarsCsv({ interval: "1m" })).rejects.toBeInstanceOf(
      HttpError,
    );
    await expect(fetchBarsCsv({ interval: "1m" })).rejects.toMatchObject({
      code: "blob_error_response",
    });
  });
});
