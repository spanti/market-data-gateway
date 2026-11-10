import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import {
  buildBarsV2SearchParams,
  fetchBarsV2History,
  fetchBarsV2Json,
} from "@/api/endpoints/bars-v2";
import type { BarMessageEnvelope } from "@/api/endpoints/bars";
import { server } from "@/test/server";

const INSTRUMENT = "binance:BTC/USDT";

function envelope(openTs: string, revision = 0): BarMessageEnvelope {
  return {
    schema_version: 1,
    data_type: "bar",
    instrument_id: INSTRUMENT,
    event_ts: openTs,
    ingest_ts: "2026-11-09T04:24:42.946489000Z",
    feed: "fake",
    seq: null,
    payload: {
      interval: "1m",
      open_ts: openTs,
      close_ts: openTs,
      aggregation_source: "external",
      open: "65000.1",
      high: "65010.0",
      low: "64990.0",
      close: "65005.5",
      volume: "12.34",
      trade_count: 128,
      revision,
    },
  };
}

function pageResponse(
  bars: BarMessageEnvelope[],
  nextCursor: string | null,
  hasMore: boolean,
) {
  return HttpResponse.json({
    bars,
    page: { has_more: hasMore, next_cursor: nextCursor },
  });
}

describe("buildBarsV2SearchParams", () => {
  it("sends the required identity, page size and optional cursor", () => {
    const params = buildBarsV2SearchParams({
      interval: "1m",
      instrument: INSTRUMENT,
      limit: 5000,
      start: "2024-01-01T00:00:00.000Z",
      end: "2024-01-02T00:00:00.000Z",
      cursor: "opaque.token",
    });

    expect(params.get("interval")).toBe("1m");
    expect(params.get("instrument")).toBe(INSTRUMENT);
    expect(params.get("limit")).toBe("5000");
    expect(params.get("start")).toBe("2024-01-01T00:00:00.000Z");
    expect(params.get("end")).toBe("2024-01-02T00:00:00.000Z");
    expect(params.get("cursor")).toBe("opaque.token");
  });

  it("omits the cursor and start when not supplied", () => {
    const params = buildBarsV2SearchParams({
      interval: "5m",
      instrument: INSTRUMENT,
      limit: 1,
    });

    expect(params.has("cursor")).toBe(false);
    expect(params.has("start")).toBe(false);
  });
});

describe("fetchBarsV2Json", () => {
  it("calls /api/v2/bars and preserves raw revisions", async () => {
    server.use(
      http.get("*/api/v2/bars", ({ request }) => {
        const url = new URL(request.url);
        expect(url.searchParams.get("instrument")).toBe(INSTRUMENT);
        return pageResponse(
          [envelope("2024-01-01T00:00:00.000000000Z", 0), envelope("2024-01-01T00:00:00.000000000Z", 1)],
          "cursor-1",
          true,
        );
      }),
    );

    const page = await fetchBarsV2Json({
      interval: "1m",
      instrument: INSTRUMENT,
      limit: 2,
    });

    expect(page.bars).toHaveLength(2);
    expect(page.bars.map((bar) => bar.payload.revision)).toEqual([0, 1]);
    expect(page.page).toEqual({ has_more: true, next_cursor: "cursor-1" });
  });

  it("surfaces the server error envelope", async () => {
    server.use(
      http.get("*/api/v2/bars", () =>
        HttpResponse.json(
          {
            error: {
              code: "invalid_parameter",
              message: "query parameter 'cursor' is not a valid cursor",
              details: { field: "cursor", reason: "invalid_cursor" },
              request_id: "cursor-error-request",
            },
          },
          { status: 400 },
        ),
      ),
    );

    await expect(
      fetchBarsV2Json({
        interval: "1m",
        instrument: INSTRUMENT,
        limit: 1,
        cursor: "bad",
      }),
    ).rejects.toMatchObject({
      code: "invalid_parameter",
      requestId: "cursor-error-request",
      details: { field: "cursor", reason: "invalid_cursor" },
    });
  });
});

describe("fetchBarsV2History", () => {
  it("walks next_cursor pages until has_more is false", async () => {
    const requested: URL[] = [];
    server.use(
      http.get("*/api/v2/bars", ({ request }) => {
        const url = new URL(request.url);
        requested.push(url);
        const cursor = url.searchParams.get("cursor");
        if (cursor === null) {
          return pageResponse(
            [envelope("2024-01-01T00:00:00.000000000Z", 0)],
            "cursor-1",
            true,
          );
        }
        if (cursor === "cursor-1") {
          return pageResponse(
            [envelope("2024-01-01T00:01:00.000000000Z", 0)],
            "cursor-2",
            true,
          );
        }
        return pageResponse(
          [envelope("2024-01-01T00:02:00.000000000Z", 0)],
          null,
          false,
        );
      }),
    );

    const result = await fetchBarsV2History({
      instrument: INSTRUMENT,
      interval: "1m",
      start: "2024-01-01T00:00:00.000Z",
      end: "2024-01-05T00:00:00.000Z",
      limit: 1,
    });

    expect(result.complete).toBe(true);
    expect(result.pages).toBe(3);
    expect(result.bars).toHaveLength(3);
    expect(result.stop).toBeNull();
    expect(requested.map((url) => url.searchParams.get("cursor"))).toEqual([
      null,
      "cursor-1",
      "cursor-2",
    ]);
    // start is only a first-page parameter; cursor pages send only cursor.
    expect(requested[0]?.searchParams.get("start")).toBe(
      "2024-01-01T00:00:00.000Z",
    );
    expect(requested[1]?.searchParams.get("start")).toBeNull();
  });

  it("stops immediately when next_cursor does not advance and never loops", async () => {
    let requests = 0;
    server.use(
      http.get("*/api/v2/bars", ({ request }) => {
        requests += 1;
        const cursor = new URL(request.url).searchParams.get("cursor");
        return pageResponse(
          [envelope("2024-01-01T00:00:00.000000000Z")],
          cursor ?? "same-cursor",
          true,
        );
      }),
    );

    const result = await fetchBarsV2History({
      instrument: INSTRUMENT,
      interval: "1m",
      start: "2024-01-01T00:00:00.000Z",
      end: "2024-01-05T00:00:00.000Z",
      limit: 1,
    });

    // Page 1 returns same-cursor; page 2 returns same-cursor again.
    expect(requests).toBe(2);
    expect(result.complete).toBe(false);
    expect(result.pages).toBe(2);
    expect(result.stop?.reason).toBe("cursor_non_advance");
    expect(result.stop?.message).toContain("部分结果");
  });

  it("caps the page count and reports partial results", async () => {
    let request = 0;
    server.use(
      http.get("*/api/v2/bars", () => {
        request += 1;
        return pageResponse(
          [envelope("2024-01-01T00:00:00.000000000Z")],
          `cursor-${request}`,
          true,
        );
      }),
    );

    const result = await fetchBarsV2History({
      instrument: INSTRUMENT,
      interval: "1m",
      start: "2024-01-01T00:00:00.000Z",
      end: "2024-01-05T00:00:00.000Z",
      limit: 10,
      maxPages: 3,
    });

    expect(result.pages).toBe(3);
    expect(result.complete).toBe(false);
    expect(result.stop?.reason).toBe("page_cap");
  });

  it("stops when a page omits the required page metadata", async () => {
    server.use(
      http.get("*/api/v2/bars", () =>
        HttpResponse.json({
          bars: [envelope("2024-01-01T00:00:00.000000000Z")],
        }),
      ),
    );

    const result = await fetchBarsV2History({
      instrument: INSTRUMENT,
      interval: "1m",
      start: "2024-01-01T00:00:00.000Z",
      end: "2024-01-05T00:00:00.000Z",
      limit: 10,
    });

    expect(result.complete).toBe(false);
    expect(result.stop?.reason).toBe("cursor_protocol_error");
  });

  it("surfaces the server range_too_large error instead of retrying", async () => {
    server.use(
      http.get("*/api/v2/bars", () =>
        HttpResponse.json(
          {
            error: {
              code: "range_too_large",
              message: "limit too large",
              details: { field: "limit", max_limit: 100000 },
              request_id: "request-too-large",
            },
          },
          { status: 400 },
        ),
      ),
    );

    await expect(
      fetchBarsV2History({
        instrument: INSTRUMENT,
        interval: "1m",
        start: "2024-01-01T00:00:00.000Z",
        end: "2024-01-02T00:00:00.000Z",
        limit: 100000,
      }),
    ).rejects.toMatchObject({
      code: "range_too_large",
      requestId: "request-too-large",
    });
  });
});
