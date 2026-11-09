import { describe, expect, it } from "vitest";
import { decideBarsPageStep } from "@/lib/bar-pagination";
import type { BarsResponse } from "@/api/endpoints/bars";

const BASE_INPUT = {
  requestStart: "2024-01-01T00:00:00.000Z",
  lastReturnedOpenTs: null,
  pageCount: 1,
  maxPages: 10,
};

function page(
  bars: BarsResponse["bars"],
  nextStart: string | null,
  hasMore: boolean,
): BarsResponse {
  return { bars, page: { has_more: hasMore, next_start: nextStart } };
}

function bar(openTs: string) {
  return {
    schema_version: 1,
    data_type: "bar" as const,
    instrument_id: "binance:BTC/USDT",
    event_ts: "2024-01-01T00:01:00.000000000Z",
    ingest_ts: "2026-11-09T04:24:42.946489000Z",
    feed: "fake",
    seq: null,
    payload: {
      interval: "1m" as const,
      open_ts: openTs,
      close_ts: "2024-01-01T00:01:00.000000000Z",
      aggregation_source: "external" as const,
      open: "1.0",
      high: "2.0",
      low: "0.5",
      close: "1.5",
      volume: "10.0",
      trade_count: 1,
      revision: 0,
    },
  };
}

describe("pagination cursor guard", () => {
  it("completes when has_more is false", () => {
    expect(
      decideBarsPageStep({
        ...BASE_INPUT,
        page: page([bar("2024-01-01T00:00:00.000000000Z")], null, false),
      }),
    ).toEqual({ kind: "complete" });
  });

  it("continues only when next_start moves strictly forward", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      page: page(
        [bar("2024-01-01T00:00:00.000000000Z")],
        "2024-01-01T00:01:00.000Z",
        true,
      ),
    });

    expect(step).toEqual({
      kind: "continue",
      nextStart: "2024-01-01T00:01:00.000Z",
    });
  });

  it("stops when next_start equals the request start (revision-heavy livelock)", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      page: page(
        [bar("2024-01-01T00:00:00.000000000Z")],
        "2024-01-01T00:00:00.000Z",
        true,
      ),
    });

    expect(step.kind).toBe("stop");
    if (step.kind === "stop") {
      expect(step.stop.reason).toBe("cursor_non_advance");
      expect(step.stop.message).toContain("Revision-heavy Time Cursor");
      expect(step.stop.message).toContain("部分结果");
    }
  });

  it("stops when next_start moves backwards", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      requestStart: "2024-01-01T00:05:00.000Z",
      page: page(
        [bar("2024-01-01T00:05:00.000000000Z")],
        "2024-01-01T00:04:00.000Z",
        true,
      ),
    });

    expect(step.kind).toBe("stop");
    if (step.kind === "stop") {
      expect(step.stop.reason).toBe("cursor_non_advance");
    }
  });

  it("stops when next_start is not strictly after every returned open_ts", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      requestStart: "2024-01-01T00:00:00.000Z",
      lastReturnedOpenTs: "2024-01-01T00:00:00.000000000Z",
      page: page(
        [
          bar("2024-01-01T00:00:00.000000000Z"),
          bar("2024-01-01T00:00:00.000000000Z"),
        ],
        "2024-01-01T00:00:00.000Z",
        true,
      ),
    });

    expect(step.kind).toBe("stop");
    if (step.kind === "stop") {
      expect(step.stop.reason).toBe("cursor_non_advance");
    }
  });

  it("stops at a revision-group boundary even when the cursor advanced past the request", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      requestStart: "2024-01-01T00:00:00.000Z",
      lastReturnedOpenTs: "2024-01-01T00:05:00.000000000Z",
      page: page(
        [bar("2024-01-01T00:05:00.000000000Z")],
        "2024-01-01T00:05:00.000Z",
        true,
      ),
    });

    expect(step.kind).toBe("stop");
    if (step.kind === "stop") {
      expect(step.stop.reason).toBe("revision_group_boundary");
    }
  });

  it("treats has_more with an empty page or missing cursor as a protocol error", () => {
    const empty = decideBarsPageStep({
      ...BASE_INPUT,
      page: page([], "2024-01-01T00:01:00.000Z", true),
    });
    const nullCursor = decideBarsPageStep({
      ...BASE_INPUT,
      page: page([bar("2024-01-01T00:00:00.000000000Z")], null, true),
    });
    const invalidCursor = decideBarsPageStep({
      ...BASE_INPUT,
      page: page(
        [bar("2024-01-01T00:00:00.000000000Z")],
        "not-a-time",
        true,
      ),
    });

    for (const step of [empty, nullCursor, invalidCursor]) {
      expect(step.kind).toBe("stop");
      if (step.kind === "stop") {
        expect(step.stop.reason).toBe("cursor_protocol_error");
      }
    }
  });

  it("stops at the page cap before following another cursor", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      pageCount: 3,
      maxPages: 3,
      page: page(
        [bar("2024-01-01T00:00:00.000000000Z")],
        "2024-01-01T00:01:00.000Z",
        true,
      ),
    });

    expect(step.kind).toBe("stop");
    if (step.kind === "stop") {
      expect(step.stop.reason).toBe("page_cap");
      expect(step.stop.message).toContain("缩小时间范围");
    }
  });

  it("treats missing page metadata as a protocol error", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      page: { bars: [bar("2024-01-01T00:00:00.000000000Z")] },
    });

    expect(step.kind).toBe("stop");
    if (step.kind === "stop") {
      expect(step.stop.reason).toBe("cursor_protocol_error");
    }
  });
});
