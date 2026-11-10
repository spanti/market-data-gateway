import { describe, expect, it } from "vitest";
import { decideBarsPageStep } from "@/lib/bar-pagination";
import type { BarsV2Response } from "@/api/endpoints/bars-v2";
import type { BarMessageEnvelope } from "@/api/endpoints/bars";

const BASE_INPUT = {
  requestCursor: null,
  seenCursors: [] as string[],
  pageCount: 1,
  maxPages: 10,
};

function page(
  bars: BarsV2Response["bars"],
  nextCursor: string | null,
  hasMore: boolean,
): BarsV2Response {
  return { bars, page: { has_more: hasMore, next_cursor: nextCursor } };
}

function bar(revision = 0, openTs = "2024-01-01T00:00:00.000000000Z") {
  const envelope: BarMessageEnvelope = {
    schema_version: 1,
    data_type: "bar",
    instrument_id: "binance:BTC/USDT",
    event_ts: "2024-01-01T00:01:00.000000000Z",
    ingest_ts: "2026-11-09T04:24:42.946489000Z",
    feed: "fake",
    seq: null,
    payload: {
      interval: "1m",
      open_ts: openTs,
      close_ts: "2024-01-01T00:01:00.000000000Z",
      aggregation_source: "external",
      open: "1.0",
      high: "2.0",
      low: "0.5",
      close: "1.5",
      volume: "10.0",
      trade_count: 1,
      revision,
    },
  };
  return envelope;
}

describe("revision-safe cursor guard", () => {
  it("completes when has_more is false and the cursor is null", () => {
    expect(
      decideBarsPageStep({
        ...BASE_INPUT,
        page: page([bar()], null, false),
      }),
    ).toEqual({ kind: "complete" });
  });

  it("continues with the opaque next_cursor when has_more is true", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      page: page([bar()], "opaque-cursor-1", true),
    });

    expect(step).toEqual({
      kind: "continue",
      nextCursor: "opaque-cursor-1",
    });
  });

  it("stops when the cursor is unchanged", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      requestCursor: "opaque-cursor-1",
      seenCursors: ["opaque-cursor-1"],
      pageCount: 2,
      page: page([bar()], "opaque-cursor-1", true),
    });

    expect(step.kind).toBe("stop");
    if (step.kind === "stop") {
      expect(step.stop.reason).toBe("cursor_non_advance");
      expect(step.stop.message).toContain("next_cursor 未前进或重复");
      expect(step.stop.message).toContain("部分结果");
    }
  });

  it("stops when the cursor repeats an earlier cursor (cycle)", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      requestCursor: "opaque-cursor-2",
      seenCursors: ["opaque-cursor-1", "opaque-cursor-2"],
      pageCount: 3,
      page: page([bar()], "opaque-cursor-1", true),
    });

    expect(step.kind).toBe("stop");
    if (step.kind === "stop") {
      expect(step.stop.reason).toBe("cursor_non_advance");
    }
  });

  it("treats has_more with a missing or empty cursor as a protocol error", () => {
    for (const cursor of [null, ""]) {
      const step = decideBarsPageStep({
        ...BASE_INPUT,
        page: page([bar()], cursor, true),
      });
      expect(step.kind).toBe("stop");
      if (step.kind === "stop") {
        expect(step.stop.reason).toBe("cursor_protocol_error");
      }
    }
  });

  it("treats has_more with an empty page as a protocol error", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      page: page([], "opaque-cursor-1", true),
    });

    expect(step.kind).toBe("stop");
    if (step.kind === "stop") {
      expect(step.stop.reason).toBe("cursor_protocol_error");
    }
  });

  it("treats a non-null cursor on the last page as a protocol error", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      page: page([bar()], "stale-cursor", false),
    });

    expect(step.kind).toBe("stop");
    if (step.kind === "stop") {
      expect(step.stop.reason).toBe("cursor_protocol_error");
    }
  });

  it("stops at the page cap before following another cursor", () => {
    const step = decideBarsPageStep({
      ...BASE_INPUT,
      pageCount: 3,
      maxPages: 3,
      page: page([bar()], "opaque-cursor-2", true),
    });

    expect(step.kind).toBe("stop");
    if (step.kind === "stop") {
      expect(step.stop.reason).toBe("page_cap");
      expect(step.stop.message).toContain("缩小时间范围");
    }
  });

  it("treats missing or malformed page metadata as a protocol error", () => {
    const missing = decideBarsPageStep({
      ...BASE_INPUT,
      page: { bars: [bar()] } as unknown as BarsV2Response,
    });
    const malformed = decideBarsPageStep({
      ...BASE_INPUT,
      page: {
        bars: [bar()],
        page: { has_more: "yes", next_cursor: null },
      } as unknown as BarsV2Response,
    });

    for (const step of [missing, malformed]) {
      expect(step.kind).toBe("stop");
      if (step.kind === "stop") {
        expect(step.stop.reason).toBe("cursor_protocol_error");
      }
    }
  });
});
