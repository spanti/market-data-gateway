import { describe, expect, it } from "vitest";
import {
  barGroupKey,
  deriveMaxRevisionBars,
  isBarInRange,
  matchesBarQuery,
  maxOpenTs,
  type BarQueryIdentity,
} from "@/lib/bar-data";
import type { BarMessageEnvelope } from "@/api/endpoints/bars";

function envelope(
  overrides: {
    instrument?: string;
    interval?: "1m" | "5m" | "1h";
    openTs?: string;
    revision?: number;
    close?: string;
    ingestTs?: string;
    feed?: string;
  } = {},
): BarMessageEnvelope {
  return {
    schema_version: 1,
    data_type: "bar",
    instrument_id: overrides.instrument ?? "binance:BTC/USDT",
    event_ts: "2024-01-01T00:01:00.000000000Z",
    ingest_ts: overrides.ingestTs ?? "2026-11-09T04:24:42.946489000Z",
    feed: overrides.feed ?? "fake",
    seq: null,
    payload: {
      interval: overrides.interval ?? "1m",
      open_ts: overrides.openTs ?? "2024-01-01T00:00:00.000000000Z",
      close_ts: "2024-01-01T00:01:00.000000000Z",
      aggregation_source: "external",
      open: "65000.1",
      high: "65010.0",
      low: "64990.0",
      close: overrides.close ?? "65005.5",
      volume: "12.34",
      trade_count: 128,
      revision: overrides.revision ?? 0,
    },
  };
}

describe("Bar identity", () => {
  it("keys on instrument_id, interval and open_ts", () => {
    expect(barGroupKey(envelope())).toBe(
      "binance:BTC/USDT\u00001m\u00002024-01-01T00:00:00.000000000Z",
    );
    expect(
      barGroupKey(envelope({ revision: 9, close: "1.0" })),
    ).toBe(barGroupKey(envelope()));
  });
});

describe("max-revision chart view", () => {
  it("keeps only the highest revision per identity", () => {
    const bars = [
      envelope({ revision: 0, close: "1.0" }),
      envelope({ revision: 2, close: "3.0" }),
      envelope({ revision: 1, close: "2.0" }),
    ];

    const result = deriveMaxRevisionBars(bars);

    expect(result).toHaveLength(1);
    expect(result[0]?.payload.revision).toBe(2);
    expect(result[0]?.payload.close).toBe("3.0");
  });

  it("breaks equal revisions by the latest ingest_ts even when it appears earlier in the response", () => {
    // Data Contract §9: Replay keeps the highest revision and breaks equal
    // revision ties by the latest ingest_ts. The chart follows the same rule,
    // so server response order must not override a later ingest.
    const laterIngestButFirstInResponse = envelope({
      revision: 1,
      close: "11.0",
      ingestTs: "2026-11-09T05:00:00.000000000Z",
    });
    const earlierIngestButLastInResponse = envelope({
      revision: 1,
      close: "10.0",
      ingestTs: "2026-11-09T04:00:00.000000000Z",
    });

    const result = deriveMaxRevisionBars([
      laterIngestButFirstInResponse,
      earlierIngestButLastInResponse,
    ]);

    expect(result).toHaveLength(1);
    expect(result[0]).toBe(laterIngestButFirstInResponse);
  });

  it("keeps the later response-order record only when revisions and ingest_ts are equal", () => {
    const first = envelope({
      revision: 1,
      close: "10.0",
      ingestTs: "2026-11-09T05:00:00.000000000Z",
    });
    const second = envelope({
      revision: 1,
      close: "11.0",
      ingestTs: "2026-11-09T05:00:00.000000000Z",
    });

    const result = deriveMaxRevisionBars([first, second]);

    expect(result).toHaveLength(1);
    expect(result[0]).toBe(second);
  });

  it("does not mutate or re-stringify the winning envelope", () => {
    const winner = envelope({ revision: 3, close: "0.000000000000000001" });
    const result = deriveMaxRevisionBars([envelope({ revision: 1 }), winner]);

    expect(result[0]).toBe(winner);
    expect(result[0]?.payload.close).toBe("0.000000000000000001");
  });

  it("keeps separate identities and sorts by open_ts ascending", () => {
    const later = envelope({
      openTs: "2024-01-01T00:02:00.000000000Z",
      revision: 0,
    });
    const earlier = envelope({
      openTs: "2024-01-01T00:00:00.000000000Z",
      revision: 5,
    });
    const otherInterval = envelope({
      interval: "5m",
      openTs: "2024-01-01T00:00:00.000000000Z",
    });
    const otherInstrument = envelope({
      instrument: "binance:ETH/USDT",
      openTs: "2024-01-01T00:01:00.000000000Z",
    });

    const result = deriveMaxRevisionBars([
      later,
      otherInstrument,
      otherInterval,
      earlier,
    ]);

    expect(result).toHaveLength(4);
    expect(result.map((bar) => bar.payload.open_ts)).toEqual([
      "2024-01-01T00:00:00.000000000Z",
      "2024-01-01T00:00:00.000000000Z",
      "2024-01-01T00:01:00.000000000Z",
      "2024-01-01T00:02:00.000000000Z",
    ]);
  });
});

describe("range and identity filters", () => {
  const identity: BarQueryIdentity = {
    instrumentId: "binance:BTC/USDT",
    interval: "1m",
    start: new Date("2024-01-01T00:00:00.000Z"),
    end: new Date("2024-01-01T00:02:00.000Z"),
  };

  it("treats start as inclusive and end as exclusive", () => {
    expect(
      isBarInRange(envelope({ openTs: "2024-01-01T00:00:00.000000000Z" }), identity.start, identity.end),
    ).toBe(true);
    expect(
      isBarInRange(envelope({ openTs: "2024-01-01T00:01:59.999999999Z" }), identity.start, identity.end),
    ).toBe(true);
    expect(
      isBarInRange(envelope({ openTs: "2024-01-01T00:02:00.000000000Z" }), identity.start, identity.end),
    ).toBe(false);
    expect(
      isBarInRange(envelope({ openTs: "2023-12-31T23:59:59.999999999Z" }), identity.start, identity.end),
    ).toBe(false);
  });

  it("requires instrument, interval and range to match a live event", () => {
    expect(matchesBarQuery(envelope(), identity)).toBe(true);
    expect(
      matchesBarQuery(envelope({ instrument: "binance:ETH/USDT" }), identity),
    ).toBe(false);
    expect(matchesBarQuery(envelope({ interval: "5m" }), identity)).toBe(false);
    expect(
      matchesBarQuery(
        envelope({ openTs: "2024-01-01T00:02:00.000000000Z" }),
        identity,
      ),
    ).toBe(false);
  });

  it("reports the maximum returned open_ts", () => {
    expect(maxOpenTs([])).toBeNull();
    expect(
      maxOpenTs([
        envelope({ openTs: "2024-01-01T00:00:00.000000000Z" }),
        envelope({ openTs: "2024-01-01T00:05:00.000000000Z" }),
        envelope({ openTs: "2024-01-01T00:03:00.000000000Z" }),
      ]),
    ).toBe("2024-01-01T00:05:00.000000000Z");
  });
});
