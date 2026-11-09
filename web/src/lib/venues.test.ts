import { describe, expect, it } from "vitest";
import type { Instrument } from "@/api/endpoints/instruments";
import type { CollectorStatus } from "@/api/endpoints/ops";
import { deriveFeedNames, deriveVenueRows } from "@/lib/venues";

function instrument(
  instrumentId: string,
  venue: string,
  extra: Partial<Instrument> = {},
): Instrument {
  return {
    instrument_id: instrumentId,
    venue,
    symbol: instrumentId.split(":")[1] ?? instrumentId,
    asset_class: "crypto",
    instrument_type: "spot",
    base: "AAA",
    quote: "USD",
    tick_size: "0.01",
    lot_size: "0.001",
    multiplier: "1.0",
    timezone: "UTC",
    listing_ts: "2024-01-01T00:00:00.000000000Z",
    delisted_ts: null,
    ...extra,
  };
}

const collector = {
  service: "collector",
  role: "collector",
  ready: true,
  uptime_s: 10,
  feed: "feed-one",
  health: {
    tracked: [
      {
        feed: "feed-one",
        instrument_id: "venue-a:AAA/USD",
        interval: "1m" as const,
        last_event_ts: "2026-11-09T12:00:00.000000000Z",
        age_s: 20,
        threshold_s: 120,
        stale: false,
      },
      {
        feed: "feed-two",
        instrument_id: "venue-a:BBB/USD",
        interval: "1m" as const,
        last_event_ts: "2026-11-09T12:01:00.000000000Z",
        age_s: 200,
        threshold_s: 120,
        stale: true,
      },
      {
        feed: "feed-three",
        instrument_id: "not-in-reference:CCC/USD",
        interval: "1m" as const,
        last_event_ts: "2026-11-09T12:02:00.000000000Z",
        age_s: 1,
        threshold_s: 120,
        stale: false,
      },
    ],
    stale_count: 1,
    stale: [],
    gap_unrecoverable: [],
    gap_unrecoverable_count: 0,
  },
} as unknown as CollectorStatus;

describe("venue derivation", () => {
  it("derives venues from the Instrument Reference Data, not a hardcoded list", () => {
    const rows = deriveVenueRows(
      [
        instrument("venue-b:CCC/USD", "venue-b", {
          asset_class: "futures",
          instrument_type: "perp",
          delisted_ts: "2026-01-01T00:00:00.000000000Z",
        }),
        instrument("venue-a:AAA/USD", "venue-a"),
        instrument("venue-a:BBB/USD", "venue-a"),
      ],
      collector.health,
    );

    expect(rows.map((row) => row.venue)).toEqual(["venue-a", "venue-b"]);
    expect(rows[0]).toMatchObject({
      instrumentCount: 2,
      activeCount: 2,
      delistedCount: 0,
      assetClasses: ["crypto"],
      instrumentTypes: ["spot"],
      feeds: ["feed-one", "feed-two"],
      trackedCount: 2,
      staleCount: 1,
      lastEventTs: "2026-11-09T12:01:00.000000000Z",
    });
    expect(rows[1]).toMatchObject({
      instrumentCount: 1,
      activeCount: 0,
      delistedCount: 1,
      assetClasses: ["futures"],
      instrumentTypes: ["perp"],
      feeds: [],
      trackedCount: 0,
      staleCount: 0,
      lastEventTs: null,
    });
  });

  it("does not invent venues for feed instruments absent from the snapshot", () => {
    const rows = deriveVenueRows(
      [instrument("venue-a:AAA/USD", "venue-a")],
      collector.health,
    );

    expect(rows.map((row) => row.venue)).toEqual(["venue-a"]);
    expect(rows[0]?.feeds).toEqual(["feed-one"]);
    expect(rows[0]?.trackedCount).toBe(1);
  });

  it("returns no venue rows when the Instrument snapshot is empty", () => {
    expect(deriveVenueRows([], collector.health)).toEqual([]);
  });

  it("keeps feed names derived from real collector data", () => {
    expect(deriveFeedNames(collector)).toEqual([
      "feed-one",
      "feed-three",
      "feed-two",
    ]);
    expect(deriveFeedNames(null)).toEqual([]);
  });
});
