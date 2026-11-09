import type { Instrument } from "@/api/endpoints/instruments";
import type {
  CollectorHealth,
  CollectorStatus,
} from "@/api/endpoints/ops";

/**
 * The gateway has no `/venues` or `/feeds` endpoint. Venues are derived from
 * the real Instrument Reference Data; feed names come from the collector
 * `/status` document. Feed-to-venue association uses the tracked entry's
 * InstrumentId prefix only when that Instrument exists in the snapshot.
 * Nothing here hardcodes Binance/OKX/FIX.
 */

export interface VenueRow {
  venue: string;
  instrumentCount: number;
  activeCount: number;
  delistedCount: number;
  assetClasses: string[];
  instrumentTypes: string[];
  feeds: string[];
  trackedCount: number;
  staleCount: number;
  lastEventTs: string | null;
}

function sortedUnique(values: Iterable<string>): string[] {
  return [...new Set(values)].sort((left, right) => left.localeCompare(right));
}

function instrumentVenue(instrumentId: string): string | null {
  const separator = instrumentId.indexOf(":");
  return separator > 0 ? instrumentId.slice(0, separator) : null;
}

function maxTimestamp(
  left: string | null,
  right: string | null,
): string | null {
  if (right === null) {
    return left;
  }
  if (left === null) {
    return right;
  }
  return Date.parse(right) > Date.parse(left) ? right : left;
}

export function deriveVenueRows(
  instruments: Instrument[],
  health: CollectorHealth | null,
): VenueRow[] {
  const knownVenues = new Set(instruments.map((instrument) => instrument.venue));
  const knownInstrumentIds = new Set(
    instruments.map((instrument) => instrument.instrument_id),
  );
  const tracked = health?.tracked ?? [];

  const rows = new Map<string, VenueRow>();
  for (const instrument of instruments) {
    const existing = rows.get(instrument.venue);
    if (existing) {
      existing.instrumentCount += 1;
      if (instrument.delisted_ts === null) {
        existing.activeCount += 1;
      } else {
        existing.delistedCount += 1;
      }
      existing.assetClasses = sortedUnique([
        ...existing.assetClasses,
        instrument.asset_class,
      ]);
      existing.instrumentTypes = sortedUnique([
        ...existing.instrumentTypes,
        instrument.instrument_type,
      ]);
      continue;
    }
    rows.set(instrument.venue, {
      venue: instrument.venue,
      instrumentCount: 1,
      activeCount: instrument.delisted_ts === null ? 1 : 0,
      delistedCount: instrument.delisted_ts === null ? 0 : 1,
      assetClasses: [instrument.asset_class],
      instrumentTypes: [instrument.instrument_type],
      feeds: [],
      trackedCount: 0,
      staleCount: 0,
      lastEventTs: null,
    });
  }

  for (const entry of tracked) {
    const venue = instrumentVenue(entry.instrument_id);
    if (
      !venue ||
      !knownVenues.has(venue) ||
      !knownInstrumentIds.has(entry.instrument_id)
    ) {
      continue;
    }
    const row = rows.get(venue);
    if (!row) {
      continue;
    }
    row.feeds = sortedUnique([...row.feeds, entry.feed]);
    row.trackedCount += 1;
    if (entry.stale) {
      row.staleCount += 1;
    }
    row.lastEventTs = maxTimestamp(row.lastEventTs, entry.last_event_ts);
  }

  return [...rows.values()].sort((left, right) =>
    left.venue.localeCompare(right.venue),
  );
}

export function deriveFeedNames(
  collector: CollectorStatus | null,
): string[] {
  if (!collector) {
    return [];
  }
  return sortedUnique([
    ...(collector.feed ? [collector.feed] : []),
    ...collector.health.tracked.map((entry) => entry.feed),
  ]);
}
