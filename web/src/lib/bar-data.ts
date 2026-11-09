import type { BarMessageEnvelope } from "@/api/endpoints/bars";
import { parseIsoInstant } from "@/lib/time-range";

/**
 * Pure Bar identity, revision-selection and range helpers.
 *
 * Decimal and timestamp strings are preserved exactly: this module only
 * compares timestamps and revisions. It never converts decimals to Number.
 */

export type BarSource = "rest" | "ws";

export interface BarRow {
  bar: BarMessageEnvelope;
  source: BarSource;
}

export interface BarQueryIdentity {
  instrumentId: string;
  interval: string;
  start: Date;
  end: Date;
}

const KEY_SEPARATOR = "\u0000";

export function barGroupKey(bar: BarMessageEnvelope): string {
  return [
    bar.instrument_id,
    bar.payload.interval,
    bar.payload.open_ts,
  ].join(KEY_SEPARATOR);
}

function openTsMillis(bar: BarMessageEnvelope): number {
  return parseIsoInstant(bar.payload.open_ts)?.getTime() ?? Number.NaN;
}

export function compareBarsByOpenTs(
  a: BarMessageEnvelope,
  b: BarMessageEnvelope,
): number {
  const aMs = openTsMillis(a);
  const bMs = openTsMillis(b);
  if (Number.isNaN(aMs) || Number.isNaN(bMs)) {
    // Invalid timestamps never come from the gateway; fall back to raw text
    // ordering instead of throwing away the record.
    return a.payload.open_ts.localeCompare(b.payload.open_ts);
  }
  return aMs - bMs;
}

/**
 * Chart view: one Bar per `(instrument_id, interval, open_ts)`.
 *
 * The highest `revision` wins. On an equal revision the latest record in the
 * received server response order wins. The Data Contract says REST
 * equal-revision tie order is not contractual; "latest in response order" is
 * the observable assumption recorded here and in the UI caption. Raw revision
 * tables and CSV must keep every row instead.
 */
export function deriveMaxRevisionBars(
  bars: readonly BarMessageEnvelope[],
): BarMessageEnvelope[] {
  const byIdentity = new Map<string, BarMessageEnvelope>();
  for (const bar of bars) {
    const key = barGroupKey(bar);
    const existing = byIdentity.get(key);
    if (
      existing === undefined ||
      bar.payload.revision >= existing.payload.revision
    ) {
      byIdentity.set(key, bar);
    }
  }
  return [...byIdentity.values()].sort(compareBarsByOpenTs);
}

export function maxOpenTs(
  bars: readonly BarMessageEnvelope[],
): string | null {
  let max: BarMessageEnvelope | null = null;
  for (const bar of bars) {
    if (max === null || compareBarsByOpenTs(bar, max) > 0) {
      max = bar;
    }
  }
  return max?.payload.open_ts ?? null;
}

export function isBarInRange(
  bar: BarMessageEnvelope,
  start: Date,
  end: Date,
): boolean {
  const instant = parseIsoInstant(bar.payload.open_ts);
  if (instant === null) {
    return false;
  }
  const value = instant.getTime();
  return value >= start.getTime() && value < end.getTime();
}

export function matchesBarQuery(
  bar: BarMessageEnvelope,
  identity: BarQueryIdentity,
): boolean {
  return (
    bar.instrument_id === identity.instrumentId &&
    bar.payload.interval === identity.interval &&
    isBarInRange(bar, identity.start, identity.end)
  );
}
