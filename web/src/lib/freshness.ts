import type {
  CollectorHealth,
  DatasetStatus,
  FeedFreshnessEntry,
} from "@/api/endpoints/ops";

/**
 * Feed Freshness is server-computed: `age_s`, `threshold_s` and `stale` come
 * from the collector `/status` snapshot. The countdown is threshold minus age;
 * the server's `stale` flag is authoritative and must win over local arithmetic.
 * Dataset Freshness is a separate concept and never uses a stale flag.
 */

export type FreshnessTone = "good" | "critical" | "unknown";

export type FreshnessStatusLabel = "正常" | "失活" | "未知";

export interface FeedFreshnessView {
  feed: string;
  instrumentId: string;
  interval: string;
  lastEventTs: string | null;
  ageS: number | null;
  thresholdS: number | null;
  remainingS: number | null;
  stale: boolean;
  tone: FreshnessTone;
  statusLabel: FreshnessStatusLabel;
}

export interface FeedFreshnessSummary {
  tracked: number;
  stale: number;
  unknown: number;
  tone: FreshnessTone;
}

export interface DatasetFreshnessRow {
  interval: string;
  lastFlushedTs: string | null;
  ageS: number | null;
  activeFileCount: number;
  medianFileSizeBytes: number | null;
}

function isFiniteNumber(value: number | null): value is number {
  return value !== null && Number.isFinite(value);
}

export function toFeedFreshnessView(
  entry: FeedFreshnessEntry,
): FeedFreshnessView {
  const remainingS =
    isFiniteNumber(entry.age_s) && isFiniteNumber(entry.threshold_s)
      ? entry.threshold_s - entry.age_s
      : null;

  let tone: FreshnessTone;
  let statusLabel: FreshnessStatusLabel;
  if (entry.stale) {
    tone = "critical";
    statusLabel = "失活";
  } else if (isFiniteNumber(entry.age_s)) {
    tone = "good";
    statusLabel = "正常";
  } else {
    // A non-stale entry without a measurable age is not evidence of health.
    tone = "unknown";
    statusLabel = "未知";
  }

  return {
    feed: entry.feed,
    instrumentId: entry.instrument_id,
    interval: entry.interval,
    lastEventTs: entry.last_event_ts,
    ageS: entry.age_s,
    thresholdS: entry.threshold_s,
    remainingS,
    stale: entry.stale,
    tone,
    statusLabel,
  };
}

export function summarizeFeedFreshness(
  health: CollectorHealth,
): FeedFreshnessSummary {
  const views = health.tracked.map(toFeedFreshnessView);
  return {
    tracked: views.length,
    stale: views.filter((view) => view.stale).length,
    unknown: views.filter((view) => view.tone === "unknown").length,
    tone:
      views.length === 0
        ? "unknown"
        : views.some((view) => view.tone === "critical")
          ? "critical"
          : views.some((view) => view.tone === "unknown")
            ? "unknown"
            : "good",
  };
}

export function toDatasetFreshnessRows(
  status: DatasetStatus,
  nowMs: number,
): DatasetFreshnessRow[] {
  return Object.entries(status.intervals)
    .map(([interval, entry]) => {
      const parsed =
        entry.last_flushed_ts === null
          ? Number.NaN
          : Date.parse(entry.last_flushed_ts);
      return {
        interval,
        lastFlushedTs: entry.last_flushed_ts,
        ageS: Number.isFinite(parsed) ? (nowMs - parsed) / 1000 : null,
        activeFileCount: entry.active_file_count,
        medianFileSizeBytes: entry.median_file_size_bytes,
      };
    })
    .sort((left, right) => left.interval.localeCompare(right.interval));
}
