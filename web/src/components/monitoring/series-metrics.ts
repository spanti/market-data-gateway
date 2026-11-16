import type { TrendSeries } from "@/api/monitoring/types";

/**
 * Metric selection for the monitoring KPI cards.
 *
 * A series carries a stable internal identity (`id`) that is independent of
 * its display name. Labels may change for readability without changing which
 * metric is read. Every adapter supplies an explicit identity; selection
 * never parses or falls back to the display name.
 */

/** The last finite value of one line, or null when the line has no sample. */
export function latestFiniteValue(series: Pick<TrendSeries, "points">): number | null {
  for (let index = series.points.length - 1; index >= 0; index -= 1) {
    const value = series.points[index]?.value;
    if (value !== null && value !== undefined && Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

/**
 * The maximum last finite value across every line sharing one metric
 * identity. Display names are never consulted when an identity exists.
 */
export function latestSeriesValue(
  series: TrendSeries[],
  identity: string,
): number | null {
  const values = series
    .filter((item) => item.id === identity)
    .map(latestFiniteValue)
    .filter((value): value is number => value !== null);
  return values.length > 0 ? Math.max(...values) : null;
}

/** QPS is additive across route labels, so lines are summed, never maxed. */
export function latestSeriesSum(series: TrendSeries[]): number | null {
  const values = series
    .map(latestFiniteValue)
    .filter((value): value is number => value !== null);
  if (values.length === 0) {
    return null;
  }
  return values.reduce((total, value) => total + value, 0);
}

/** Latest timestamp with a finite sample, used for the "last sample" evidence. */
export function latestSampleTime(
  series: Array<Pick<TrendSeries, "points">>,
): number | null {
  let latest: number | null = null;
  for (const item of series) {
    for (const point of item.points) {
      if (point.value === null || !Number.isFinite(point.value)) {
        continue;
      }
      if (latest === null || point.ts > latest) {
        latest = point.ts;
      }
    }
  }
  return latest;
}
