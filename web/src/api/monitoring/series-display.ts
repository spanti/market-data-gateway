/**
 * Display names for trend lines. They are UI-generated copy and therefore
 * never contain `·` or `：`. The stable metric identity is carried separately
 * on `TrendSeries.id` and must not be parsed out of this label.
 */
export function trendSeriesName(
  labels: Record<string, string>,
  fallback: string,
  index: number,
  count: number,
): string {
  if (labels.path) {
    return `${fallback} ${labels.path}`;
  }
  return count > 1 ? `${fallback} ${index + 1}` : fallback;
}
