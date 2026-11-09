/**
 * Fixed PromQL templates for the Current Contract intranet-development view.
 * These are the only queries the console may send directly to Prometheus;
 * there is no arbitrary PromQL editor. Admin API mode never calls these.
 */

export const LATENCY_QUANTILES = [
  { id: "p50", quantile: 0.5, label: "P50" },
  { id: "p95", quantile: 0.95, label: "P95" },
  { id: "p99", quantile: 0.99, label: "P99" },
] as const;

export type LatencyQuantileId = (typeof LATENCY_QUANTILES)[number]["id"];

export function formatPrometheusDuration(durationMs: number): string {
  const seconds = Math.round(durationMs / 1000);
  if (seconds % 3600 === 0) {
    return `${seconds / 3600}h`;
  }
  if (seconds % 60 === 0) {
    return `${seconds / 60}m`;
  }
  return `${seconds}s`;
}

/**
 * QPS is the windowed rate of completed HTTP requests. The histogram records
 * requests at completion, so in-flight downloads are not counted until they
 * finish.
 */
export function buildQpsQuery(rateWindow: string): string {
  return `sum(rate(mdgw_api_http_requests_total[${rateWindow}]))`;
}

/**
 * Peak QPS is not a stored metric. It is the maximum of sampled 1m-window
 * rates over the selected range. The range and step must be labelled in the UI
 * because this is not a true instantaneous peak.
 */
export function buildPeakQpsQuery({
  lookbackMs,
  rateWindow,
  stepSeconds,
}: {
  lookbackMs: number;
  rateWindow: string;
  stepSeconds: number;
}): string {
  return `max_over_time((sum(rate(mdgw_api_http_requests_total[${rateWindow}])))[${formatPrometheusDuration(
    lookbackMs,
  )}:${stepSeconds}s])`;
}

/**
 * P50/P95/P99 come from the finite 5ms-5s histogram buckets and include the
 * complete response stream time, not just time to first byte.
 */
export function buildLatencyQuantileQuery(
  quantile: number,
  rateWindow: string,
): string {
  return `histogram_quantile(${quantile.toFixed(
    2,
  )}, sum by (le) (rate(mdgw_api_http_request_duration_seconds_bucket[${rateWindow}])))`;
}
