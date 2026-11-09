/**
 * Monitoring refresh and time-range defaults.
 *
 * Both polling intervals are configurable through Vite env variables while the
 * production defaults stay 5s (Service/Dataset `/status`) and 15s (Prometheus
 * trends), matching the operator-console spec.
 */

export const DEFAULT_STATUS_POLL_INTERVAL_MS = 5000;
export const DEFAULT_PROMETHEUS_POLL_INTERVAL_MS = 15000;

/** Console Operation status polling (accepted -> running -> terminal). */
export const OPERATION_POLL_INTERVAL_MS = 3000;
/** Active Export Job progress polling while a job is queued/running. */
export const ACTIVE_JOB_POLL_INTERVAL_MS = 3000;

/** Never poll faster than once per second, even if configured to do so. */
const MIN_POLL_INTERVAL_MS = 1000;

export function resolvePollIntervalMs(
  raw: string | undefined,
  fallback: number,
): number {
  if (raw === undefined) {
    return fallback;
  }
  const trimmed = raw.trim();
  if (!/^\d+$/.test(trimmed)) {
    return fallback;
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed < MIN_POLL_INTERVAL_MS) {
    return fallback;
  }
  return Math.floor(parsed);
}

export const STATUS_POLL_INTERVAL_MS = resolvePollIntervalMs(
  import.meta.env.VITE_STATUS_POLL_INTERVAL_MS,
  DEFAULT_STATUS_POLL_INTERVAL_MS,
);

export const PROMETHEUS_POLL_INTERVAL_MS = resolvePollIntervalMs(
  import.meta.env.VITE_PROMETHEUS_POLL_INTERVAL_MS,
  DEFAULT_PROMETHEUS_POLL_INTERVAL_MS,
);

export interface MonitoringRangeOption {
  id: string;
  label: string;
  durationMs: number;
  /** Query-range step in seconds. */
  stepSeconds: number;
  /** Rate window used by the QPS and latency queries. */
  qpsRateWindow: string;
  /** Rate window used inside the peak-QPS subquery. */
  peakRateWindow: string;
  /** Sampling step inside the peak-QPS subquery. */
  peakStepSeconds: number;
}

export const MONITORING_RANGES: readonly MonitoringRangeOption[] = [
  {
    id: "15m",
    label: "15 分钟",
    durationMs: 15 * 60 * 1000,
    stepSeconds: 15,
    qpsRateWindow: "5m",
    peakRateWindow: "1m",
    peakStepSeconds: 15,
  },
  {
    id: "1h",
    label: "1 小时",
    durationMs: 60 * 60 * 1000,
    stepSeconds: 30,
    qpsRateWindow: "5m",
    peakRateWindow: "1m",
    peakStepSeconds: 15,
  },
  {
    id: "6h",
    label: "6 小时",
    durationMs: 6 * 60 * 60 * 1000,
    stepSeconds: 60,
    qpsRateWindow: "5m",
    peakRateWindow: "1m",
    peakStepSeconds: 15,
  },
  {
    id: "24h",
    label: "24 小时",
    durationMs: 24 * 60 * 60 * 1000,
    stepSeconds: 300,
    qpsRateWindow: "5m",
    peakRateWindow: "1m",
    peakStepSeconds: 15,
  },
];

export const DEFAULT_MONITORING_RANGE_ID = "1h";

export function isMonitoringRangeId(value: string): boolean {
  return MONITORING_RANGES.some((range) => range.id === value);
}

export function getMonitoringRange(id: string | null | undefined): MonitoringRangeOption {
  return (
    MONITORING_RANGES.find((range) => range.id === id) ??
    MONITORING_RANGES.find((range) => range.id === DEFAULT_MONITORING_RANGE_ID) ??
    MONITORING_RANGES[0]!
  );
}

export function monitoringRangeFromParams(
  params: URLSearchParams,
): MonitoringRangeOption {
  return getMonitoringRange(params.get("range"));
}

/** Peak QPS is the maximum of sampled 1m-window rates over this exact range. */
export function peakRangeLabel(range: MonitoringRangeOption): string {
  return `最近 ${range.label}`;
}

export function peakQueryDescription(range: MonitoringRangeOption): string {
  return `${peakRangeLabel(range)} · ${range.peakRateWindow} rate · ${range.peakStepSeconds}s step`;
}
