import {
  queryPrometheusInstant,
  queryPrometheusRange,
} from "@/api/prometheus";
import type { PrometheusQueryResult } from "@/api/http/prometheus";
import {
  buildLatencyQuantileQuery,
  buildPeakQpsQuery,
  buildQpsQuery,
  LATENCY_QUANTILES,
} from "@/api/monitoring/promql";
import type { TrendSeries, TrendWindow } from "@/api/monitoring/types";

function seconds(ms: number): number {
  return Math.floor(ms / 1000);
}

function seriesName(
  labels: Record<string, string>,
  fallback: string,
  index: number,
  count: number,
): string {
  if (labels.path) {
    return `${fallback} · ${labels.path}`;
  }
  return count > 1 ? `${fallback} ${index + 1}` : fallback;
}

/**
 * Prometheus samples are seconds; the chart uses milliseconds. Non-finite
 * values (including the literal `NaN`) become null so the line has a gap.
 * Nothing is zero-filled.
 */
export function prometheusResultToTrendSeries(
  result: PrometheusQueryResult,
  fallbackName: string,
): TrendSeries[] {
  return result.series.map((series, index) => ({
    name: seriesName(series.labels, fallbackName, index, result.series.length),
    labels: series.labels,
    points: series.points.map((point) => ({
      ts: Math.round(point.ts * 1000),
      value: Number.isFinite(point.value) ? point.value : null,
    })),
  }));
}

export function maxFiniteSample(result: PrometheusQueryResult): number | null {
  let peak: number | null = null;
  for (const series of result.series) {
    for (const point of series.points) {
      if (!Number.isFinite(point.value)) {
        continue;
      }
      if (peak === null || point.value > peak) {
        peak = point.value;
      }
    }
  }
  return peak;
}

export async function fetchQpsTrend(
  window: TrendWindow,
  rateWindow: string,
  signal?: AbortSignal,
): Promise<TrendSeries[]> {
  const result = await queryPrometheusRange(
    {
      query: buildQpsQuery(rateWindow),
      start: seconds(window.startMs),
      end: seconds(window.endMs),
      step: `${window.stepSeconds}s`,
    },
    signal,
  );
  return prometheusResultToTrendSeries(result, "QPS");
}

export async function fetchPeakQps(
  {
    endMs,
    lookbackMs,
    rateWindow,
    stepSeconds,
  }: {
    endMs: number;
    lookbackMs: number;
    rateWindow: string;
    stepSeconds: number;
  },
  signal?: AbortSignal,
): Promise<number | null> {
  const result = await queryPrometheusInstant(
    {
      query: buildPeakQpsQuery({ lookbackMs, rateWindow, stepSeconds }),
      time: seconds(endMs),
    },
    signal,
  );
  return maxFiniteSample(result);
}

export async function fetchLatencyTrends(
  window: TrendWindow,
  rateWindow: string,
  signal?: AbortSignal,
): Promise<TrendSeries[]> {
  const results = await Promise.all(
    LATENCY_QUANTILES.map((item) =>
      queryPrometheusRange(
        {
          query: buildLatencyQuantileQuery(item.quantile, rateWindow),
          start: seconds(window.startMs),
          end: seconds(window.endMs),
          step: `${window.stepSeconds}s`,
        },
        signal,
      ),
    ),
  );
  return results.flatMap((result, index) =>
    prometheusResultToTrendSeries(
      result,
      LATENCY_QUANTILES[index]?.label ?? `P${index + 1}`,
    ).map((series) => ({
      ...series,
      // Histogram observations are seconds; the dashboard displays ms.
      points: series.points.map((point) => ({
        ...point,
        value:
          point.value === null || !Number.isFinite(point.value)
            ? null
            : point.value * 1000,
      })),
    })),
  );
}
