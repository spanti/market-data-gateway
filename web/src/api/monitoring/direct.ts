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
import { trendSeriesName } from "@/api/monitoring/series-display";
import type { TrendSeries, TrendWindow } from "@/api/monitoring/types";

function seconds(ms: number): number {
  return Math.floor(ms / 1000);
}

/**
 * Prometheus samples are seconds; the chart uses milliseconds. Non-finite
 * values (including the literal `NaN`) become null so the line has a gap.
 * Nothing is zero-filled.
 *
 * `identity` is the stable metric identity used by KPI selection. It is
 * required: display labels may change without changing which metric is read.
 */
export function prometheusResultToTrendSeries(
  result: PrometheusQueryResult,
  fallbackName: string,
  identity: string,
): TrendSeries[] {
  return result.series.map((series, index) => ({
    id: identity,
    name: trendSeriesName(series.labels, fallbackName, index, result.series.length),
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
  return prometheusResultToTrendSeries(result, "QPS", "qps");
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
  return results.flatMap((result, index) => {
    const quantile = LATENCY_QUANTILES[index];
    return prometheusResultToTrendSeries(
      result,
      quantile?.label ?? `P${index + 1}`,
      quantile?.id ?? `p${index + 1}`,
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
    }));
  });
}
