import { describe, expect, it } from "vitest";
import type { TrendSeries } from "@/api/monitoring/types";
import {
  latestSampleTime,
  latestSeriesSum,
  latestSeriesValue,
} from "@/components/monitoring/series-metrics";

function series(
  id: string,
  name: string,
  points: Array<[number, number | null]>,
): TrendSeries {
  return {
    id,
    name,
    points: points.map(([ts, value]) => ({ ts, value })),
  };
}

const t0 = Date.parse("2026-11-09T11:00:00.000Z");
const t1 = Date.parse("2026-11-09T11:00:30.000Z");
const t2 = Date.parse("2026-11-09T11:01:00.000Z");

describe("series identity is separate from display name", () => {
  it("looks up a metric by stable identity regardless of its display label", () => {
    const p99 = series("p99", "HTTP P99 延迟", [
      [t0, 100],
      [t2, 500],
    ]);

    expect(latestSeriesValue([p99], "p99")).toBe(500);

    const renamed = { ...p99, name: "完全不同的显示名称" };
    expect(latestSeriesValue([renamed], "p99")).toBe(500);
  });

  it("never matches on the display label when the identity differs", () => {
    const p50 = series("p50", "P99", [[t2, 250]]);

    expect(latestSeriesValue([p50], "p99")).toBeNull();
  });

  it("ignores a series whose display name matches but whose identity differs", () => {
    const p95 = series("p95", "P99", [[t2, 250]]);

    expect(latestSeriesValue([p95], "P99")).toBeNull();
  });

  it("takes the maximum across lines that share one metric identity", () => {
    const routeA = series("p99", "P99 /api/v1/bars", [[t2, 180]]);
    const routeB = series("p99", "P99 /api/v1/instruments", [[t2, 240]]);

    expect(latestSeriesValue([routeA, routeB], "p99")).toBe(240);
  });
});

describe("metric value semantics stay unchanged", () => {
  it("sums the last finite value of every QPS line", () => {
    const routeA = series("qps", "QPS /api/v1/bars", [
      [t0, 8],
      [t1, 10],
    ]);
    const routeB = series("qps", "QPS /ws", [
      [t1, 2],
      [t2, null],
    ]);

    expect(latestSeriesSum([routeA, routeB])).toBe(12);
  });

  it("ignores non-finite samples and returns null when nothing is finite", () => {
    const line = series("qps", "QPS", [
      [t0, 8],
      [t1, Number.NaN],
      [t2, null],
    ]);

    expect(latestSeriesSum([line])).toBe(8);
    expect(latestSeriesSum([])).toBeNull();
    expect(
      latestSeriesSum([series("qps", "QPS", [[t2, Number.NaN]])]),
    ).toBeNull();
  });

  it("reports the latest finite sample timestamp across series", () => {
    const routeA = series("qps", "QPS /a", [[t0, 1]]);
    const routeB = series("qps", "QPS /b", [
      [t1, 1],
      [t2, Number.NaN],
    ]);

    expect(latestSampleTime([routeA, routeB])).toBe(t1);
    expect(latestSampleTime([])).toBeNull();
  });
});
