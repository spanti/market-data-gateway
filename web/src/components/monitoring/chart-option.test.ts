import { describe, expect, it } from "vitest";
import {
  alignTrendPoints,
  buildTrendChartOption,
  LIGHT_CHART_PALETTE,
  readChartPalette,
} from "@/components/monitoring/chart-option";
import type { TrendSeries } from "@/api/monitoring/types";

function series(name: string, points: Array<[number, number | null]>): TrendSeries {
  return {
    name,
    points: points.map(([ts, value]) => ({ ts, value })),
  };
}

const t0 = Date.parse("2026-11-09T11:00:00.000Z");
const t1 = Date.parse("2026-11-09T11:00:30.000Z");
const t2 = Date.parse("2026-11-09T11:01:00.000Z");

describe("trend alignment", () => {
  it("aligns series on the union of timestamps and keeps gaps as null", () => {
    const aligned = alignTrendPoints([
      series("P50", [
        [t0, 0.01],
        [t2, 0.02],
      ]),
      series("P95", [
        [t1, 0.25],
        [t2, null],
      ]),
    ]);

    expect(aligned.timestamps).toEqual([t0, t1, t2]);
    expect(aligned.rows).toEqual([
      { ts: t0, values: [0.01, null] },
      { ts: t1, values: [null, 0.25] },
      { ts: t2, values: [0.02, null] },
    ]);
  });

  it("does not duplicate a timestamp within one series", () => {
    const aligned = alignTrendPoints([
      series("QPS", [
        [t0, 1],
        [t0, 2],
      ]),
    ]);

    expect(aligned.timestamps).toEqual([t0]);
    expect(aligned.rows[0]?.values).toEqual([2]);
  });
});

describe("trend chart option", () => {
  const seriesInput = [
    {
      name: "P50",
      color: LIGHT_CHART_PALETTE.series[0]!,
      points: [
        { ts: t0, value: 0.01 },
        { ts: t2, value: 0.02 },
      ],
    },
    {
      name: "P95",
      color: LIGHT_CHART_PALETTE.series[1]!,
      points: [
        { ts: t1, value: 0.25 },
        { ts: t2, value: null },
      ],
    },
  ];

  it("uses one value axis, 2px lines and no zero-forcing", () => {
    const option = buildTrendChartOption({
      title: "HTTP latency",
      description: "P50/P95/P99",
      unit: "ms",
      series: seriesInput,
      palette: LIGHT_CHART_PALETTE,
    });

    expect(Array.isArray(option.yAxis)).toBe(false);
    expect(option.yAxis).toMatchObject({ type: "value", scale: true });
    const chartSeries = option.series as Array<Record<string, unknown>>;
    expect(chartSeries).toHaveLength(2);
    expect(chartSeries[0]).toMatchObject({
      name: "P50",
      type: "line",
      connectNulls: false,
      showSymbol: false,
    });
    expect(chartSeries[0]?.lineStyle).toMatchObject({ width: 2 });
    expect(chartSeries[0]?.data).toEqual([0.01, null, 0.02]);
    expect(chartSeries[1]?.data).toEqual([null, 0.25, null]);
  });

  it("fills only single-series charts with a 10% area", () => {
    const single = buildTrendChartOption({
      title: "QPS",
      description: "",
      unit: "req/s",
      series: [seriesInput[0]!],
      palette: LIGHT_CHART_PALETTE,
    });
    const multi = buildTrendChartOption({
      title: "Latency",
      description: "",
      unit: "ms",
      series: seriesInput,
      palette: LIGHT_CHART_PALETTE,
    });

    expect((single.series as Array<Record<string, unknown>>)[0]?.areaStyle).toEqual(
      { opacity: 0.1 },
    );
    expect(
      (multi.series as Array<Record<string, unknown>>).map(
        (item) => item.areaStyle,
      ),
    ).toEqual([undefined, undefined]);
  });

  it("uses an axis-triggered tooltip, no animation and an ARIA description", () => {
    const option = buildTrendChartOption({
      title: "QPS",
      description: "窗口速率",
      unit: "req/s",
      series: [seriesInput[0]!],
      palette: LIGHT_CHART_PALETTE,
    });

    expect(option.tooltip).toMatchObject({ trigger: "axis" });
    expect(option.animation).toBe(false);
    expect(option.aria).toMatchObject({ enabled: true });
    expect(option.grid).toMatchObject({ left: 52, right: 24, top: 20, bottom: 38 });
  });

  it("reads the chart palette from semantic CSS variables with a safe fallback", () => {
    const values: Record<string, string> = {
      "--card": "#111111",
      "--foreground": "#eeeeee",
      "--muted-foreground": "#aaaaaa",
      "--faint-foreground": "#888888",
      "--chart-grid": "#222222",
      "--chart-axis": "#333333",
      "--chart-series-1": "#0000ff",
      "--chart-series-2": "#ff0000",
      "--chart-series-3": "#00ff00",
    };
    const palette = readChartPalette({
      getPropertyValue: (name) => values[name] ?? "",
    } as CSSStyleDeclaration);

    expect(palette.surface).toBe("#111111");
    expect(palette.series).toEqual(["#0000ff", "#ff0000", "#00ff00"]);
  });
});
