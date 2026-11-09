import type { TrendPoint } from "@/api/monitoring/types";
import { formatUtcTime } from "@/lib/format";

/**
 * Variant B chart rules: one value axis, 2px lines, no symbol noise, no
 * animation, no zero-filled gaps, and an explicit ARIA description. The
 * canvas is always paired with an accessible data table by TrendChart.
 */

export interface ChartPalette {
  surface: string;
  textPrimary: string;
  textSecondary: string;
  muted: string;
  grid: string;
  axis: string;
  series: string[];
}

export const LIGHT_CHART_PALETTE: ChartPalette = {
  surface: "#fcfcfb",
  textPrimary: "#0b0b0b",
  textSecondary: "#52514e",
  muted: "#898781",
  grid: "#e1e0d9",
  axis: "#c3c2b7",
  series: ["#2a78d6", "#eb6834", "#1baf7a"],
};

export const DARK_CHART_PALETTE: ChartPalette = {
  surface: "#1a1a19",
  textPrimary: "#f0efec",
  textSecondary: "#c3c2b7",
  muted: "#898781",
  grid: "#2c2c2a",
  axis: "#383835",
  series: ["#3987e5", "#d95926", "#199e70"],
};

export function readChartPalette(
  style: Pick<CSSStyleDeclaration, "getPropertyValue">,
  fallback: ChartPalette = LIGHT_CHART_PALETTE,
): ChartPalette {
  const read = (name: string): string =>
    style.getPropertyValue(name).trim() || "";
  return {
    surface: read("--card") || fallback.surface,
    textPrimary: read("--foreground") || fallback.textPrimary,
    textSecondary:
      read("--muted-foreground") || fallback.textSecondary,
    muted: read("--faint-foreground") || fallback.muted,
    grid: read("--chart-grid") || fallback.grid,
    axis: read("--chart-axis") || fallback.axis,
    series: [1, 2, 3].map(
      (index) => read(`--chart-series-${index}`) || fallback.series[index - 1]!,
    ),
  };
}

export interface ColoredTrendSeries {
  name: string;
  color: string;
  points: TrendPoint[];
}

export interface AlignedTrend {
  timestamps: number[];
  /** One row per timestamp; each row aligns with the input series order. */
  rows: Array<{ ts: number; values: Array<number | null> }>;
}

export function alignTrendPoints(
  series: Array<{ points: TrendPoint[] }>,
): AlignedTrend {
  const timestamps = new Set<number>();
  for (const item of series) {
    for (const point of item.points) {
      timestamps.add(point.ts);
    }
  }
  const sorted = [...timestamps].sort((left, right) => left - right);

  const lookups = series.map((item) => {
    const lookup = new Map<number, number | null>();
    for (const point of item.points) {
      lookup.set(point.ts, point.value);
    }
    return lookup;
  });

  return {
    timestamps: sorted,
    rows: sorted.map((ts) => ({
      ts,
      values: lookups.map((lookup) => lookup.get(ts) ?? null),
    })),
  };
}

export interface TrendChartOption {
  animation: boolean;
  color: string[];
  grid: Record<string, unknown>;
  tooltip: Record<string, unknown>;
  xAxis: Record<string, unknown>;
  yAxis: Record<string, unknown>;
  series: Array<Record<string, unknown>>;
  aria: Record<string, unknown>;
}

export interface BuildTrendChartOptionInput {
  title: string;
  description: string;
  unit: string;
  series: ColoredTrendSeries[];
  palette: ChartPalette;
}

export function buildTrendChartOption({
  title,
  description,
  unit,
  series,
  palette,
}: BuildTrendChartOptionInput): TrendChartOption {
  const aligned = alignTrendPoints(series);

  return {
    animation: false,
    color: palette.series,
    grid: { left: 52, right: 24, top: 20, bottom: 38 },
    tooltip: {
      trigger: "axis",
      axisPointer: {
        type: "line",
        lineStyle: { color: palette.axis, width: 1, type: "solid" },
      },
      backgroundColor: palette.surface,
      borderColor: palette.grid,
      textStyle: { color: palette.textPrimary, fontSize: 12 },
      valueFormatter: (value: unknown) =>
        typeof value === "number" && Number.isFinite(value)
          ? `${value} ${unit}`
          : "未知",
    },
    xAxis: {
      type: "category",
      data: aligned.timestamps.map((ts) =>
        formatUtcTime(ts).replace(" UTC", ""),
      ),
      boundaryGap: false,
      axisLine: {
        lineStyle: { color: palette.axis, width: 1, type: "solid" },
      },
      axisTick: { show: false },
      axisLabel: { color: palette.muted, fontSize: 11, margin: 12 },
    },
    yAxis: {
      type: "value",
      scale: true,
      axisLine: { show: false },
      axisLabel: { color: palette.muted, fontSize: 11 },
      splitLine: {
        lineStyle: { color: palette.grid, width: 1, type: "solid" },
      },
    },
    series: series.map((item, index) => ({
      name: item.name,
      type: "line",
      data: aligned.rows.map((row) => row.values[index] ?? null),
      showSymbol: false,
      symbol: "circle",
      symbolSize: 8,
      lineStyle: {
        width: 2,
        color: item.color,
        cap: "round",
        join: "round",
      },
      itemStyle: {
        color: item.color,
        borderColor: palette.surface,
        borderWidth: 2,
      },
      areaStyle: series.length === 1 ? { opacity: 0.1 } : undefined,
      emphasis: { focus: "series", scale: true },
      connectNulls: false,
    })),
    aria: {
      enabled: true,
      description: description ? `${title}。${description}` : title,
    },
  };
}

export function seriesColors(
  series: Array<{ name: string }>,
  palette: ChartPalette,
): Array<{ name: string; color: string }> {
  return series.map((item, index) => ({
    name: item.name,
    color: palette.series[index % palette.series.length] ?? palette.series[0]!,
  }));
}
