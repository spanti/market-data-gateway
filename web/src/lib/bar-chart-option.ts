import type { BarMessageEnvelope } from "@/api/endpoints/bars";
import {
  LIGHT_CHART_PALETTE,
  type ChartPalette,
} from "@/components/monitoring/chart-option";
import type { LineSeriesOption } from "echarts/charts";
import type {
  AriaComponentOption,
  GridComponentOption,
  TooltipComponentOption,
} from "echarts/components";
import type { ComposeOption } from "echarts/core";
import { parseIsoInstant } from "@/lib/time-range";

/**
 * Chart option for the max-revision Bar view.
 *
 * Only chart coordinates (`open/high/low/close`) convert to Number. The raw
 * envelopes keep their exact decimal strings and are rendered unchanged in
 * the legend and data table. Missing/invalid decimals become gaps
 * (`connectNulls: false`) rather than zero.
 */

export type BarChartOption = ComposeOption<
  LineSeriesOption | GridComponentOption | TooltipComponentOption | AriaComponentOption
>;

export const BAR_CHART_SERIES = ["close", "high", "low"] as const;
export type BarChartSeriesKey = (typeof BAR_CHART_SERIES)[number];

export const BAR_CHART_COLORS: Record<BarChartSeriesKey, string> = {
  close: "#2a78d6",
  high: "#eb6834",
  low: "#1baf7a",
};

function pad(value: number, length = 2): string {
  return String(value).padStart(length, "0");
}

export function formatUtcChartLabel(openTs: string): string {
  const instant = parseIsoInstant(openTs);
  if (instant === null) {
    return openTs;
  }
  return [
    `${instant.getUTCFullYear()}-${pad(instant.getUTCMonth() + 1)}-${pad(
      instant.getUTCDate(),
    )}`,
    `${pad(instant.getUTCHours())}:${pad(instant.getUTCMinutes())}`,
  ].join(" ");
}

function toCoordinate(value: string): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function seriesData(
  bars: readonly BarMessageEnvelope[],
  key: BarChartSeriesKey,
): Array<number | null> {
  return bars.map((bar) => toCoordinate(bar.payload[key]));
}

function formatCoordinate(value: unknown): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "未知";
  }
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 12,
    useGrouping: false,
  }).format(value);
}

export function buildBarChartOption(
  bars: readonly BarMessageEnvelope[],
  description = "最大 Revision Bar 图。只用图表坐标做数值转换，精确值见图例与数据表。",
  palette: ChartPalette = LIGHT_CHART_PALETTE,
): BarChartOption {
  const categories = bars.map((bar) => formatUtcChartLabel(bar.payload.open_ts));
  const colors = BAR_CHART_SERIES.map(
    (key, index) => palette.series[index] ?? BAR_CHART_COLORS[key],
  );

  return {
    animation: false,
    color: colors,
    aria: {
      enabled: true,
      description,
    },
    grid: { left: 64, right: 24, top: 20, bottom: 38 },
    tooltip: {
      trigger: "axis",
      axisPointer: {
        type: "line",
        lineStyle: { color: palette.axis, width: 1, type: "solid" },
      },
      backgroundColor: palette.surface,
      borderColor: palette.grid,
      textStyle: { color: palette.textPrimary, fontSize: 12 },
      valueFormatter: (value: unknown) => formatCoordinate(value),
    },
    xAxis: {
      type: "category",
      data: categories,
      boundaryGap: false,
      axisLine: {
        lineStyle: { color: palette.axis, width: 1, type: "solid" },
      },
      axisTick: { show: false },
      axisLabel: { color: palette.muted, fontSize: 11, margin: 12 },
    },
    yAxis: {
      type: "value",
      // A price chart must not force a zero baseline; zero would flatten the
      // entire series. Missing points stay gaps, not zero.
      scale: true,
      axisLine: { show: false },
      axisLabel: { color: palette.muted, fontSize: 11 },
      splitLine: {
        lineStyle: { color: palette.grid, width: 1, type: "solid" },
      },
    },
    series: BAR_CHART_SERIES.map((key, index) => ({
      name: key,
      type: "line",
      data: seriesData(bars, key),
      showSymbol: false,
      symbol: "circle",
      symbolSize: 8,
      lineStyle: {
        width: 2,
        color: colors[index],
        cap: "round",
        join: "round",
      },
      itemStyle: {
        color: colors[index],
        borderColor: palette.surface,
        borderWidth: 2,
      },
      emphasis: { focus: "series", scale: true },
      connectNulls: false,
      animation: false,
    })),
  };
}
