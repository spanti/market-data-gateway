import type { BarMessageEnvelope } from "@/api/endpoints/bars";
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
): BarChartOption {
  const categories = bars.map((bar) => formatUtcChartLabel(bar.payload.open_ts));

  return {
    animation: false,
    color: BAR_CHART_SERIES.map((key) => BAR_CHART_COLORS[key]),
    aria: {
      enabled: true,
      description,
    },
    grid: { left: 64, right: 24, top: 20, bottom: 38 },
    tooltip: {
      trigger: "axis",
      axisPointer: {
        type: "line",
        lineStyle: { color: "#c3c2b7", width: 1, type: "solid" },
      },
      backgroundColor: "#fcfcfb",
      borderColor: "#e1e0d9",
      textStyle: { color: "#0b0b0b", fontSize: 12 },
      valueFormatter: (value: unknown) => formatCoordinate(value),
    },
    xAxis: {
      type: "category",
      data: categories,
      boundaryGap: false,
      axisLine: { lineStyle: { color: "#c3c2b7", width: 1, type: "solid" } },
      axisTick: { show: false },
      axisLabel: { color: "#898781", fontSize: 11, margin: 12 },
    },
    yAxis: {
      type: "value",
      // A price chart must not force a zero baseline; zero would flatten the
      // entire series. Missing points stay gaps, not zero.
      scale: true,
      axisLine: { show: false },
      axisLabel: { color: "#898781", fontSize: 11 },
      splitLine: { lineStyle: { color: "#e1e0d9", width: 1, type: "solid" } },
    },
    series: BAR_CHART_SERIES.map((key) => ({
      name: key,
      type: "line",
      data: seriesData(bars, key),
      showSymbol: false,
      symbol: "circle",
      symbolSize: 8,
      lineStyle: {
        width: 2,
        color: BAR_CHART_COLORS[key],
        cap: "round",
        join: "round",
      },
      itemStyle: {
        color: BAR_CHART_COLORS[key],
        borderColor: "#fcfcfb",
        borderWidth: 2,
      },
      emphasis: { focus: "series", scale: true },
      connectNulls: false,
      animation: false,
    })),
  };
}
