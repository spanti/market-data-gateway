import {
  init,
  use as registerEChartsModules,
  type ECharts,
  type EChartsCoreOption,
} from "echarts/core";
import { LineChart } from "echarts/charts";
import {
  AriaComponent,
  GridComponent,
  TooltipComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ChartPanel, type ChartPanelState } from "@/components/chart-panel";
import { ChartDataTable } from "@/components/monitoring/chart-data-table";
import {
  alignTrendPoints,
  buildTrendChartOption,
  LIGHT_CHART_PALETTE,
  readChartPalette,
  seriesColors,
  type ChartPalette,
} from "@/components/monitoring/chart-option";
import type { UnknownCause } from "@/components/states";
import type { TrendSeries } from "@/api/monitoring/types";
import { formatNumber, formatUtcTime } from "@/lib/format";

// Modular ECharts registration: no full-distribution import, and only the
// components this dashboard actually uses.
registerEChartsModules([
  LineChart,
  GridComponent,
  TooltipComponent,
  AriaComponent,
  CanvasRenderer,
]);

export interface TrendChartProps {
  title: string;
  description?: string;
  unit: string;
  series: TrendSeries[];
  state?: ChartPanelState;
  unknownCause?: UnknownCause;
  errorMessage?: string;
  onRetry?: () => void;
  palette?: ChartPalette;
  className?: string;
}

function lastFiniteValue(points: TrendSeries["points"]): number | null {
  for (let index = points.length - 1; index >= 0; index -= 1) {
    const value = points[index]?.value;
    if (value !== null && value !== undefined && Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

function pointCount(series: TrendSeries[]): number {
  return alignTrendPoints(series).timestamps.length;
}

/**
 * Trend chart with an inline legend, keyboard point readout, hover tooltip, and
 * a data-table fallback. The canvas is a visual summary only; the table below
 * it is always the exact-value source.
 */
export function TrendChart({
  title,
  description = "",
  unit,
  series,
  state = "ready",
  unknownCause = "not-loaded",
  errorMessage,
  onRetry,
  palette,
  className,
}: TrendChartProps) {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<ECharts | null>(null);
  const aligned = useMemo(() => alignTrendPoints(series), [series]);
  const lastIndex = Math.max(0, aligned.timestamps.length - 1);
  const [activeIndex, setActiveIndex] = useState(lastIndex);

  const resolvedPalette = useMemo<ChartPalette>(() => {
    if (palette) {
      return palette;
    }
    if (typeof window === "undefined") {
      return LIGHT_CHART_PALETTE;
    }
    return readChartPalette(
      window.getComputedStyle(document.documentElement),
    );
  }, [palette]);

  const colors = useMemo(
    () => seriesColors(series, resolvedPalette),
    [series, resolvedPalette],
  );

  const option = useMemo(
    () =>
      buildTrendChartOption({
        title,
        description,
        unit,
        series: series.map((item, index) => ({
          name: item.name,
          color: colors[index]?.color ?? resolvedPalette.series[0]!,
          points: item.points,
        })),
        palette: resolvedPalette,
      }),
    [title, description, unit, series, colors, resolvedPalette],
  );

  useEffect(() => {
    const element = hostRef.current;
    // jsdom has no ResizeObserver/canvas; production browsers do. The table
    // fallback keeps the chart data available either way.
    if (!element || typeof ResizeObserver === "undefined") {
      return;
    }
    let chart: ECharts | null = null;
    try {
      chart = init(element, undefined, { renderer: "canvas" });
      chart.setOption(option as unknown as EChartsCoreOption, {
        notMerge: true,
      });
      chartRef.current = chart;
    } catch {
      chart?.dispose();
      chartRef.current = null;
      return;
    }
    const observer = new ResizeObserver(() => {
      chart?.resize();
    });
    observer.observe(element);
    return () => {
      observer.disconnect();
      chart?.dispose();
      chartRef.current = null;
    };
  }, [option]);

  useEffect(() => {
    setActiveIndex((current) =>
      Math.min(current, Math.max(0, aligned.timestamps.length - 1)),
    );
  }, [aligned.timestamps.length]);

  const showPoint = useCallback((index: number) => {
    chartRef.current?.dispatchAction({ type: "showTip", dataIndex: index });
  }, []);

  const readout =
    aligned.timestamps.length > 0
      ? [
          formatUtcTime(aligned.timestamps[activeIndex] ?? null),
          ...series.map((item, index) => {
            const value = aligned.rows[activeIndex]?.values[index] ?? null;
            return `${item.name} ${formatNumber(value, 3)}${unit ? ` ${unit}` : ""}`;
          }),
        ].join(" · ")
      : "";

  const legend = series.map((item, index) => ({
    name: item.name,
    color: colors[index]?.color ?? resolvedPalette.series[0]!,
    value: formatNumber(lastFiniteValue(item.points), 3),
    unit,
  }));

  const canRender = state === "ready";

  return (
    <ChartPanel
      title={title}
      description={description}
      legend={legend}
      state={state}
      unknownCause={unknownCause}
      errorMessage={errorMessage}
      onRetry={onRetry}
      dataTable={
        canRender ? (
          <ChartDataTable
            series={series}
            unit={unit}
            caption={`${title} 数据表`}
          />
        ) : undefined
      }
      dataTableSummary={`查看数据表（${pointCount(series)} 个时间点）`}
      className={className}
    >
      <div className="relative">
        <div
          ref={hostRef}
          role="img"
          tabIndex={0}
          data-chart="true"
          aria-label={`${title} 图表。聚焦后使用左右方向键读取时间点，完整数值见下方数据表。`}
          className="h-[260px] w-full rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
          onFocus={() => {
            setActiveIndex(lastIndex);
            showPoint(lastIndex);
          }}
          onBlur={() => {
            chartRef.current?.dispatchAction({ type: "hideTip" });
          }}
          onKeyDown={(event) => {
            if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") {
              return;
            }
            event.preventDefault();
            setActiveIndex((current) => {
              const next =
                event.key === "ArrowLeft"
                  ? Math.max(0, current - 1)
                  : Math.min(lastIndex, current + 1);
              showPoint(next);
              return next;
            });
          }}
        />
        {aligned.timestamps.length === 0 ? (
          <p className="text-xs text-faint-foreground">
            当前窗口没有样本；不会插值或补零。
          </p>
        ) : null}
      </div>
      {readout ? (
        <p
          role="status"
          aria-live="polite"
          className="mt-2 text-[11px] tabular-nums text-muted-foreground"
        >
          {readout}
        </p>
      ) : null}
    </ChartPanel>
  );
}
