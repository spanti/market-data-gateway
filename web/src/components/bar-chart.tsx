import * as echarts from "echarts/core";
import { LineChart } from "echarts/charts";
import {
  AriaComponent,
  GridComponent,
  TooltipComponent,
} from "echarts/components";
import { SVGRenderer } from "echarts/renderers";
import type { EChartsType } from "echarts/core";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { useEffect, useId, useMemo, useRef, useState } from "react";
import type { KeyboardEvent as ReactKeyboardEvent, ReactNode } from "react";
import type { BarMessageEnvelope } from "@/api/endpoints/bars";
import {
  ChartPanel,
  type ChartLegendItem,
  type ChartPanelState,
} from "@/components/chart-panel";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  BAR_CHART_COLORS,
  BAR_CHART_SERIES,
  buildBarChartOption,
} from "@/lib/bar-chart-option";
import { deriveMaxRevisionBars } from "@/lib/bar-data";
import { useChartPalette } from "@/components/monitoring/chart-option";

echarts.use([LineChart, GridComponent, TooltipComponent, AriaComponent, SVGRenderer]);

const DATA_TABLE_PAGE_SIZE = 100;

export interface BarChartProps {
  /** Raw REST + WS rows. The chart applies max-revision selection itself. */
  bars: readonly BarMessageEnvelope[];
  state?: ChartPanelState;
  errorMessage?: ReactNode;
  requestId?: string | null;
  onRetry?: () => void;
  className?: string;
}

function BarDataTable({ bars }: { bars: readonly BarMessageEnvelope[] }) {
  const [page, setPage] = useState(0);
  const pageCount = Math.max(1, Math.ceil(bars.length / DATA_TABLE_PAGE_SIZE));
  const currentPage = Math.min(page, pageCount - 1);
  const start = currentPage * DATA_TABLE_PAGE_SIZE;
  const visible = bars.slice(start, start + DATA_TABLE_PAGE_SIZE);

  return (
    <div>
      <Table>
        <TableCaption>
          数据表与图表使用同一组最大 Revision 数据；十进制与时间戳为原始字符串。
          第 {currentPage + 1} / {pageCount} 页，共 {bars.length} 个时间点。
        </TableCaption>
        <TableHeader>
          <TableRow>
            <TableHead>UTC 时间</TableHead>
            <TableHead className="text-right">open</TableHead>
            <TableHead className="text-right">high</TableHead>
            <TableHead className="text-right">low</TableHead>
            <TableHead className="text-right">close</TableHead>
            <TableHead className="text-right">volume</TableHead>
            <TableHead className="text-right">trade_count</TableHead>
            <TableHead className="text-right">revision</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {visible.map((bar) => (
            <TableRow key={`${bar.payload.open_ts}-${bar.payload.revision}`}>
              <TableCell className="whitespace-nowrap font-mono">
                {bar.payload.open_ts}
              </TableCell>
              <TableCell className="text-right font-mono tabular-nums">
                {bar.payload.open}
              </TableCell>
              <TableCell className="text-right font-mono tabular-nums">
                {bar.payload.high}
              </TableCell>
              <TableCell className="text-right font-mono tabular-nums">
                {bar.payload.low}
              </TableCell>
              <TableCell className="text-right font-mono tabular-nums">
                {bar.payload.close}
              </TableCell>
              <TableCell className="text-right font-mono tabular-nums">
                {bar.payload.volume}
              </TableCell>
              <TableCell className="text-right font-mono tabular-nums">
                {bar.payload.trade_count ?? "—"}
              </TableCell>
              <TableCell className="text-right font-mono tabular-nums">
                {bar.payload.revision}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
      {pageCount > 1 ? (
        <div className="mt-2 flex items-center justify-end gap-2">
          <Button
            size="sm"
            variant="outline"
            onClick={() => setPage(Math.max(0, currentPage - 1))}
            disabled={currentPage === 0}
          >
            <ChevronLeft aria-hidden="true" />
            上一页
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() =>
              setPage(Math.min(pageCount - 1, currentPage + 1))
            }
            disabled={currentPage >= pageCount - 1}
          >
            下一页
            <ChevronRight aria-hidden="true" />
          </Button>
        </div>
      ) : null}
    </div>
  );
}

/**
 * Max-revision Bar chart. Only the line coordinates convert to Number; the
 * inline legend and the always-present data table show the exact server
 * strings. The chart is never the only data source.
 */
export function BarChart({
  bars,
  state = "ready",
  errorMessage,
  requestId,
  onRetry,
  className,
}: BarChartProps) {
  const points = useMemo(() => deriveMaxRevisionBars(bars), [bars]);
  const palette = useChartPalette();
  const option = useMemo(
    () => buildBarChartOption(points, undefined, palette),
    [points, palette],
  );
  const effectiveState: ChartPanelState =
    state === "ready" && points.length === 0 ? "empty" : state;
  const hostRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<EChartsType | null>(null);
  const [selectedIndex, setSelectedIndex] = useState<number | null>(null);
  const chartLabelId = useId();

  const selectedBar =
    selectedIndex === null ? null : (points[selectedIndex] ?? null);
  const lastBar = points[points.length - 1] ?? null;

  const legend: ChartLegendItem[] = lastBar
    ? BAR_CHART_SERIES.map((key, index) => ({
        name: key,
        color: palette.series[index] ?? BAR_CHART_COLORS[key],
        value: lastBar.payload[key],
      }))
    : [];

  useEffect(() => {
    const host = hostRef.current;
    if (!host || points.length === 0) {
      chartRef.current = null;
      return;
    }
    let chart: EChartsType | null = null;
    try {
      chart = echarts.init(host, undefined, {
        renderer: "svg",
        width: host.clientWidth > 0 ? host.clientWidth : 800,
        height: 260,
      });
      chart.setOption(option, { notMerge: true });
      chartRef.current = chart;
    } catch {
      // If a test/embedded environment has no usable DOM, the exact data
      // table below remains the complete, keyboard-readable source.
      chartRef.current = null;
    }

    const handleResize = () => {
      try {
        chart?.resize();
      } catch {
        // Ignore resize failures on a disposed/hidden chart.
      }
    };
    window.addEventListener("resize", handleResize);
    return () => {
      window.removeEventListener("resize", handleResize);
      try {
        chart?.dispose();
      } catch {
        // Ignore dispose failures.
      }
      chartRef.current = null;
    };
  }, [option, points.length]);

  useEffect(() => {
    if (selectedIndex === null) {
      return;
    }
    try {
      chartRef.current?.dispatchAction({
        type: "showTip",
        seriesIndex: 0,
        dataIndex: selectedIndex,
      });
    } catch {
      // Tooltip dispatch is an enhancement; the aria-live readout is primary.
    }
  }, [selectedIndex]);

  const handleKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (points.length === 0) {
      return;
    }
    if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
      event.preventDefault();
      const current = selectedIndex ?? points.length - 1;
      const delta = event.key === "ArrowLeft" ? -1 : 1;
      const next = Math.min(points.length - 1, Math.max(0, current + delta));
      setSelectedIndex(next);
    }
    if (event.key === "Home") {
      event.preventDefault();
      setSelectedIndex(0);
    }
    if (event.key === "End") {
      event.preventDefault();
      setSelectedIndex(points.length - 1);
    }
  };

  const readout = selectedBar
    ? `时间 ${selectedBar.payload.open_ts}，close ${selectedBar.payload.close}，high ${selectedBar.payload.high}，low ${selectedBar.payload.low}`
    : "聚焦图表后使用左右方向键读取数据点；完整值见下方数据表。";

  return (
    <ChartPanel
      className={className}
      title="最大 Revision Bar 图"
      description="单轴价格 · 2px 线 · 缺失点断开。按 (instrument_id, interval, open_ts) 保留最大 revision；revision 相同时取最新 ingest_ts，二者都相同才按服务端响应顺序取最后一条（Data Contract §9）。仅图表坐标转为 Number。"
      legend={legend}
      state={effectiveState}
      errorMessage={
        <>
          {errorMessage}
          {requestId ? (
            <div className="mt-1 font-mono text-[11px]">
              Request ID: {requestId}
            </div>
          ) : null}
        </>
      }
      onRetry={onRetry}
      dataTableSummary={`查看数据表（${points.length} 个时间点）`}
      dataTable={points.length > 0 ? <BarDataTable bars={points} /> : undefined}
    >
      <div>
        {/*
          ECharts' AriaComponent overwrites the host's aria-label with its own
          generated chart description. aria-labelledby keeps the keyboard
          instruction stable in the accessibility tree.
        */}
        <span id={chartLabelId} className="sr-only">
          最大 Revision Bar 价格图。使用左右方向键逐点读取，完整精确值见下方数据表。
        </span>
        <div
          ref={hostRef}
          data-chart="true"
          role="img"
          tabIndex={0}
          aria-labelledby={chartLabelId}
          aria-describedby="bar-chart-readout"
          onFocus={() => {
            if (points.length > 0 && selectedIndex === null) {
              setSelectedIndex(points.length - 1);
            }
          }}
          onKeyDown={handleKeyDown}
          className="h-[260px] w-full rounded-sm outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
        />
        <p
          id="bar-chart-readout"
          aria-live="polite"
          className="mt-2 min-h-4 text-[11px] text-muted-foreground"
        >
          {readout}
        </p>
      </div>
    </ChartPanel>
  );
}
