import type * as React from "react";
import {
  EmptyState,
  ErrorState,
  LoadingState,
  UnknownState,
  type UnknownCause,
} from "@/components/states";
import {
  PanelDescription,
  PanelHeader,
  PanelTitle,
} from "@/components/ui/panel";
import { cn } from "@/lib/utils";

export interface ChartLegendItem {
  name: string;
  /** CSS color, preferably a chart-series token. */
  color: string;
  value: string | null;
  unit?: string;
}

export type ChartPanelState = "ready" | "loading" | "empty" | "unknown" | "error";

export interface ChartPanelProps {
  title: string;
  description?: React.ReactNode;
  legend?: ChartLegendItem[];
  state?: ChartPanelState;
  unknownCause?: UnknownCause;
  errorMessage?: React.ReactNode;
  onRetry?: () => void;
  /** Chart canvas mount point. The canvas is never the only data source. */
  children?: React.ReactNode;
  /** Accessible data-table fallback slot, always rendered when provided. */
  dataTable?: React.ReactNode;
  dataTableSummary?: string;
  className?: string;
}

/**
 * Chart contract: inline legend with current values, explicit states, and an
 * accessible `<details>` data-table fallback. The panel wrapper carries the
 * border and surface; the chart implementation only fills the body.
 */
export function ChartPanel({
  title,
  description,
  legend = [],
  state = "ready",
  unknownCause = "not-exposed",
  errorMessage,
  onRetry,
  children,
  dataTable,
  dataTableSummary = "查看数据表",
  className,
}: ChartPanelProps) {
  return (
    <section
      data-slot="chart-panel"
      data-state={state}
      aria-label={title}
      className={cn("border border-border bg-card p-5", className)}
    >
      <PanelHeader>
        <div>
          <PanelTitle>{title}</PanelTitle>
          {description ? (
            <PanelDescription>{description}</PanelDescription>
          ) : null}
        </div>
        {legend.length > 0 ? (
          <div className="flex flex-wrap gap-x-4 gap-y-2 text-xs">
            {legend.map((item) => (
              <span key={item.name} className="inline-flex items-center gap-1.5">
                <span
                  className="h-0.5 w-4 rounded-full"
                  style={{ backgroundColor: item.color }}
                  aria-hidden="true"
                />
                <span className="text-muted-foreground">{item.name}</span>
                <span className="font-semibold tabular-nums">
                  {item.value ?? "未知"}
                </span>
                {item.unit ? (
                  <span className="text-faint-foreground">{item.unit}</span>
                ) : null}
              </span>
            ))}
          </div>
        ) : null}
      </PanelHeader>

      <div className="mt-3">
        {state === "loading" ? <LoadingState showSkeleton /> : null}
        {state === "empty" ? (
          <EmptyState
            title="没有数据"
            description="不会自动扩大时间范围；请调整筛选条件后重试。"
          />
        ) : null}
        {state === "unknown" ? <UnknownState cause={unknownCause} /> : null}
        {state === "error" ? (
          <ErrorState
            description={errorMessage ?? "图表数据加载失败。"}
            onRetry={onRetry}
          />
        ) : null}
        {state === "ready" ? children : null}
      </div>

      {dataTable ? (
        <details className="mt-4">
          <summary className="cursor-pointer text-xs text-muted-foreground">
            {dataTableSummary}
          </summary>
          <div className="mt-2 max-h-52 overflow-auto">{dataTable}</div>
        </details>
      ) : null}
    </section>
  );
}
