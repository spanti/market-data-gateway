import type * as React from "react";
import { HelpTrigger, type HelpTriggerSide } from "@/components/help-trigger";
import {
  EmptyState,
  ErrorState,
  LoadingState,
  UnknownState,
  type UnknownCause,
} from "@/components/states";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

export interface ChartLegendItem {
  name: string;
  /** CSS color, preferably a chart-series token. */
  color: string;
  value: string | null;
  unit?: string;
}

export type ChartPanelState = "ready" | "loading" | "empty" | "unknown" | "error";

export interface ChartPanelHelp {
  /** Accessible name for the `?` trigger. */
  label: string;
  /** Supplementary definitions and source window facts. */
  content: React.ReactNode;
  side?: HelpTriggerSide;
}

export interface ChartPanelProps {
  title: string;
  help?: ChartPanelHelp;
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
 * accessible `<details>` data-table fallback. The panel wrapper carries the D
 * card surface; the chart implementation only fills the body. Help carries
 * definitions only, never required error or operation feedback.
 */
export function ChartPanel({
  title,
  help,
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
    <Card
      data-slot="chart-panel"
      role="region"
      data-state={state}
      aria-label={title}
      className={cn("p-5", className)}
    >
      <CardHeader className="p-0">
        <div className="min-w-0">
          <div className="flex min-w-0 items-center gap-1.5">
            <CardTitle>{title}</CardTitle>
            {help ? (
              <HelpTrigger
                label={help.label}
                content={help.content}
                side={help.side ?? "bottom"}
              />
            ) : null}
          </div>
        </div>
        {legend.length > 0 ? (
          <div className="flex flex-wrap gap-x-4 gap-y-2 text-xs">
            {legend.map((item) => (
              <span
                key={item.name}
                className="inline-flex min-w-0 items-center gap-1.5"
              >
                <span
                  className="h-0.5 w-4 shrink-0 rounded-full"
                  style={{ backgroundColor: item.color }}
                  aria-hidden="true"
                />
                <span className="break-words text-muted-foreground">
                  {item.name}
                </span>
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
      </CardHeader>
      <CardContent className="p-0 pt-3">
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

        {dataTable ? (
          <details className="mt-4">
            <summary className="cursor-pointer text-xs text-muted-foreground">
              {dataTableSummary}
            </summary>
            <div className="mt-2 max-h-52 overflow-auto">{dataTable}</div>
          </details>
        ) : null}
      </CardContent>
    </Card>
  );
}
