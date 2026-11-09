import type * as React from "react";
import { EmptyState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { PanelDescription, PanelHeader, PanelTitle } from "@/components/ui/panel";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { BarInterval } from "@/lib/time-range";
import { cn } from "@/lib/utils";

export interface FreshnessRow {
  instrumentId: string;
  feed: string;
  interval: BarInterval;
  ageS: number | null;
  thresholdS: number | null;
  stale: boolean | null;
}

export interface FreshnessMatrixProps {
  title?: string;
  description?: React.ReactNode;
  overallStatus?: React.ReactNode;
  rows: FreshnessRow[];
  className?: string;
}

function formatSeconds(value: number | null): string {
  if (value === null) {
    return "—";
  }
  return `${Math.round(value)}s`;
}

function remainingLabel(row: FreshnessRow): string {
  if (row.ageS === null || row.thresholdS === null) {
    return "阈值未知";
  }
  const remaining = row.thresholdS - row.ageS;
  if (remaining <= 0) {
    return "已超出阈值";
  }
  return `剩余 ${Math.round(remaining)}s`;
}

/**
 * Feed Freshness matrix shell. Feed Freshness is separate from Dataset
 * Freshness and is never inferred from connection state.
 */
export function FreshnessMatrix({
  title = "Feed Freshness",
  description = "按 (feed, InstrumentId, Bar Interval) 展示服务端提供的 age_s / threshold_s / stale。",
  overallStatus,
  rows,
  className,
}: FreshnessMatrixProps) {
  return (
    <section
      data-slot="freshness-matrix"
      aria-label={title}
      className={cn(
        "mt-7 border-y border-border bg-card py-4",
        className,
      )}
    >
      <PanelHeader className="px-4">
        <div>
          <PanelTitle>{title}</PanelTitle>
          <PanelDescription>{description}</PanelDescription>
        </div>
        {overallStatus}
      </PanelHeader>
      <div className="mt-3">
        {rows.length === 0 ? (
          <EmptyState
            title="暂无 Feed Freshness 数据"
            description="没有真实跟踪项时保持空白，不生成健康记录。"
          />
        ) : (
          <Table>
            <caption className="sr-only">
              Feed Freshness，按 feed、InstrumentId 和 Bar Interval 展示年龄与阈值
            </caption>
            <TableHeader>
              <TableRow>
                <TableHead>InstrumentId</TableHead>
                <TableHead>Feed</TableHead>
                <TableHead>Bar Interval</TableHead>
                <TableHead className="text-right">Age / Threshold</TableHead>
                <TableHead>状态</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <TableRow key={`${row.feed}:${row.instrumentId}:${row.interval}`}>
                  <TableCell className="font-mono">{row.instrumentId}</TableCell>
                  <TableCell>{row.feed}</TableCell>
                  <TableCell className="font-mono">{row.interval}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    <div>
                      {formatSeconds(row.ageS)} / {formatSeconds(row.thresholdS)}
                    </div>
                    <div className="text-[11px] text-faint-foreground">
                      {remainingLabel(row)}
                    </div>
                  </TableCell>
                  <TableCell>
                    {row.stale === null ? (
                      <StatusBadge tone="unknown">未暴露</StatusBadge>
                    ) : row.stale ? (
                      <StatusBadge tone="critical">失活</StatusBadge>
                    ) : (
                      <StatusBadge tone="good">正常</StatusBadge>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </div>
    </section>
  );
}
