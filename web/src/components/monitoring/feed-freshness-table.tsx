import type * as React from "react";
import { Database } from "lucide-react";
import {
  HelpTrigger,
  type HelpTriggerSide,
} from "@/components/help-trigger";
import type { FeedFreshnessRow } from "@/lib/freshness";
import {
  EmptyState,
  ErrorState,
  LoadingState,
  UnknownState,
} from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatUtcDateTime } from "@/lib/format";
import { cn } from "@/lib/utils";
import { IconTile } from "@/components/icon-tile";

export interface FeedFreshnessTableProps {
  rows: FeedFreshnessRow[];
  help?: { label: string; content: React.ReactNode; side?: HelpTriggerSide };
  state?: "loading" | "ready" | "error" | "unknown";
  unknownReason?: React.ReactNode;
  errorMessage?: string | null;
  lastSuccessAt?: number | null;
  className?: string;
}

function seconds(value: number | null): string {
  return value === null ? "—" : `${Math.round(value)}s`;
}

/**
 * Data-detail table for Feed Freshness. It intentionally duplicates the
 * triage card's facts so exact server values can be audited without hover.
 */
export function FeedFreshnessTable({
  rows,
  help,
  state = "ready",
  unknownReason,
  errorMessage,
  lastSuccessAt = null,
  className,
}: FeedFreshnessTableProps) {
  return (
    <Card
      data-slot="feed-freshness-table"
      role="region"
      aria-label="Feed Freshness 明细"
      className={cn("min-w-0 p-5", className)}
    >
      <CardHeader className="p-0">
        <div className="flex min-w-0 items-center gap-2">
          <IconTile icon={Database} />
          <CardTitle>Feed Freshness 明细</CardTitle>
          {help ? (
            <HelpTrigger
              label={help.label}
              content={help.content}
              side={help.side ?? "bottom"}
            />
          ) : null}
        </div>
        <span className="font-mono text-[11px] tabular-nums text-faint-foreground">
          {rows.length} 项
        </span>
      </CardHeader>
      <CardContent className="p-0 pt-3">
        {state === "unknown" ? (
          <UnknownState cause="not-exposed" reason={unknownReason} />
        ) : state === "loading" ? (
          <LoadingState label="正在读取 Feed Freshness 明细" showSkeleton />
        ) : state === "error" ? (
          <ErrorState
            description={
              <span>
                Feed Freshness 明细加载失败。最后成功{" "}
                {formatUtcDateTime(lastSuccessAt)}。不会继续展示旧明细为当前健康。
                {errorMessage ? (
                  <>
                    <span className="text-muted-foreground"> Raw error </span>
                    <span data-raw-evidence="true">{errorMessage}</span>
                  </>
                ) : null}
              </span>
            }
          />
        ) : null}
        {state !== "ready" ? null : rows.length === 0 ? (
          <EmptyState
            title="没有 Feed Freshness 明细"
            description="没有真实跟踪项时保持空白，不生成健康记录。"
          />
        ) : (
          <Table className="min-w-[760px] text-xs">
            <caption className="sr-only">
              Feed Freshness 明细，按 feed、InstrumentId 和 Bar Interval 展示服务端 age、threshold、stale 与最后事件
            </caption>
            <TableHeader>
              <TableRow>
                <TableHead>InstrumentId</TableHead>
                <TableHead>Feed</TableHead>
                <TableHead>Bar Interval</TableHead>
                <TableHead className="text-right">Age</TableHead>
                <TableHead className="text-right">Threshold</TableHead>
                <TableHead>Last Event</TableHead>
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
                    {seconds(row.ageS)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {seconds(row.thresholdS)}
                  </TableCell>
                  <TableCell className="whitespace-nowrap font-mono text-[11px]">
                    {formatUtcDateTime(row.lastEventTs ?? null)}
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
      </CardContent>
    </Card>
  );
}
