import { Network } from "lucide-react";
import type * as React from "react";
import {
  HelpTrigger,
  type HelpTriggerSide,
} from "@/components/help-trigger";
import { STATUS_POLL_INTERVAL_MS } from "@/lib/monitoring-config";
import { SourceStatus, type SourceStatusState } from "@/components/monitoring/source-status";
import { StatusBadge } from "@/components/status-badge";
import {
  EmptyState,
  ErrorState,
  LoadingState,
} from "@/components/states";
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
import type { VenueRow } from "@/lib/venues";
import { cn } from "@/lib/utils";

export interface UpstreamTableProps {
  rows: VenueRow[];
  state: SourceStatusState;
  lastSuccessAt: number | null;
  errorMessage?: string | null;
  help?: { label: string; content: React.ReactNode; side?: HelpTriggerSide };
  className?: string;
}

function NotExposed() {
  return <StatusBadge tone="unknown">未暴露</StatusBadge>;
}

/**
 * Venue rows are derived from the real Instrument Reference Data and the
 * collector's tracked feed entries. Connection state, heartbeat, reconnect
 * count, network latency and I/O rate are absent from the contract: they are
 * rendered as explicit gaps and never inferred from freshness.
 */
export function UpstreamTable({
  rows,
  state,
  lastSuccessAt,
  errorMessage,
  help,
  className,
}: UpstreamTableProps) {
  return (
    <Card
      data-slot="upstream-table"
      role="region"
      aria-label="上游状态"
      className={cn("min-w-0 p-5", className)}
    >
      <CardHeader className="p-0">
        <div className="flex min-w-0 items-center gap-2">
          <span className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-accent-soft text-accent-strong">
            <Network className="size-4" aria-hidden="true" />
          </span>
          <CardTitle>上游状态</CardTitle>
          {help ? (
            <HelpTrigger
              label={help.label}
              content={help.content}
              side={help.side ?? "bottom"}
            />
          ) : null}
        </div>
        <span className="font-mono text-[11px] tabular-nums text-faint-foreground">
          {rows.length} 个 Venue
        </span>
      </CardHeader>
      <CardContent className="p-0 pt-3">
        <p className="text-[11px] leading-relaxed text-muted-foreground">
          Venue 来自 /api/v1/instruments。Feed Freshness 来自 collector
          /status。空值是契约事实，不用健康色补空白。
        </p>
        <SourceStatus
          className="mt-3"
          source="Instrument Reference /api/v1/instruments"
          window={`当前快照（${STATUS_POLL_INTERVAL_MS / 1000} 秒轮询）`}
          state={state}
          lastSuccessAt={lastSuccessAt}
          reason={
            state === "error"
              ? errorMessage ?? "Instrument Reference 不可用"
              : null
          }
        />
        <div className="mt-3">
          {state === "loading" ? (
            <LoadingState label="正在读取 Instrument Reference" showSkeleton />
          ) : null}
          {state === "error" ? (
            <ErrorState
              description={
                <span>
                  Instrument Reference 加载失败。最后成功{" "}
                  {formatUtcDateTime(lastSuccessAt)}。没有真实 Instrument
                  时不生成 Venue 行。
                </span>
              }
            />
          ) : null}
          {state === "ready" && rows.length === 0 ? (
            <EmptyState
              title="暂无 Instrument Reference 数据"
              description="没有真实 Instrument 时不生成 Venue 行，也不预置任何交易所或 Feed。"
            />
          ) : null}
          {state === "ready" && rows.length > 0 ? (
            <Table className="min-w-[1040px] text-xs">
              <caption className="sr-only">
                上游观测，展示 Venue、Instrument 数量、Feed、连接字段能力缺口与 Feed Freshness
              </caption>
              <TableHeader>
                <TableRow>
                  <TableHead>Venue / Feed</TableHead>
                  <TableHead className="text-right">Instruments</TableHead>
                  <TableHead>Asset Classes</TableHead>
                  <TableHead>连接状态</TableHead>
                  <TableHead>心跳</TableHead>
                  <TableHead>重连计数</TableHead>
                  <TableHead>网络延迟</TableHead>
                  <TableHead>I/O Rate</TableHead>
                  <TableHead>最后事件</TableHead>
                  <TableHead>Feed Freshness</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((row) => (
                  <TableRow key={row.venue}>
                    <TableCell>
                      <div className="font-mono font-medium">{row.venue}</div>
                      <div className="text-[11px] text-faint-foreground">
                        {row.feeds.length > 0
                          ? row.feeds.join(", ")
                          : "Feed 未暴露"}
                      </div>
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      <div>{row.instrumentCount}</div>
                      <div className="text-[11px] text-faint-foreground">
                        {row.activeCount} 活跃，{row.delistedCount} 已下架
                      </div>
                    </TableCell>
                    <TableCell>{row.assetClasses.join(", ")}</TableCell>
                    <TableCell>
                      <NotExposed />
                    </TableCell>
                    <TableCell>
                      <NotExposed />
                    </TableCell>
                    <TableCell>
                      <NotExposed />
                    </TableCell>
                    <TableCell>
                      <NotExposed />
                    </TableCell>
                    <TableCell>
                      <NotExposed />
                    </TableCell>
                    <TableCell className="whitespace-nowrap font-mono text-[11px]">
                      {formatUtcDateTime(row.lastEventTs)}
                    </TableCell>
                    <TableCell>
                      {row.trackedCount === 0 ? (
                        <span className="text-faint-foreground">—</span>
                      ) : (
                        <StatusBadge
                          tone={row.staleCount > 0 ? "critical" : "good"}
                        >
                          {row.trackedCount} 项跟踪，{row.staleCount} 项失活
                        </StatusBadge>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : null}
        </div>
        <p className="mt-3 border-t border-card-border pt-3 text-[11px] leading-relaxed text-faint-foreground">
          Freshness 只表示最近事件年龄，不证明当前连接健康。连接、心跳、重连计数与网络延迟均未暴露，不从
          Freshness 推断。
        </p>
      </CardContent>
    </Card>
  );
}
