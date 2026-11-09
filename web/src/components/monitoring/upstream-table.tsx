import { Network } from "lucide-react";
import { STATUS_POLL_INTERVAL_MS } from "@/lib/monitoring-config";
import { SourceStatus, type SourceStatusState } from "@/components/monitoring/source-status";
import { StatusBadge } from "@/components/status-badge";
import {
  EmptyState,
  ErrorState,
  LoadingState,
} from "@/components/states";
import { PanelDescription, PanelHeader, PanelTitle } from "@/components/ui/panel";
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
  className,
}: UpstreamTableProps) {
  return (
    <section
      aria-labelledby="upstream-title"
      className={cn("mt-6 border border-border bg-card pt-4", className)}
    >
      <PanelHeader className="px-4">
        <div>
          <PanelTitle id="upstream-title">上游观测与未暴露字段</PanelTitle>
          <PanelDescription>
            Venue 从 /api/v1/instruments 派生 · Feed 从 collector /status
            派生 · 空值是契约事实，不用绿色圆点补空白。
          </PanelDescription>
        </div>
        <Network className="size-4 text-faint-foreground" aria-hidden="true" />
      </PanelHeader>
      <div className="mt-2 px-4">
        <SourceStatus
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
      </div>
      <div className="mt-2">
        {state === "loading" ? (
          <LoadingState label="正在读取 Instrument Reference…" showSkeleton />
        ) : null}
        {state === "error" ? (
          <ErrorState
            description={
              <span>
                Instrument Reference 加载失败：
                {errorMessage ?? "数据源不可用"}。最后成功：
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
          <Table>
            <caption className="sr-only">
              上游观测：Venue、Instrument 数量、Feed、连接字段能力缺口与 Feed Freshness
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
                      {row.activeCount} 活跃 · {row.delistedCount} 已下架
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
                        {row.trackedCount} 项跟踪 · {row.staleCount} 项失活
                      </StatusBadge>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : null}
      </div>
      <p className="mt-3 border-t border-border px-4 py-3 text-[11px] text-faint-foreground">
        Freshness 只表示最近事件年龄，不证明当前连接健康。网络延迟与真实 I/O
        Rate 均未暴露；不从 Freshness 推断连接状态、心跳或重连。
      </p>
    </section>
  );
}
