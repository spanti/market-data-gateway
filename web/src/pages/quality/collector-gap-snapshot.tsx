import { RefreshCw } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { fetchCollectorStatus } from "@/api/endpoints/ops";
import { HttpError } from "@/api/http/types";
import { Button } from "@/components/ui/button";
import { Panel, PanelDescription, PanelHeader, PanelTitle } from "@/components/ui/panel";
import { StatusBadge } from "@/components/status-badge";
import {
  EmptyState,
  LoadingState,
  UnknownState,
} from "@/components/states";
import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatRange, formatUtcTimestamp, gapReasonLabel } from "./quality-format";

export const COLLECTOR_GAP_SNAPSHOT_QUERY_KEY = [
  "quality",
  "collector-gap-snapshot",
] as const;

function errorText(error: unknown): string {
  if (error instanceof HttpError) {
    return error.message;
  }
  return error instanceof Error ? error.message : "未知错误";
}

/**
 * Current Contract source of truth: `health.gap_unrecoverable` inside the
 * Collector `/status` snapshot. It is process-local and disappears on restart;
 * this component only ever renders the latest server response and never
 * appends it to a client-side history.
 */
export function CollectorGapSnapshot() {
  const query = useQuery({
    queryKey: COLLECTOR_GAP_SNAPSHOT_QUERY_KEY,
    queryFn: ({ signal }) => fetchCollectorStatus(signal),
    refetchInterval: 5_000,
  });

  const gaps = query.data?.health?.gap_unrecoverable ?? [];
  const serverCount = query.data?.health?.gap_unrecoverable_count;
  const hasError = query.isError;

  return (
    <Panel className="mt-6 p-5" aria-label="Unrecoverable Backfill Gap 当前快照">
      <PanelHeader>
        <div>
          <PanelTitle>Unrecoverable Backfill Gap</PanelTitle>
          <PanelDescription>
            来源：Collector <span className="font-mono">/status</span> ·{" "}
            <span className="font-mono">health.gap_unrecoverable</span>。这是
            Collector 当前进程内快照，重启后丢失；前端只展示最新一次读取，
            不累积客户端历史，也不提供持久化或手动解决。
          </PanelDescription>
        </div>
        {hasError ? (
          <StatusBadge tone="unknown">未知</StatusBadge>
        ) : gaps.length > 0 ? (
          <StatusBadge tone="warning">
            {gaps.length} 项进程内 Gap
          </StatusBadge>
        ) : query.isSuccess ? (
          <StatusBadge tone="info">进程内快照</StatusBadge>
        ) : (
          <StatusBadge tone="unknown">读取中</StatusBadge>
        )}
      </PanelHeader>

      <div className="mt-3 flex flex-wrap items-center justify-between gap-2">
        <p className="text-xs text-muted-foreground">
          {query.isSuccess ? (
            <>
              Collector 上报计数{" "}
              <span className="font-mono">gap_unrecoverable_count</span>：
              {serverCount ?? "未知"} · 本次读取{" "}
              {formatUtcTimestamp(new Date(query.dataUpdatedAt).toISOString())}
            </>
          ) : (
            "尚未取得可确认的 Collector 快照。"
          )}
        </p>
        <Button
          size="sm"
          variant="outline"
          onClick={() => void query.refetch()}
          disabled={query.isFetching}
          aria-label="刷新快照"
        >
          <RefreshCw
            className={query.isFetching ? "animate-spin" : undefined}
            aria-hidden="true"
          />
          刷新快照
        </Button>
      </div>

      {query.isPending ? (
        <LoadingState label="正在读取 Collector 进程内快照…" />
      ) : query.isError ? (
        <UnknownState
          cause="failed"
          reason={
            <span>
              无法读取 Collector <span className="font-mono">/status</span>（
              {errorText(query.error)}）。当前 Gap 状态未知，不能显示为空或健康。
            </span>
          }
        />
      ) : gaps.length === 0 ? (
        <EmptyState
          title="本次快照没有 gap_unrecoverable 记录"
          description="空只代表当前 Collector 进程内没有记录；该快照在重启后丢失，前端不保留历史。"
        />
      ) : (
        <Table className="mt-3">
          <TableCaption>
            Collector 进程内 Unrecoverable Backfill Gap 快照（非持久历史）
          </TableCaption>
          <TableHeader>
            <TableRow>
              <TableHead>InstrumentId</TableHead>
              <TableHead>Bar Interval</TableHead>
              <TableHead>范围</TableHead>
              <TableHead>原因</TableHead>
              <TableHead>记录时间</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {gaps.map((gap, index) => (
              <TableRow key={`${gap.instrument_id}-${gap.recorded_at}-${index}`}>
                <TableCell className="font-mono">{gap.instrument_id}</TableCell>
                <TableCell className="font-mono">{gap.interval}</TableCell>
                <TableCell className="tabular-nums">
                  {formatRange(gap.from_ts, gap.to_ts)}
                </TableCell>
                <TableCell>
                  <span>{gapReasonLabel(gap.reason)}</span>{" "}
                  <span className="font-mono text-[11px] text-faint-foreground">
                    {gap.reason}
                  </span>
                </TableCell>
                <TableCell className="tabular-nums text-muted-foreground">
                  {formatUtcTimestamp(gap.recorded_at)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </Panel>
  );
}
