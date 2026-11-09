import { RefreshCw } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { fetchCollectorStatus } from "@/api/endpoints/ops";
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
import { errorText } from "@/lib/errors";
import { STATUS_POLL_INTERVAL_MS } from "@/lib/monitoring-config";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import {
  formatRange,
  formatUtcTimestamp,
  backfillGapReasonLabel,
} from "./quality-format";

export const COLLECTOR_BACKFILL_GAP_SNAPSHOT_QUERY_KEY = [
  "quality",
  "collector-gap-snapshot",
] as const;

const POLL_SECONDS = STATUS_POLL_INTERVAL_MS / 1000;

/**
 * Current Contract source of truth: `health.gap_unrecoverable` inside the
 * Collector `/status` snapshot. It is process-local and disappears on restart;
 * this component only ever renders the latest server response and never
 * appends it to a client-side history.
 *
 * The Collector `/status` endpoint is unauthenticated and Admin mode denies it,
 * so the query is enabled only in Current Contract. Admin mode renders an
 * explicit note pointing to the persistent Backfill Gap list; capabilities
 * unknown renders unknown without issuing any request.
 */
export function CollectorBackfillGapSnapshot() {
  const { capability } = useRuntimeMode();
  const mode = capability?.mode ?? null;
  const directEnabled = mode === "current-contract";
  const query = useQuery({
    queryKey: COLLECTOR_BACKFILL_GAP_SNAPSHOT_QUERY_KEY,
    queryFn: ({ signal }) => fetchCollectorStatus(signal),
    refetchInterval: STATUS_POLL_INTERVAL_MS,
    enabled: directEnabled,
  });

  const gaps = directEnabled ? (query.data?.health?.gap_unrecoverable ?? []) : [];
  const serverCount = query.data?.health?.gap_unrecoverable_count;
  const hasError = directEnabled && query.isError;

  const badge = (() => {
    if (mode === null) {
      return <StatusBadge tone="unknown">发现能力中</StatusBadge>;
    }
    if (mode === "capabilities-unknown") {
      return <StatusBadge tone="warning">未知</StatusBadge>;
    }
    if (mode === "admin-api") {
      return <StatusBadge tone="warning">需要 Current Contract</StatusBadge>;
    }
    if (hasError) {
      return <StatusBadge tone="unknown">未知</StatusBadge>;
    }
    if (gaps.length > 0) {
      return (
        <StatusBadge tone="warning">{gaps.length} 项进程内 Gap</StatusBadge>
      );
    }
    if (query.isSuccess) {
      return <StatusBadge tone="info">进程内快照</StatusBadge>;
    }
    return <StatusBadge tone="unknown">读取中</StatusBadge>;
  })();

  return (
    <Panel className="mt-6 p-5" aria-label="Unrecoverable Backfill Gap 当前快照">
      <PanelHeader>
        <div>
          <PanelTitle>Unrecoverable Backfill Gap</PanelTitle>
          <PanelDescription>
            来源：Collector <span className="font-mono">/status</span> ·{" "}
            <span className="font-mono">health.gap_unrecoverable</span>。这是
            Collector 当前进程内快照，重启后丢失；前端只展示最新一次读取，
            不累积客户端历史，也不提供持久化或手动解决。仅在 Current Contract
            模式可用；每 {POLL_SECONDS} 秒轮询。
          </PanelDescription>
        </div>
        {badge}
      </PanelHeader>

      {mode === null ? (
        <LoadingState label="正在发现运行时能力…" />
      ) : mode === "capabilities-unknown" ? (
        <UnknownState
          cause="not-loaded"
          reason="能力未知，暂不可用。能力发现失败；未向 Collector /status 发出任何请求。"
        />
      ) : mode === "admin-api" ? (
        <div
          role="note"
          className="mt-3 border-l-2 border-status-warning pl-3 text-sm"
        >
          <p className="font-medium">Collector 进程内快照仅在 Current Contract 模式可用</p>
          <p className="mt-1 text-xs text-muted-foreground">
            Admin API 模式不读取无认证的{" "}
            <span className="font-mono">/ops/collector/status</span>。请查看下方
            “持久 Unrecoverable Backfill Gap”（Admin API 数据）；
            进程内快照不是持久历史，不能替代持久 Gap 列表。
          </p>
        </div>
      ) : (
        <>
          <div className="mt-3 flex flex-wrap items-center justify-between gap-2">
            <p className="text-xs text-muted-foreground">
              {query.isSuccess ? (
                <>
                  Collector 上报计数{" "}
                  <span className="font-mono">gap_unrecoverable_count</span>：
                  {serverCount ?? "未知"} · 本次读取{" "}
                  {formatUtcTimestamp(
                    new Date(query.dataUpdatedAt).toISOString(),
                  )}
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
                  无法读取 Collector <span className="font-mono">/status</span>
                  （{errorText(query.error)}）。当前 Gap 状态未知，不能显示为空或健康。
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
                  <TableRow
                    key={`${gap.instrument_id}-${gap.recorded_at}-${index}`}
                  >
                    <TableCell className="font-mono">
                      {gap.instrument_id}
                    </TableCell>
                    <TableCell className="font-mono">{gap.interval}</TableCell>
                    <TableCell className="tabular-nums">
                      {formatRange(gap.from_ts, gap.to_ts)}
                    </TableCell>
                    <TableCell>
                      <span>{backfillGapReasonLabel(gap.reason)}</span>{" "}
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
        </>
      )}
    </Panel>
  );
}
