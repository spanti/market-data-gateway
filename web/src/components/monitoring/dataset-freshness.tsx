import { SourceStatus, type SourceStatusState } from "@/components/monitoring/source-status";
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
import type { DatasetFreshnessRow } from "@/lib/freshness";
import {
  formatBytes,
  formatDurationSeconds,
  formatUtcDateTime,
} from "@/lib/format";
import { cn } from "@/lib/utils";

export interface DatasetFreshnessProps {
  rows: DatasetFreshnessRow[];
  state: SourceStatusState;
  lastSuccessAt: number | null;
  errorMessage?: string | null;
  className?: string;
}

/**
 * Dataset Freshness is separate from Feed Freshness. The contract exposes a
 * flush timestamp per interval but no Dataset threshold, so this section never
 * claims a row is stale or healthy.
 */
export function DatasetFreshness({
  rows,
  state,
  lastSuccessAt,
  errorMessage,
  className,
}: DatasetFreshnessProps) {
  return (
    <section
      aria-labelledby="dataset-freshness-title"
      className={cn("mt-7 border-y border-border bg-card py-4", className)}
    >
      <PanelHeader className="px-4">
        <div>
          <PanelTitle id="dataset-freshness-title">Dataset Freshness</PanelTitle>
          <PanelDescription>
            来自 /api/v1/status.intervals.last_flushed_ts · 独立于 Feed
            Freshness · 契约未提供 Dataset 阈值，不判定 stale。
          </PanelDescription>
        </div>
      </PanelHeader>
      <div className="mt-2 px-4">
        <SourceStatus
          source="Dataset /api/v1/status"
          window="当前快照（5 秒轮询）"
          state={state}
          lastSuccessAt={lastSuccessAt}
          reason={
            state === "error"
              ? errorMessage ?? "Dataset status 不可用"
              : null
          }
        />
      </div>
      <div className="mt-2">
        {state === "loading" ? (
          <LoadingState label="正在读取 Dataset status…" showSkeleton />
        ) : null}
        {state === "error" ? (
          <ErrorState
            description={
              <span>
                Dataset /api/v1/status 加载失败：
                {errorMessage ?? "数据源不可用"}。最后成功：
                {formatUtcDateTime(lastSuccessAt)}。
              </span>
            }
          />
        ) : null}
        {state === "ready" && rows.length === 0 ? (
          <EmptyState
            title="暂无 Dataset Freshness 数据"
            description="Dataset 尚无 flush 记录；不生成健康记录，也不推断 Feed 状态。"
          />
        ) : null}
        {state === "ready" && rows.length > 0 ? (
          <Table>
            <caption className="sr-only">
              Dataset Freshness，按 Bar Interval 展示最近 flush 时间、客户端计算的年龄、活动文件数与文件大小中位数
            </caption>
            <TableHeader>
              <TableRow>
                <TableHead>Bar Interval</TableHead>
                <TableHead>Last Flushed</TableHead>
                <TableHead className="text-right">Age（客户端计算）</TableHead>
                <TableHead className="text-right">Active Files</TableHead>
                <TableHead className="text-right">Median File Size</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <TableRow key={row.interval}>
                  <TableCell className="font-mono">{row.interval}</TableCell>
                  <TableCell className="whitespace-nowrap font-mono text-[11px]">
                    {formatUtcDateTime(row.lastFlushedTs)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {formatDurationSeconds(row.ageS)}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {row.activeFileCount}
                  </TableCell>
                  <TableCell className="text-right tabular-nums">
                    {formatBytes(row.medianFileSizeBytes)}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        ) : null}
      </div>
    </section>
  );
}
