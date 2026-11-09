import { useState, type FormEvent } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  fetchBackfillGaps,
  type BackfillGapQuery,
} from "@/api/endpoints/backfill";
import { HttpError } from "@/api/http/types";
import { DisabledAction } from "@/components/disabled-reason";
import { ForbiddenState } from "@/components/permission-gate";
import { Pagination } from "@/components/pagination";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Panel, PanelDescription, PanelHeader, PanelTitle } from "@/components/ui/panel";
import { Select } from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { BAR_INTERVALS, type BarInterval } from "@/lib/time-range";
import { CAPABILITY_KEYS } from "@/runtime/capability-gates";
import { queryErrorInfo } from "@/lib/errors";
import {
  formatRange,
  formatUtcTimestamp,
  backfillGapReasonLabel,
} from "./quality-format";
import { useQualityGate } from "./quality-gates";

interface BackfillGapFilters {
  status: "" | "open" | "resolved";
  feed: string;
  instrumentId: string;
  interval: "" | BarInterval;
  reason: string;
}

const EMPTY_BACKFILL_GAP_FILTERS: BackfillGapFilters = {
  status: "",
  feed: "",
  instrumentId: "",
  interval: "",
  reason: "",
};

function toQuery(
  filters: BackfillGapFilters,
  cursor: string | null,
): BackfillGapQuery {
  return {
    status: filters.status || undefined,
    feed: filters.feed || undefined,
    instrument_id: filters.instrumentId || undefined,
    interval: filters.interval || undefined,
    reason: filters.reason || undefined,
    cursor: cursor ?? undefined,
    limit: 50,
  };
}

function linkedJobs(gap: {
  resolved_by_job_id: string | null;
  related_job_ids: string[];
}): string {
  if (gap.resolved_by_job_id) {
    return `已由 ${gap.resolved_by_job_id} 解决`;
  }
  if (gap.related_job_ids.length > 0) {
    return `关联：${gap.related_job_ids.join(", ")}`;
  }
  return "—";
}

/**
 * Persistent Unrecoverable Backfill Gap — Admin API only. Filters are sent to
 * the server; the client never fetches a page and filters it locally. There is
 * no physical delete and no manual resolve: only a successful Job resolves a
 * Gap.
 */
export function PersistentBackfillGapSection() {
  const gate = useQualityGate(
    "backfill.job.view",
    CAPABILITY_KEYS.persistentBackfillGaps,
  );
  const [draft, setDraft] = useState<BackfillGapFilters>(EMPTY_BACKFILL_GAP_FILTERS);
  const [applied, setApplied] = useState<BackfillGapFilters>(EMPTY_BACKFILL_GAP_FILTERS);
  const [cursor, setCursor] = useState<string | null>(null);

  const query = useQuery({
    queryKey: ["quality", "backfill-gaps", applied, cursor],
    queryFn: ({ signal }) => fetchBackfillGaps(toQuery(applied, cursor), signal),
    enabled: gate.enabled,
  });

  function applyFilters(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setApplied({
      ...draft,
      feed: draft.feed.trim(),
      instrumentId: draft.instrumentId.trim(),
      reason: draft.reason.trim(),
    });
    setCursor(null);
  }

  function resetFilters() {
    setDraft(EMPTY_BACKFILL_GAP_FILTERS);
    setApplied(EMPTY_BACKFILL_GAP_FILTERS);
    setCursor(null);
  }

  const errorInfo = query.isError ? queryErrorInfo(query.error) : null;

  return (
    <Panel className="mt-6 p-5" aria-label="持久 Unrecoverable Backfill Gap">
      <PanelHeader>
        <div>
          <PanelTitle>持久 Unrecoverable Backfill Gap</PanelTitle>
          <PanelDescription>
            来源：Admin API{" "}
            <span className="font-mono">/api/v1/admin/backfill-gaps</span>
            。由服务端持久化，含首次/最近发现、原因、范围、open/resolved
            与关联 Backfill Job；筛选由服务端执行。没有物理删除或手动解决，
            只有成功 Job 才会把 Gap 置为 resolved。
          </PanelDescription>
        </div>
        {gate.enabled ? (
          <StatusBadge tone="info">Admin API 持久数据</StatusBadge>
        ) : null}
      </PanelHeader>

      {!gate.enabled ? (
        <div className="mt-4 flex flex-col gap-2">
          <DisabledAction label="查看持久 Gap" reason={gate.reason} />
          <p className="text-xs text-muted-foreground">
            Current Contract 只提供 Collector 进程内快照；持久 Gap、筛选与
            open/resolved 状态需要 Admin API。这里不会把进程内快照累积成伪历史。
          </p>
        </div>
      ) : (
        <>
          <form
            aria-label="持久 Gap 筛选"
            onSubmit={applyFilters}
            className="mt-4 flex flex-wrap items-end gap-3 border-b border-border pb-3"
          >
            <div className="flex w-32 flex-col gap-1">
              <Label htmlFor="gap-filter-status">Gap 状态</Label>
              <Select
                id="gap-filter-status"
                value={draft.status}
                onChange={(event) =>
                  setDraft({
                    ...draft,
                    status: event.target.value as BackfillGapFilters["status"],
                  })
                }
              >
                <option value="">全部</option>
                <option value="open">open</option>
                <option value="resolved">resolved</option>
              </Select>
            </div>
            <div className="flex w-40 flex-col gap-1">
              <Label htmlFor="gap-filter-feed">Feed</Label>
              <Input
                id="gap-filter-feed"
                value={draft.feed}
                onChange={(event) =>
                  setDraft({ ...draft, feed: event.target.value })
                }
                placeholder="binance_spot"
              />
            </div>
            <div className="flex w-52 flex-col gap-1">
              <Label htmlFor="gap-filter-instrument">InstrumentId</Label>
              <Input
                id="gap-filter-instrument"
                value={draft.instrumentId}
                onChange={(event) =>
                  setDraft({ ...draft, instrumentId: event.target.value })
                }
                placeholder="binance:BTC/USDT"
              />
            </div>
            <div className="flex w-32 flex-col gap-1">
              <Label htmlFor="gap-filter-interval">Bar Interval</Label>
              <Select
                id="gap-filter-interval"
                value={draft.interval}
                onChange={(event) =>
                  setDraft({
                    ...draft,
                    interval: event.target.value as BackfillGapFilters["interval"],
                  })
                }
              >
                <option value="">全部</option>
                {BAR_INTERVALS.map((interval) => (
                  <option key={interval} value={interval}>
                    {interval}
                  </option>
                ))}
              </Select>
            </div>
            <div className="flex w-52 flex-col gap-1">
              <Label htmlFor="gap-filter-reason">原因</Label>
              <Select
                id="gap-filter-reason"
                value={draft.reason}
                onChange={(event) =>
                  setDraft({ ...draft, reason: event.target.value })
                }
              >
                <option value="">全部</option>
                <option value="history_unsupported">
                  历史数据不受支持 (history_unsupported)
                </option>
                <option value="backfill_limit_exceeded">
                  超出补采窗口 (backfill_limit_exceeded)
                </option>
              </Select>
            </div>
            <div className="flex items-center gap-2">
              <Button type="submit" size="sm">
                应用 Gap 筛选
              </Button>
              <Button
                type="button"
                size="sm"
                variant="outline"
                onClick={resetFilters}
              >
                重置 Gap 筛选
              </Button>
            </div>
          </form>

          {query.isPending ? (
            <LoadingState label="正在读取持久 Gap…" />
          ) : query.isError ? (
            errorInfo && query.error instanceof HttpError && query.error.status === 403 ? (
              <ForbiddenState
                message="缺少查看持久 Gap 的权限。"
                requestId={errorInfo.requestId}
              />
            ) : (
              <ErrorState
                title="加载持久 Gap 失败"
                description={errorInfo?.message}
                requestId={errorInfo?.requestId}
                onRetry={() => void query.refetch()}
              />
            )
          ) : (query.data?.items.length ?? 0) === 0 ? (
            <EmptyState
              title="没有匹配的持久 Gap"
              description="筛选由服务端执行；空结果不代表数据健康。Gap 只会由成功 Job 解决。"
            />
          ) : (
            <>
              <Table className="mt-3">
                <TableCaption>
                  持久 Unrecoverable Backfill Gap（Admin API 数据）
                </TableCaption>
                <TableHeader>
                  <TableRow>
                    <TableHead>Gap</TableHead>
                    <TableHead>状态</TableHead>
                    <TableHead>Feed</TableHead>
                    <TableHead>InstrumentId / Bar Interval</TableHead>
                    <TableHead>范围</TableHead>
                    <TableHead>原因</TableHead>
                    <TableHead>首次 / 最近发现</TableHead>
                    <TableHead className="text-right">次数</TableHead>
                    <TableHead>关联 Backfill Job</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {query.data?.items.map((gap) => (
                    <TableRow key={gap.gap_id}>
                      <TableCell className="font-mono">{gap.gap_id}</TableCell>
                      <TableCell>
                        <StatusBadge
                          tone={gap.status === "resolved" ? "good" : "warning"}
                        >
                          {gap.status}
                        </StatusBadge>
                      </TableCell>
                      <TableCell>{gap.feed}</TableCell>
                      <TableCell>
                        <div className="font-mono">{gap.instrument_id}</div>
                        <div className="font-mono text-[11px] text-faint-foreground">
                          {gap.interval}
                        </div>
                      </TableCell>
                      <TableCell className="tabular-nums">
                        {formatRange(gap.start, gap.end)}
                      </TableCell>
                      <TableCell>
                        <span>{backfillGapReasonLabel(gap.reason)}</span>{" "}
                        <span className="font-mono text-[11px] text-faint-foreground">
                          {gap.reason}
                        </span>
                      </TableCell>
                      <TableCell className="tabular-nums">
                        <div>首次 {formatUtcTimestamp(gap.first_discovered_at)}</div>
                        <div className="text-faint-foreground">
                          最近 {formatUtcTimestamp(gap.last_discovered_at)}
                        </div>
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {gap.occurrence_count} 次
                      </TableCell>
                      <TableCell className="font-mono text-[11px]">
                        {linkedJobs(gap)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>

              <div className="mt-3 flex flex-wrap items-center justify-between gap-2">
                <Pagination
                  hasMore={query.data?.page.has_more ?? false}
                  onNext={
                    query.data?.page.next_cursor
                      ? () => setCursor(query.data?.page.next_cursor ?? null)
                      : undefined
                  }
                  label="游标分页 · 服务端筛选"
                />
                <span className="text-xs text-faint-foreground">
                  Cursor 由服务端生成，前端不解析；Gap 永不物理删除。
                </span>
              </div>
            </>
          )}
        </>
      )}
    </Panel>
  );
}
