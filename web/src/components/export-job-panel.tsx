import { useInfiniteQuery, useMutation } from "@tanstack/react-query";
import { Download, RefreshCw, XCircle } from "lucide-react";
import { useId, useState } from "react";
import type { CapabilityState } from "@/api/capabilities";
import type { ExportJob, ExportJobStatus } from "@/api/endpoints/admin";
import {
  cancelExportJob,
  createExportJob,
  effectiveExportJobStatus,
  exportJobDownloadPath,
  isActiveExportJob,
  listExportJobs,
} from "@/api/endpoints/export-jobs";
import { HttpError } from "@/api/http/types";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { ErrorState, LoadingState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Panel, PanelDescription, PanelHeader, PanelTitle } from "@/components/ui/panel";
import type {
  ExportPathDecision,
  ExportRangeChunk,
} from "@/lib/export-policy";
import { formatBytes } from "@/lib/format";
import {
  adminCapabilityAvailability,
  CAPABILITY_KEYS,
} from "@/runtime/capability-gates";
import {
  disabledReasonFor,
  type PermissionSubject,
} from "@/runtime/permissions";
import { cn } from "@/lib/utils";
import type { BarInterval } from "@/lib/time-range";
import { toQueryInstant } from "@/lib/time-range";

/**
 * CONTRACT TARGET — Admin Export Job UI.
 *
 * The current backend has no `/api/v1/admin/export-jobs`. This component is
 * exercised against MSW fixtures only and must never be reported as real
 * backend integration. It never falls back from an unavailable job to a huge
 * synchronous Blob.
 */

export interface ExportJobQuery {
  instrumentId: string;
  interval: BarInterval;
  start: Date;
  end: Date;
}

export interface ExportJobPanelProps {
  query: ExportJobQuery | null;
  decision: ExportPathDecision;
  subject: PermissionSubject;
  /** Full runtime capability state; the shared gate reads mode/version/key. */
  capability?: CapabilityState | null;
  /** Session subject; used only for the per-user "one running job" hint. */
  currentSubject?: string | null;
  className?: string;
  /** Deterministic clock for tests. */
  now?: Date;
}

const STATUS_COPY: Record<
  ExportJobStatus,
  { tone: "good" | "warning" | "critical" | "unknown" | "info"; label: string }
> = {
  queued: { tone: "info", label: "排队中" },
  running: { tone: "info", label: "生成中" },
  succeeded: { tone: "good", label: "已完成" },
  failed: { tone: "critical", label: "失败" },
  cancelling: { tone: "warning", label: "取消中" },
  cancelled: { tone: "unknown", label: "已取消" },
  expired: { tone: "warning", label: "已过期" },
};

function quotaMessage(error: HttpError): string | null {
  if (error.code !== "quota_exceeded") {
    return null;
  }
  const quota =
    typeof error.details.quota === "string" ? error.details.quota : "running_jobs";
  const limit =
    typeof error.details.limit === "number" ? error.details.limit : null;
  const retryAfter =
    typeof error.details.retry_after_s === "number"
      ? `，请 ${error.details.retry_after_s} 秒后重试`
      : "";
  return `配额已用尽（${quota}${limit !== null ? `，上限 ${limit}` : ""}）${retryAfter}。`;
}

function JobProgress({ job }: { job: ExportJob }) {
  const { progress } = job;
  if (progress.indeterminate || progress.percent === null) {
    return (
      <div className="mt-2 text-xs text-muted-foreground">
        <p className="font-medium">进度未知</p>
        <p>
          已处理 {progress.processed_rows.toLocaleString("en-US")} 行 /{" "}
          {formatBytes(progress.processed_bytes)}
          {progress.total_rows !== null
            ? `；后端总量 ${progress.total_rows.toLocaleString("en-US")} 行`
            : "；后端未提供可靠总量"}
        </p>
      </div>
    );
  }
  return (
    <div className="mt-2 text-xs text-muted-foreground">
      <progress
        className="h-2 w-full max-w-md"
        max={100}
        value={progress.percent}
      >
        {progress.percent}%
      </progress>
      <p className="mt-1">
        {progress.percent}% · {progress.processed_rows.toLocaleString("en-US")}
        {progress.total_rows !== null
          ? ` / ${progress.total_rows.toLocaleString("en-US")}`
          : ""}{" "}
        行 · {formatBytes(progress.processed_bytes)}
      </p>
    </div>
  );
}

function ExportJobItem({
  job,
  now,
  cancelling,
  onCancel,
}: {
  job: ExportJob;
  now: Date;
  cancelling: boolean;
  onCancel: (jobId: string) => void;
}) {
  const status = effectiveExportJobStatus(job, now);
  const copy = STATUS_COPY[status];
  const expired = status === "expired";
  const canCancel = isActiveExportJob(job.status) && status !== "cancelling";
  const downloadable = status === "succeeded" && job.result !== null;

  return (
    <article
      data-job-id={job.job_id}
      className="border border-border bg-card p-4"
      aria-label={`Export Job ${job.job_id}`}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="font-mono text-sm font-medium">{job.job_id}</p>
          <p className="mt-1 text-xs text-muted-foreground">
            <span className="font-mono">{job.instrument_id}</span> ·{" "}
            {job.interval} · {job.range_days} 天
          </p>
        </div>
        <StatusBadge tone={copy.tone}>{copy.label}</StatusBadge>
      </div>

      <dl className="mt-3 grid grid-cols-1 gap-x-4 gap-y-1 text-xs sm:grid-cols-2">
        <div>
          <dt className="text-faint-foreground">范围（start 闭 / end 开）</dt>
          <dd className="font-mono">
            {job.start} → {job.end}
          </dd>
        </div>
        <div>
          <dt className="text-faint-foreground">创建者</dt>
          <dd>{job.created_by.display_name}</dd>
        </div>
        <div>
          <dt className="text-faint-foreground">创建时间</dt>
          <dd className="font-mono">{job.created_at}</dd>
        </div>
        <div>
          <dt className="text-faint-foreground">24 小时过期</dt>
          <dd className="font-mono">{job.expires_at ?? "未生成文件"}</dd>
        </div>
      </dl>

      <JobProgress job={job} />

      {job.result ? (
        <div className="mt-3 border-t border-border pt-2 text-xs">
          <p>
            文件：<span className="font-mono">{job.result.file_name}</span> ·{" "}
            {formatBytes(job.result.size_bytes)} ·{" "}
            {job.result.row_count.toLocaleString("en-US")} 行
          </p>
          <p className="mt-1 break-all">
            SHA-256：<span className="font-mono">{job.result.sha256}</span>
          </p>
        </div>
      ) : null}

      {job.error ? (
        <p className="mt-2 text-xs text-status-critical">
          失败：{job.error.code} · {job.error.message}
        </p>
      ) : null}

      <div className="mt-3 flex flex-wrap items-center gap-2">
        {downloadable ? (
          <a
            href={exportJobDownloadPath(job)}
            className="inline-flex h-8 items-center gap-1.5 rounded-md border border-input bg-card px-2.5 text-xs font-medium hover:bg-muted"
          >
            <Download className="size-3.5" aria-hidden="true" />
            原生下载（支持 Range）
          </a>
        ) : null}
        {expired ? (
          <span className="text-xs text-status-warning">
            文件已过期（24 小时）；下载将返回 410 export_expired。
          </span>
        ) : null}
        {canCancel ? (
          <Button
            size="sm"
            variant="outline"
            disabled={cancelling}
            onClick={() => onCancel(job.job_id)}
          >
            <XCircle aria-hidden="true" />
            {cancelling ? "请求取消中…" : "取消任务"}
          </Button>
        ) : null}
        {job.status === "cancelling" ? (
          <span className="text-xs text-muted-foreground">
            合作式取消进行中；未到终态前不宣称已取消。
          </span>
        ) : null}
        {job.status === "succeeded" && expired ? (
          <span className="text-xs text-muted-foreground">
            服务端仍显示 succeeded，但 expires_at 已过，按已过期处理。
          </span>
        ) : null}
      </div>
    </article>
  );
}

export function ExportJobPanel({
  query,
  decision,
  subject,
  capability,
  currentSubject = null,
  className,
  now,
}: ExportJobPanelProps) {
  const reasonId = useId();
  const role = subject.kind === "role" ? subject.role : null;
  const roleReason = disabledReasonFor(subject, "export.job.create");
  const availability = adminCapabilityAvailability(
    capability ?? null,
    CAPABILITY_KEYS.exportJob,
  );
  const capabilityReason =
    subject.kind === "role" && availability.kind === "disabled"
      ? availability.reason
      : null;
  const blockedReason = roleReason ?? capabilityReason;
  const jobsEnabled = role !== null && blockedReason === null;
  const currentTime = now ?? new Date();

  const jobsQuery = useInfiniteQuery({
    queryKey: ["admin", "export-jobs"],
    queryFn: ({ pageParam, signal }) =>
      listExportJobs(
        {
          cursor: pageParam,
          limit: 50,
        },
        signal,
      ),
    initialPageParam: null as string | null,
    getNextPageParam: (last) =>
      last.page.has_more && last.page.next_cursor
        ? last.page.next_cursor
        : undefined,
    enabled: jobsEnabled,
    // Poll while a server-reported job is still queued/running/cancelling.
    // Terminal states stop polling; expired is derived from expires_at.
    refetchInterval: (query) => {
      const pages = query.state.data?.pages ?? [];
      const active = pages
        .flatMap((page) => page.items)
        .some((job) =>
          isActiveExportJob(effectiveExportJobStatus(job, currentTime)),
        );
      return active ? 3000 : false;
    },
  });

  const jobs =
    jobsQuery.data?.pages.flatMap((page) => page.items) ?? [];
  // The server enforces the quota. This hint only reflects the current
  // subject's own jobs, because SuperAdmin sees everyone's jobs but may still
  // create their own (subject to the server's quota/global limits).
  const hasActiveJob = jobs.some(
    (job) =>
      isActiveExportJob(effectiveExportJobStatus(job, currentTime)) &&
      (currentSubject === null ||
        job.created_by.subject === currentSubject),
  );

  const [pendingChunk, setPendingChunk] = useState<{
    index: number;
    chunk: ExportRangeChunk;
  } | null>(null);
  const [createdChunkStarts, setCreatedChunkStarts] = useState<
    ReadonlySet<string>
  >(() => new Set());

  const createMutation = useMutation({
    mutationFn: (range: ExportRangeChunk) => {
      if (!query) {
        throw new Error("missing query");
      }
      return createExportJob({
        instrument_id: query.instrumentId,
        interval: query.interval,
        start: toQueryInstant(range.start),
        end: toQueryInstant(range.end),
        format: "csv",
      });
    },
    onSuccess: (_job, range) => {
      if (decision.kind === "split") {
        setCreatedChunkStarts((previous) => {
          const next = new Set(previous);
          next.add(range.start.toISOString());
          return next;
        });
      }
      void jobsQuery.refetch();
    },
  });

  const cancelMutation = useMutation({
    mutationFn: (jobId: string) => cancelExportJob(jobId),
    onSuccess: () => {
      void jobsQuery.refetch();
    },
  });

  let createDisabledMessage: string | null = null;
  if (decision.kind === "direct") {
    createDisabledMessage =
      "≤31 个 UTC 天：请使用上方小文件 CSV，不需要 Export Job。";
  } else if (decision.kind === "invalid") {
    createDisabledMessage = decision.message;
  } else if (decision.kind === "split") {
    createDisabledMessage = `超过 366 个 UTC 天：需拆分为 ${decision.chunkCount} 个 Export Job，不能创建单个任务。`;
  } else if (blockedReason) {
    createDisabledMessage = blockedReason.message;
  } else if (!query) {
    createDisabledMessage = "请先选择 InstrumentId、Bar Interval 与时间范围。";
  } else if (hasActiveJob) {
    createDisabledMessage =
      "已有运行中的 Export Job；每个用户默认最多 1 个（queued / running / cancelling）。";
  }

  const createEnabled =
    jobsEnabled && decision.kind === "job" && query !== null && !hasActiveJob;

  /**
   * >366-day ranges may only be created one chunk at a time. The server owns
   * the per-user one-running-job quota; this gate avoids a second request
   * while one is in flight, one is running, or the chunk was already created
   * in this session.
   */
  function splitChunkBlockReason(chunk: ExportRangeChunk): string | null {
    if (blockedReason) {
      return blockedReason.message;
    }
    if (!query) {
      return "请先选择 InstrumentId、Bar Interval 与时间范围。";
    }
    if (hasActiveJob) {
      return "已有运行中的 Export Job；每个用户默认最多 1 个（queued / running / cancelling）。";
    }
    if (createMutation.isPending) {
      return "正在创建 Export Job；请等待当前任务进入运行状态后再创建下一段。";
    }
    if (createdChunkStarts.has(chunk.start.toISOString())) {
      return "该段已创建 Export Job；不会重复创建。";
    }
    return null;
  }

  const splitCreateEnabled =
    jobsEnabled && query !== null && !hasActiveJob && !createMutation.isPending;

  const createError =
    createMutation.error instanceof HttpError ? createMutation.error : null;
  const cancelError =
    cancelMutation.error instanceof HttpError ? cancelMutation.error : null;
  const quota = createError ? quotaMessage(createError) : null;

  return (
    <Panel className={cn("mt-6 p-5", className)} aria-label="Export Job">
      <PanelHeader>
        <div>
          <PanelTitle>Admin Export Job（31–366 天）</PanelTitle>
          <PanelDescription>
            31–366 个 UTC 天只创建服务端 Export Job；前端不读取 Blob，不重新序列化
            CSV，完成后使用浏览器原生下载（服务端支持 Range）。不会自动退回超大 Blob。
            不支持多 Instrument ZIP。
          </PanelDescription>
        </div>
        <span className="text-xs text-faint-foreground">
          {decision.message}
        </span>
      </PanelHeader>

      <p className="mt-2 text-[11px] text-faint-foreground">
        Admin API 契约目标：当前后端未实现时保持禁用并显示原因；本面板的契约测试不计为真实后端联调。
      </p>

      <div className="mt-4 flex flex-wrap items-center gap-2">
        <Button
          disabled={!createEnabled}
          aria-describedby={createDisabledMessage ? reasonId : undefined}
          onClick={() => {
            if (query) {
              createMutation.mutate({ start: query.start, end: query.end });
            }
          }}
        >
          {createMutation.isPending ? "创建中…" : "创建 Export Job"}
        </Button>
        {createDisabledMessage ? (
          <span id={reasonId} className="text-xs text-faint-foreground">
            {createDisabledMessage}
          </span>
        ) : null}
        {jobsEnabled ? (
          <Button
            size="sm"
            variant="outline"
            onClick={() => void jobsQuery.refetch()}
            disabled={jobsQuery.isFetching}
          >
            <RefreshCw aria-hidden="true" />
            刷新任务列表
          </Button>
        ) : null}
      </div>

      {decision.kind === "split" ? (
        <div className="mt-3 border-l-2 border-status-warning pl-2 text-xs">
          <p className="font-medium text-status-warning">
            需要拆分为 {decision.chunkCount} 个 Export Job（每段 ≤366 天）。
            逐段创建；一次只能有一个运行中的 Job，配额由服务端执行。
          </p>
          <p className="mt-1 text-muted-foreground">
            每段是独立的单 Instrument Export Job；不会合并为多 Instrument
            ZIP，也不会自动拼接 CSV。
          </p>
          <ul
            data-testid="export-split-chunks"
            className="mt-2 flex max-h-72 flex-col gap-1 overflow-auto pr-1"
          >
            {decision.chunks.map((chunk, index) => {
              const blockReason = splitChunkBlockReason(chunk);
              const reasonId = `export-chunk-${index}-reason`;
              return (
                <li
                  key={chunk.start.toISOString()}
                  className="flex flex-wrap items-center gap-2"
                >
                  <span className="min-w-0 flex-1 font-mono text-muted-foreground">
                    第 {index + 1} / {decision.chunkCount} 段：{" "}
                    {chunk.start.toISOString()} → {chunk.end.toISOString()}
                  </span>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={!splitCreateEnabled || blockReason !== null}
                    aria-describedby={blockReason ? reasonId : undefined}
                    onClick={() => setPendingChunk({ index, chunk })}
                  >
                    创建第 {index + 1} 段 Export Job
                  </Button>
                  {blockReason ? (
                    <span
                      id={reasonId}
                      className="w-full text-[11px] text-faint-foreground"
                    >
                      {blockReason}
                    </span>
                  ) : null}
                </li>
              );
            })}
          </ul>
        </div>
      ) : null}

      <div className="mt-3 text-xs text-muted-foreground">
        {role === "SuperAdmin"
          ? "可见范围：全部 Export Job（服务端按角色过滤）。"
          : role !== null
            ? "可见范围：仅本人 Export Job（服务端按所有权过滤；前端不做安全过滤）。"
            : "未分配 Access Role；Export Job 只在 Admin API 模式可用。"}
      </div>

      {quota ? (
        <p role="alert" className="mt-3 text-xs text-status-critical">
          {quota}
        </p>
      ) : null}

      {createError ? (
        <ErrorState
          className="mt-3"
          title="Export Job 创建失败"
          description={
            <>
              <div className="font-mono text-[11px]">{createError.code}</div>
              <div>{quota ?? createError.message}</div>
            </>
          }
          requestId={createError.requestId}
        />
      ) : null}

      {cancelError ? (
        <ErrorState
          className="mt-3"
          title="取消 Export Job 失败"
          description={
            <>
              <div className="font-mono text-[11px]">{cancelError.code}</div>
              <div>
                {cancelError.code === "conflict"
                  ? "任务已不在可取消状态；请刷新后按实际终态处理。"
                  : cancelError.message}
              </div>
            </>
          }
          requestId={cancelError.requestId}
        />
      ) : null}

      {jobsEnabled ? (
        <div className="mt-4 flex flex-col gap-3">
          {jobsQuery.isPending ? <LoadingState label="正在加载 Export Job…" /> : null}
          {jobsQuery.isError ? (
            <ErrorState
              title="Export Job 列表加载失败"
              description={
                jobsQuery.error instanceof HttpError
                  ? jobsQuery.error.message
                  : "无法读取 Export Job 列表。"
              }
              requestId={
                jobsQuery.error instanceof HttpError
                  ? jobsQuery.error.requestId
                  : null
              }
              onRetry={() => void jobsQuery.refetch()}
            />
          ) : null}
          {jobs.length === 0 && !jobsQuery.isPending ? (
            <p className="text-xs text-muted-foreground">
              当前没有可见的 Export Job。
            </p>
          ) : null}
          {jobs.map((job) => (
            <ExportJobItem
              key={job.job_id}
              job={job}
              now={currentTime}
              cancelling={
                cancelMutation.isPending &&
                cancelMutation.variables === job.job_id
              }
              onCancel={(jobId) => cancelMutation.mutate(jobId)}
            />
          ))}
          {jobsQuery.hasNextPage ? (
            <Button
              size="sm"
              variant="outline"
              onClick={() => void jobsQuery.fetchNextPage()}
              disabled={jobsQuery.isFetchingNextPage}
            >
              {jobsQuery.isFetchingNextPage ? "加载中…" : "加载更多任务"}
            </Button>
          ) : null}
        </div>
      ) : (
        <p className="mt-3 text-xs text-faint-foreground">
          {blockedReason?.code === "requires-admin-api"
            ? "需要 Admin API：当前只读服务没有异步 Export Job；禁用该控件，且不会退回超大 Blob。"
            : blockedReason?.message}
        </p>
      )}

      <ConfirmDialog
        open={pendingChunk !== null}
        onOpenChange={(open) => {
          if (!open) {
            setPendingChunk(null);
          }
        }}
        title={
          pendingChunk
            ? `确认创建第 ${pendingChunk.index + 1} 段 Export Job`
            : "确认创建 Export Job"
        }
        description={
          pendingChunk ? (
            <span>
              第 {pendingChunk.index + 1} / {decision.chunkCount} 段：
              <span className="font-mono">
                {" "}
                {pendingChunk.chunk.start.toISOString()} →{" "}
                {pendingChunk.chunk.end.toISOString()}
              </span>
              。将创建一个独立的单 Instrument Export Job；不会合并为多
              Instrument ZIP。每段 ≤366 天，创建后需等待其不再运行才能创建下一段。
            </span>
          ) : undefined
        }
        confirmLabel="确认创建该段"
        submitting={createMutation.isPending}
        requireReason={false}
        onConfirm={() => {
          if (!pendingChunk) {
            return;
          }
          const chunk = pendingChunk.chunk;
          setPendingChunk(null);
          createMutation.mutate(chunk);
        }}
      />
    </Panel>
  );
}
