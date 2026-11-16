import { zodResolver } from "@hookform/resolvers/zod";
import { useInfiniteQuery, useMutation } from "@tanstack/react-query";
import { Download, RefreshCw, XCircle } from "lucide-react";
import { useEffect, useId, useState } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";
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
import { DisabledReason } from "@/components/disabled-reason";
import { HelpTrigger } from "@/components/help-trigger";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import type {
  ExportPathDecision,
  ExportRangeChunk,
} from "@/lib/export-policy";
import { formatBytes } from "@/lib/format";
import { cn } from "@/lib/utils";
import { ACTIVE_JOB_POLL_INTERVAL_MS } from "@/lib/monitoring-config";
import {
  adminCapabilityAvailability,
  CAPABILITY_KEYS,
} from "@/runtime/capability-gates";
import {
  disabledReasonFor,
  type DisabledReason as DisabledReasonValue,
  type PermissionSubject,
} from "@/runtime/permissions";
import { useRetryAfter } from "@/runtime/use-retry-after";
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

const PANEL_HELP =
  "31 到 366 个 UTC 天只创建服务端 Export Job。异步能力不可用时保持禁用，不会自动退回超大 Blob。完成后使用浏览器原生下载，服务端支持 Range；前端不读取 Blob，也不重新序列化 CSV。超过 366 天需逐段创建独立的单 Instrument 任务，不支持多 Instrument ZIP。";

const STATUS_HELP =
  "queued 排队中，running 生成中，succeeded 已完成，failed 失败，cancelling 取消中，cancelled 已取消，expired 已过期。取消是协作式的，未到终态前不宣称已取消。契约未提供可靠百分比时显示进度未知，不渲染百分比；文件生成后 24 小时内可下载，过期后下载返回 410 export_expired。";

function isUnknownOutcome(error: HttpError): boolean {
  return error.code === "network_error" || error.code === "timeout";
}

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
  const processedRows = progress.processed_rows.toLocaleString("en-US");
  const totalRows =
    progress.total_rows !== null
      ? progress.total_rows.toLocaleString("en-US")
      : null;

  if (progress.indeterminate || progress.percent === null) {
    return (
      <div className="mt-3 text-xs" data-testid="export-job-progress">
        <StatusBadge tone="unknown">进度未知</StatusBadge>
        <p className="mt-1.5 flex flex-wrap gap-x-3 gap-y-1 text-muted-foreground">
          <span>已处理 {processedRows} 行</span>
          <span>{formatBytes(progress.processed_bytes)}</span>
          <span>
            {totalRows !== null
              ? `后端总量 ${totalRows} 行`
              : "后端未提供可靠总量"}
          </span>
        </p>
      </div>
    );
  }

  return (
    <div className="mt-3 text-xs" data-testid="export-job-progress">
      <progress
        className="h-2 w-full max-w-md"
        max={100}
        value={progress.percent}
        aria-label={`已完成 ${progress.percent}%`}
      >
        {progress.percent}%
      </progress>
      <p className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-muted-foreground">
        <span>{progress.percent}%</span>
        <span>
          已处理 {processedRows}
          {totalRows !== null ? ` / ${totalRows}` : ""} 行
        </span>
        <span>{formatBytes(progress.processed_bytes)}</span>
      </p>
    </div>
  );
}

function ExportJobItem({
  job,
  now,
  cancelling,
  cancelBlockReason,
  onCancel,
}: {
  job: ExportJob;
  now: Date;
  cancelling: boolean;
  cancelBlockReason: DisabledReasonValue | null;
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
      className="min-w-0 rounded-lg border border-card-border bg-surface-sunken p-4"
      aria-label={`Export Job ${job.job_id}`}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="break-all font-mono text-sm font-medium">{job.job_id}</p>
          <p className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <span className="break-all font-mono">{job.instrument_id}</span>
            <span>{job.interval}</span>
          </p>
        </div>
        <StatusBadge tone={copy.tone}>{copy.label}</StatusBadge>
      </div>

      <dl className="mt-3 grid grid-cols-1 gap-x-4 gap-y-2 text-xs sm:grid-cols-2 xl:grid-cols-4">
        <div className="min-w-0">
          <dt className="text-faint-foreground">范围（start 闭 / end 开）</dt>
          <dd className="mt-0.5 break-all font-mono">
            {job.start} → {job.end}
          </dd>
        </div>
        <div className="min-w-0">
          <dt className="text-faint-foreground">范围天数</dt>
          <dd className="mt-0.5">{job.range_days} 天</dd>
        </div>
        <div className="min-w-0">
          <dt className="text-faint-foreground">创建者</dt>
          <dd className="mt-0.5 break-words">{job.created_by.display_name}</dd>
        </div>
        <div className="min-w-0">
          <dt className="text-faint-foreground">创建时间</dt>
          <dd className="mt-0.5 break-all font-mono">{job.created_at}</dd>
        </div>
        <div className="min-w-0">
          <dt className="text-faint-foreground">24 小时过期</dt>
          <dd className="mt-0.5 break-all font-mono">
            {job.expires_at ?? "未生成文件"}
          </dd>
        </div>
      </dl>

      <JobProgress job={job} />

      {job.result ? (
        <dl className="mt-3 grid grid-cols-1 gap-x-4 gap-y-2 border-t border-card-border pt-3 text-xs sm:grid-cols-2 xl:grid-cols-4">
          <div className="min-w-0">
            <dt className="text-faint-foreground">文件</dt>
            <dd className="mt-0.5 break-all font-mono">
              {job.result.file_name}
            </dd>
          </div>
          <div className="min-w-0">
            <dt className="text-faint-foreground">大小</dt>
            <dd className="mt-0.5">{formatBytes(job.result.size_bytes)}</dd>
          </div>
          <div className="min-w-0">
            <dt className="text-faint-foreground">行数</dt>
            <dd className="mt-0.5">
              {job.result.row_count.toLocaleString("en-US")} 行
            </dd>
          </div>
          <div className="min-w-0">
            <dt className="text-faint-foreground">SHA-256</dt>
            <dd className="mt-0.5 break-all font-mono">{job.result.sha256}</dd>
          </div>
        </dl>
      ) : null}

      {job.error ? (
        <div
          role="alert"
          className="mt-3 rounded-lg bg-status-critical-bg px-3 py-2 text-xs text-status-critical-fg"
        >
          <p className="font-medium">任务失败</p>
          <p className="mt-1 break-all font-mono">{job.error.code}</p>
          <p className="mt-1 break-words" data-raw-evidence="true">
            {job.error.message}
          </p>
        </div>
      ) : null}

      <div className="mt-3 flex flex-wrap items-center gap-2">
        {downloadable ? (
          <Button asChild size="sm" variant="outline">
            <a href={exportJobDownloadPath(job)}>
              <Download aria-hidden="true" />
              原生下载（支持 Range）
            </a>
          </Button>
        ) : null}
        {expired ? (
          <p className="w-full text-xs text-status-warning-fg">
            文件已过期（24 小时）；下载将返回 410 export_expired。
          </p>
        ) : null}
        {canCancel ? (
          <>
            <Button
              size="sm"
              variant="outline"
              disabled={cancelling || cancelBlockReason !== null}
              aria-describedby={
                cancelBlockReason
                  ? `export-cancel-${job.job_id}-reason`
                  : undefined
              }
              onClick={() => onCancel(job.job_id)}
            >
              <XCircle aria-hidden="true" />
              {cancelling ? "请求取消中…" : "取消任务"}
            </Button>
            <DisabledReason
              id={`export-cancel-${job.job_id}-reason`}
              reason={cancelBlockReason}
            />
          </>
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

const splitChunkSchema = z.object({
  chunkIndex: z
    .number({ invalid_type_error: "请选择目标分段" })
    .int("请选择目标分段")
    .min(0, "请选择目标分段"),
  confirmed: z
    .boolean()
    .refine((value) => value, "请确认只创建所选分段"),
});

type SplitChunkFormValues = z.infer<typeof splitChunkSchema>;

function SplitChunkCreateDialog({
  open,
  onOpenChange,
  chunks,
  initialIndex,
  creating,
  onConfirm,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  chunks: readonly ExportRangeChunk[];
  initialIndex: number;
  creating: boolean;
  onConfirm: (index: number) => void;
}) {
  const confirmedId = useId();
  const chunkErrorId = useId();
  const {
    register,
    handleSubmit,
    reset,
    watch,
    formState: { errors, isValid },
  } = useForm<SplitChunkFormValues>({
    resolver: zodResolver(splitChunkSchema),
    mode: "onChange",
    defaultValues: { chunkIndex: initialIndex, confirmed: false },
  });

  useEffect(() => {
    if (open) {
      reset({ chunkIndex: initialIndex, confirmed: false });
    }
  }, [initialIndex, open, reset]);

  const selectedIndex = watch("chunkIndex");
  const selectedChunk = Number.isInteger(selectedIndex)
    ? chunks[selectedIndex]
    : undefined;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>确认创建单独分段 Export Job</DialogTitle>
          <DialogDescription>
            每段是独立的单 Instrument Export Job；不会合并为多 Instrument
            ZIP。每段 ≤366 天，创建后需等待其不再运行才能创建下一段。
          </DialogDescription>
        </DialogHeader>

        <form
          noValidate
          onSubmit={handleSubmit((values) => {
            if (!creating) {
              onConfirm(values.chunkIndex);
            }
          })}
        >
          <div className="mt-4 flex flex-col gap-3">
            <div className="flex flex-col gap-1">
              <Label htmlFor="export-split-chunk">目标分段</Label>
              <Select
                id="export-split-chunk"
                aria-invalid={Boolean(errors.chunkIndex)}
                aria-describedby={
                  errors.chunkIndex ? chunkErrorId : undefined
                }
                {...register("chunkIndex", {
                  setValueAs: (value: string) => Number(value),
                })}
              >
                {chunks.map((chunk, index) => (
                  <option key={chunk.start.toISOString()} value={index}>
                    第 {index + 1} / {chunks.length} 段{" "}
                    {chunk.start.toISOString()} → {chunk.end.toISOString()}
                  </option>
                ))}
              </Select>
              {errors.chunkIndex?.message ? (
                <p
                  id={chunkErrorId}
                  role="alert"
                  className="text-[11px] text-status-critical"
                >
                  {errors.chunkIndex.message}
                </p>
              ) : null}
            </div>

            {selectedChunk ? (
              <p className="text-[11px] text-muted-foreground">
                <span>所选范围</span>{" "}
                <span className="break-all font-mono">
                  {selectedChunk.start.toISOString()} →{" "}
                  {selectedChunk.end.toISOString()}
                </span>
              </p>
            ) : null}

            <div className="flex flex-col gap-1">
              <label
                htmlFor={confirmedId}
                className="inline-flex items-center gap-2 text-xs"
              >
                <input
                  id={confirmedId}
                  type="checkbox"
                  aria-invalid={Boolean(errors.confirmed)}
                  aria-describedby={
                    errors.confirmed ? "export-confirm-error" : undefined
                  }
                  {...register("confirmed")}
                />
                我确认只创建所选分段，不创建合并 ZIP
              </label>
              {errors.confirmed?.message ? (
                <p
                  id="export-confirm-error"
                  role="alert"
                  className="text-[11px] text-status-critical"
                >
                  {errors.confirmed.message}
                </p>
              ) : null}
            </div>
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={creating}
            >
              取消
            </Button>
            <Button type="submit" disabled={creating || !isValid}>
              {creating ? "创建中…" : "确认创建该段"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
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
  const retryAfter = useRetryAfter();
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
      return active ? ACTIVE_JOB_POLL_INTERVAL_MS : false;
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
      "≤31 个 UTC 天。请使用上方小文件 CSV，不需要 Export Job。";
  } else if (decision.kind === "invalid") {
    createDisabledMessage = decision.message;
  } else if (decision.kind === "split") {
    createDisabledMessage = `超过 366 个 UTC 天。需拆分为 ${decision.chunkCount} 个 Export Job，不能创建单个任务。`;
  } else if (blockedReason) {
    createDisabledMessage = blockedReason.message;
  } else if (retryAfter.reason) {
    createDisabledMessage = retryAfter.reason.message;
  } else if (!query) {
    createDisabledMessage = "请先选择 InstrumentId、Bar Interval 与时间范围。";
  } else if (hasActiveJob) {
    createDisabledMessage =
      "已有运行中的 Export Job；每个用户默认最多 1 个（queued / running / cancelling）。";
  }

  const createEnabled =
    jobsEnabled &&
    decision.kind === "job" &&
    query !== null &&
    !hasActiveJob &&
    !createMutation.isPending &&
    !retryAfter.blocked;

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
    if (retryAfter.reason) {
      return retryAfter.reason.message;
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
    jobsEnabled &&
    query !== null &&
    !hasActiveJob &&
    !createMutation.isPending &&
    !retryAfter.blocked;

  const createError =
    createMutation.error instanceof HttpError ? createMutation.error : null;
  const cancelError =
    cancelMutation.error instanceof HttpError ? cancelMutation.error : null;
  const quota = createError ? quotaMessage(createError) : null;
  const createOutcomeUnknown = createError
    ? isUnknownOutcome(createError)
    : false;
  const cancelOutcomeUnknown = cancelError
    ? isUnknownOutcome(cancelError)
    : false;

  const visibilityCopy =
    role === "SuperAdmin"
      ? "可见范围 全部 Export Job（服务端按角色过滤）。"
      : role !== null
        ? "可见范围 仅本人 Export Job（服务端按所有权过滤；前端不做安全过滤）。"
        : "未分配 Access Role；Export Job 只在 Admin API 模式可用。";

  return (
    <Card
      className={cn("mt-4", className)}
      role="region"
      aria-label="Export Job"
    >
      <CardHeader>
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-1.5">
            <CardTitle>Export Job（31–366 天）</CardTitle>
            <HelpTrigger
              label="Export Job 说明"
              content={PANEL_HELP}
              side="bottom"
            />
            <HelpTrigger
              label="Export Job 状态说明"
              content={STATUS_HELP}
              side="bottom"
            />
          </div>
          <p className="mt-1 text-xs text-muted-foreground">
            {decision.message}
          </p>
        </div>
        <StatusBadge tone="unknown">Admin API 契约目标</StatusBadge>
      </CardHeader>

      <CardContent>
        <p className="text-[11px] text-faint-foreground">
          Admin API 契约目标。当前后端未实现时保持禁用并显示原因；本面板的契约测试不计为真实后端联调。
        </p>
        <p className="mt-2 text-xs text-muted-foreground">
          31–366 个 UTC 天只创建服务端任务。异步能力不可用时保持禁用，不会自动退回超大 Blob。
        </p>

        <div className="mt-3 flex flex-wrap items-center gap-2">
          <Button
            disabled={!createEnabled}
            aria-describedby={createDisabledMessage ? reasonId : undefined}
            onClick={() => {
              if (query && !retryAfter.blocked) {
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
          <div className="mt-4 rounded-lg bg-status-warning-bg px-3 py-3 text-xs text-status-warning-fg">
            <div className="flex flex-wrap items-center gap-2">
              <StatusBadge tone="warning">需要拆分</StatusBadge>
              <span className="font-medium">
                需要拆分为 {decision.chunkCount} 个 Export Job（每段 ≤366 天）
              </span>
            </div>
            <p className="mt-1.5">
              逐段创建；一次只能有一个运行中的 Job，配额由服务端执行。
            </p>
            <p className="mt-1">
              每段是独立的单 Instrument Export Job；不会合并为多 Instrument
              ZIP，也不会自动拼接 CSV。
            </p>
            <ul
              data-testid="export-split-chunks"
              className="mt-2 flex max-h-72 flex-col gap-2 overflow-y-auto pr-1"
            >
              {decision.chunks.map((chunk, index) => {
                const blockReason = splitChunkBlockReason(chunk);
                const reasonId = `export-chunk-${index}-reason`;
                return (
                  <li
                    key={chunk.start.toISOString()}
                    className="flex min-w-0 flex-wrap items-center gap-2 rounded-md bg-card/60 p-2"
                  >
                    <span className="min-w-0 flex-1 break-all font-mono">
                      第 {index + 1} / {decision.chunkCount} 段{" "}
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
                        className="w-full text-[11px]"
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

        <p className="mt-3 text-xs text-muted-foreground">{visibilityCopy}</p>

        {quota ? (
          <p role="alert" className="mt-3 text-xs text-status-critical">
            {quota}
          </p>
        ) : null}

        {createError ? (
          <ErrorState
            className="mt-3"
            title={
              createOutcomeUnknown
                ? "Export Job 创建结果未知"
                : "Export Job 创建失败"
            }
            description={
              <>
                <div className="font-mono text-[11px]">{createError.code}</div>
                <div>
                  {quota ?? (
                    <span data-raw-evidence="true">
                      {createError.message}
                    </span>
                  )}
                </div>
                {createOutcomeUnknown ? (
                  <div className="mt-1">
                    未自动重试。请求可能已在服务端生效，请刷新任务列表确认。
                  </div>
                ) : null}
              </>
            }
            requestId={createError.requestId}
          />
        ) : null}

        {cancelError ? (
          <ErrorState
            className="mt-3"
            title={
              cancelOutcomeUnknown
                ? "取消 Export Job 结果未知"
                : "取消 Export Job 失败"
            }
            description={
              <>
                <div className="font-mono text-[11px]">{cancelError.code}</div>
                <div>
                  {cancelError.code === "conflict" ? (
                    "任务已不在可取消状态；请刷新后按实际终态处理。"
                  ) : (
                    <span data-raw-evidence="true">
                      {cancelError.message}
                    </span>
                  )}
                </div>
                {cancelOutcomeUnknown ? (
                  <div className="mt-1">
                    未自动重试。请刷新任务列表确认实际状态。
                  </div>
                ) : null}
              </>
            }
            requestId={cancelError.requestId}
          />
        ) : null}

        {jobsEnabled ? (
          <div className="mt-4 flex flex-col gap-3">
            {jobsQuery.isPending ? (
              <LoadingState label="正在加载 Export Job…" />
            ) : null}
            {jobsQuery.isError ? (
              <ErrorState
                title="Export Job 列表加载失败"
                description={
                  jobsQuery.error instanceof HttpError ? (
                    <span data-raw-evidence="true">
                      {jobsQuery.error.message}
                    </span>
                  ) : (
                    "无法读取 Export Job 列表。"
                  )
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
              <EmptyState
                title="当前没有可见的 Export Job"
                description="已创建的任务会显示在这里；文件生成后 24 小时内可下载。"
              />
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
                cancelBlockReason={retryAfter.reason}
                onCancel={(jobId) => {
                  if (retryAfter.blocked) {
                    return;
                  }
                  cancelMutation.mutate(jobId);
                }}
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
              ? "需要 Admin API。当前只读服务没有异步 Export Job；禁用该控件，且不会退回超大 Blob。"
              : blockedReason?.message}
          </p>
        )}

        <SplitChunkCreateDialog
          open={pendingChunk !== null}
          onOpenChange={(open) => {
            if (!open) {
              setPendingChunk(null);
            }
          }}
          chunks={decision.chunks}
          initialIndex={pendingChunk?.index ?? 0}
          creating={createMutation.isPending}
          onConfirm={(index) => {
            const chunk = decision.chunks[index];
            if (!chunk || retryAfter.blocked) {
              return;
            }
            setPendingChunk(null);
            createMutation.mutate(chunk);
          }}
        />
      </CardContent>
    </Card>
  );
}
