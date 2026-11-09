import { useId, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { BackfillJob } from "@/api/endpoints/admin";
import {
  cancelBackfillJob,
  fetchBackfillJobs,
  newIdempotencyKey,
  retryBackfillJob,
} from "@/api/endpoints/backfill";
import { HttpError } from "@/api/http/types";
import { ConfirmDialog } from "@/components/confirm-dialog";
import {
  DisabledAction,
  DisabledReason,
} from "@/components/disabled-reason";
import { ForbiddenState } from "@/components/permission-gate";
import { Pagination } from "@/components/pagination";
import {
  EmptyState,
  ErrorState,
  LoadingState,
} from "@/components/states";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Panel, PanelDescription, PanelHeader, PanelTitle } from "@/components/ui/panel";
import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { DisabledReason as DisabledReasonValue } from "@/runtime/permissions";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import { BackfillJobCreateForm } from "./backfill-job-create-form";
import {
  formatRange,
  formatUtcTimestamp,
  isJobCancellable,
  isJobRetryable,
  jobStatusPresentation,
} from "./quality-format";
import { useQualityGate } from "./quality-gates";

function queryErrorText(error: unknown): {
  message: string;
  requestId: string | null;
} {
  if (error instanceof HttpError) {
    return {
      message: `${error.code}: ${error.message}`,
      requestId: error.requestId,
    };
  }
  return {
    message: error instanceof Error ? error.message : "未知错误",
    requestId: null,
  };
}

function JobProgress({ job }: { job: BackfillJob }) {
  const presentation = jobStatusPresentation(job.status);
  if (isJobCancellable(job.status)) {
    return (
      <div className="flex flex-col gap-1">
        <div
          role="progressbar"
          aria-label={`Job ${job.job_id} 进度（不确定）`}
          aria-valuetext="进度不确定"
          className="h-1.5 w-24 overflow-hidden bg-surface-sunken"
        >
          <div className="h-full w-1/2 animate-pulse bg-accent-strong" />
        </div>
        <span className="text-[11px] text-faint-foreground">
          进度不确定：契约未提供百分比
        </span>
      </div>
    );
  }
  return (
    <span className="text-[11px] text-faint-foreground">
      {presentation.note ?? "—"}
    </span>
  );
}

function JobLinkage({ job }: { job: BackfillJob }) {
  if (job.retry_of_job_id) {
    return (
      <span className="font-mono text-[11px]">
        重试自 {job.retry_of_job_id}
      </span>
    );
  }
  if (job.status === "succeeded" && job.resolved_gap_ids.length > 0) {
    return (
      <span className="font-mono text-[11px]">
        已解决 Gap：{job.resolved_gap_ids.join(", ")}
      </span>
    );
  }
  if (job.status === "partially_succeeded") {
    return (
      <span className="text-[11px] text-status-serious">
        未关闭全部 Gap；部分区间失败或被拒绝
      </span>
    );
  }
  return <span className="text-faint-foreground">—</span>;
}

function JobError({ job }: { job: BackfillJob }) {
  if (!job.error) {
    return null;
  }
  const interrupted = job.error.code === "interrupted";
  return (
    <div className="mt-1 text-[11px] text-status-critical">
      {interrupted ? <span className="font-medium">已中断（interrupted）</span> : null}{" "}
      <span className="font-mono">{job.error.code}</span>: {job.error.message}
    </div>
  );
}

interface ActionNotice {
  tone: StatusTone;
  title: string;
  detail: string;
  requestId: string | null;
}

function describeActionError(
  error: unknown,
  kind: "cancel" | "retry",
): ActionNotice {
  const fallbackTitle = kind === "cancel" ? "取消失败" : "重试失败";
  if (error instanceof HttpError) {
    const requestId = error.requestId;
    if (error.status === null) {
      return {
        tone: "unknown",
        title: "结果未知：未自动重试",
        detail:
          kind === "cancel"
            ? "无法确认取消请求是否被接受；已写入的 Revision 不会撤回。请刷新列表确认。"
            : "无法确认重试是否已创建新 Job。请刷新列表确认，不要重复提交。",
        requestId,
      };
    }
    if (error.status === 401) {
      return { tone: "critical", title: "需要登录", detail: "会话已失效，本次请求未执行。", requestId };
    }
    if (error.status === 403) {
      return { tone: "critical", title: "没有权限", detail: "缺少取消或重试 Backfill Job 的权限。", requestId };
    }
    if (error.status === 409) {
      const reason =
        typeof error.details.reason === "string" ? error.details.reason : null;
      if (reason === "job_not_cancellable") {
        return {
          tone: "warning",
          title: "冲突：无法取消",
          detail: "该 Job 已结束或状态已改变，服务端拒绝取消。请刷新列表。",
          requestId,
        };
      }
      return {
        tone: "warning",
        title: "冲突：未执行",
        detail: `${error.code}: ${error.message}`,
        requestId,
      };
    }
    return {
      tone: "critical",
      title: fallbackTitle,
      detail: `${error.code}: ${error.message}`,
      requestId,
    };
  }
  return {
    tone: "critical",
    title: fallbackTitle,
    detail: error instanceof Error ? error.message : "未知错误",
    requestId: null,
  };
}

function RowAction({
  label,
  reason,
  onClick,
}: {
  label: string;
  reason: DisabledReasonValue | null;
  onClick: () => void;
}) {
  const reasonId = useId();
  return (
    <div className="flex flex-col gap-1">
      <Button
        size="sm"
        variant="outline"
        disabled={reason !== null}
        aria-describedby={reason ? reasonId : undefined}
        onClick={onClick}
      >
        {label}
      </Button>
      <DisabledReason id={reasonId} reason={reason} className="text-[10px]" />
    </div>
  );
}

/**
 * Backfill Job — Admin API only. The contract provides `current_range` and
 * counters but no percentage, so running/cancelling progress is explicitly
 * indeterminate. Retry creates a new linked Job; the old row is never rewritten.
 */
export function BackfillJobsSection() {
  const { session } = useRuntimeMode();
  const queryClient = useQueryClient();
  const gate = useQualityGate("backfill.job.view", "backfill_job");
  const createGate = useQualityGate("backfill.job.create", "backfill_job");
  const actionGate = useQualityGate("backfill.job.cancel", "backfill_job");
  const [cursor, setCursor] = useState<string | null>(null);
  const [pendingAction, setPendingAction] = useState<
    { kind: "cancel" | "retry"; job: BackfillJob } | null
  >(null);
  const [actionNotice, setActionNotice] = useState<ActionNotice | null>(null);

  const query = useQuery({
    queryKey: ["quality", "backfill-jobs", cursor],
    queryFn: ({ signal }) =>
      fetchBackfillJobs(
        { cursor: cursor ?? undefined, limit: 50 },
        signal,
      ),
    enabled: gate.enabled,
  });

  const invalidateQualityData = () => {
    void queryClient.invalidateQueries({
      queryKey: ["quality", "backfill-jobs"],
    });
    void queryClient.invalidateQueries({
      queryKey: ["quality", "backfill-gaps"],
    });
  };

  const cancelMutation = useMutation({
    mutationFn: ({ job, reason }: { job: BackfillJob; reason: string }) =>
      cancelBackfillJob(
        job.job_id,
        { reason },
        {
          csrfToken: session?.csrfToken ?? null,
          idempotencyKey: newIdempotencyKey(),
        },
      ),
    onSuccess: (_updated, variables) => {
      setPendingAction(null);
      setActionNotice({
        tone: "good",
        title: "取消请求已接受",
        detail: `Job ${variables.job.job_id}：协作式取消请求已被接受；最终状态以服务端刷新结果为准，已写入的 Revision 不会撤回。`,
        requestId: null,
      });
      invalidateQualityData();
    },
    onError: (error) => {
      setPendingAction(null);
      setActionNotice(describeActionError(error, "cancel"));
    },
  });

  const retryMutation = useMutation({
    mutationFn: ({ job, reason }: { job: BackfillJob; reason: string }) =>
      retryBackfillJob(
        job.job_id,
        { reason },
        {
          csrfToken: session?.csrfToken ?? null,
          idempotencyKey: newIdempotencyKey(),
        },
      ),
    onSuccess: (newJob, variables) => {
      setPendingAction(null);
      setActionNotice({
        tone: "good",
        title: `已创建关联的新 Job ${newJob.job_id}`,
        detail: `重试自 ${variables.job.job_id}；旧 Job 状态与计数不变，新 Job 重新扫描原始范围。`,
        requestId: null,
      });
      invalidateQualityData();
    },
    onError: (error) => {
      setPendingAction(null);
      setActionNotice(describeActionError(error, "retry"));
    },
  });

  const errorInfo = query.isError ? queryErrorText(query.error) : null;

  return (
    <Panel className="mt-6 p-5" aria-label="Backfill Job">
      <PanelHeader>
        <div>
          <PanelTitle>Backfill Job</PanelTitle>
          <PanelDescription>
            展示 queued / running / succeeded / partially_succeeded / failed /
            cancelling / cancelled、计数与当前区间。重试创建关联新 Job，旧 Job
            不改写；取消不撤回已写入 Revision。高周期补采计划为“补采 1m +
            内部 Bar Aggregation”。计数中的 written_bars
            包含内部聚合生成的父 Bar，不等于目标 Interval 总数。
          </PanelDescription>
        </div>
        {gate.enabled ? (
          <StatusBadge tone="info">Admin API 持久数据</StatusBadge>
        ) : null}
      </PanelHeader>

      {!gate.enabled ? (
        <div className="mt-4 flex flex-col gap-2">
          <DisabledAction label="触发 Backfill Job" reason={gate.reason} />
          <p className="text-xs text-muted-foreground">
            Current Contract 没有持久 Backfill Job API。本区域不会显示虚构
            Job、计数或进度；所有触发与跟踪能力需要 Admin API。
          </p>
        </div>
      ) : (
        <>
          <BackfillJobCreateForm disabledReason={createGate.reason} />

          {actionNotice ? (
            <div
              role="alert"
              className="mt-3 flex flex-col gap-1 border border-border p-2"
            >
              <StatusBadge tone={actionNotice.tone}>
                {actionNotice.title}
              </StatusBadge>
              <p className="text-xs text-muted-foreground">
                {actionNotice.detail}
              </p>
              {actionNotice.requestId ? (
                <p className="font-mono text-[11px] text-faint-foreground">
                  Request ID: {actionNotice.requestId}
                </p>
              ) : null}
            </div>
          ) : null}

          {query.isPending ? (
            <LoadingState label="正在读取 Backfill Job…" />
          ) : query.isError ? (
            errorInfo &&
            query.error instanceof HttpError &&
            query.error.status === 403 ? (
              <ForbiddenState
                message="缺少查看 Backfill Job 的权限。"
                requestId={errorInfo.requestId}
              />
            ) : (
              <ErrorState
                title="加载 Backfill Job 失败"
                description={errorInfo?.message}
                requestId={errorInfo?.requestId}
                onRetry={() => void query.refetch()}
              />
            )
          ) : (query.data?.items.length ?? 0) === 0 ? (
            <EmptyState
              title="没有 Backfill Job"
              description="服务端没有返回 Job；这里不会补造任务、计数或进度。"
            />
          ) : (
            <>
              <Table className="mt-3">
                <TableCaption>Backfill Job 列表（Admin API 数据）</TableCaption>
                <TableHeader>
                  <TableRow>
                    <TableHead>状态</TableHead>
                    <TableHead>Job</TableHead>
                    <TableHead>InstrumentId / Bar Interval</TableHead>
                    <TableHead>范围</TableHead>
                    <TableHead>当前区间 / 进度</TableHead>
                    <TableHead>计数</TableHead>
                    <TableHead>Gap / 重试关联</TableHead>
                    <TableHead>操作</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {query.data?.items.map((job) => {
                    const presentation = jobStatusPresentation(job.status);
                    return (
                      <TableRow key={job.job_id}>
                        <TableCell>
                          <StatusBadge tone={presentation.tone}>
                            {presentation.label}
                          </StatusBadge>
                          {presentation.note ? (
                            <div className="mt-1 text-[11px] text-faint-foreground">
                              {presentation.note}
                            </div>
                          ) : null}
                        </TableCell>
                        <TableCell>
                          <div className="font-mono">{job.job_id}</div>
                          <div className="text-[11px] text-faint-foreground">
                            {formatUtcTimestamp(job.created_at)}
                          </div>
                          <div className="text-[11px] text-muted-foreground">
                            {job.requested_by.display_name} ·{" "}
                            {job.requested_by.access_role}
                          </div>
                          <JobError job={job} />
                        </TableCell>
                        <TableCell>
                          <div className="font-mono">{job.instrument_id}</div>
                          <div className="font-mono text-[11px] text-faint-foreground">
                            {job.interval} · {job.plan.mode}
                          </div>
                        </TableCell>
                        <TableCell className="tabular-nums">
                          {formatRange(job.start, job.end)}
                          <div className="text-[11px] text-muted-foreground">
                            {job.reason}
                          </div>
                        </TableCell>
                        <TableCell className="tabular-nums">
                          {job.current_range
                            ? formatRange(
                                job.current_range.start,
                                job.current_range.end,
                              )
                            : "—"}
                          <JobProgress job={job} />
                        </TableCell>
                        <TableCell className="tabular-nums text-[11px]">
                          扫描 {job.counters.scanned_bars} · 缺失{" "}
                          {job.counters.gap_bars} · 补采{" "}
                          {job.counters.backfilled_bars} · 写入{" "}
                          {job.counters.written_bars} · 拒绝{" "}
                          {job.counters.rejected_bars}
                        </TableCell>
                        <TableCell>
                          <JobLinkage job={job} />
                        </TableCell>
                        <TableCell>
                          {isJobCancellable(job.status) ? (
                            <RowAction
                              label={`取消 Job ${job.job_id}`}
                              reason={actionGate.reason}
                              onClick={() =>
                                setPendingAction({ kind: "cancel", job })
                              }
                            />
                          ) : isJobRetryable(job.status) ? (
                            <RowAction
                              label={`重试 Job ${job.job_id}`}
                              reason={actionGate.reason}
                              onClick={() =>
                                setPendingAction({ kind: "retry", job })
                              }
                            />
                          ) : (
                            <span className="text-faint-foreground">—</span>
                          )}
                        </TableCell>
                      </TableRow>
                    );
                  })}
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
                  label="游标分页"
                />
                <span className="text-xs text-faint-foreground">
                  Job 状态由服务端推进；前端不自动重放写操作。
                </span>
              </div>
            </>
          )}

          <ConfirmDialog
            open={pendingAction !== null}
            onOpenChange={(open) => {
              if (!open) {
                setPendingAction(null);
              }
            }}
            title={
              pendingAction?.kind === "cancel"
                ? "确认取消 Backfill Job"
                : "确认重试 Backfill Job"
            }
            description={
              pendingAction ? (
                pendingAction.kind === "cancel" ? (
                  <span>
                    Job <span className="font-mono">{pendingAction.job.job_id}</span>{" "}
                    使用协作式取消：已写入的 Revision 不会撤回；取消请求被接受
                    不等于已取消，最终状态以服务端为准。
                  </span>
                ) : (
                  <span>
                    重试会创建关联的新 Job（retry_of_job_id=
                    <span className="font-mono">{pendingAction.job.job_id}</span>
                    ），旧 Job 状态与计数不变；新 Job 重新扫描原始范围。
                  </span>
                )
              ) : undefined
            }
            confirmLabel={
              pendingAction?.kind === "cancel"
                ? "确认取消 Job"
                : "确认重试 Job"
            }
            submitting={
              cancelMutation.isPending || retryMutation.isPending
            }
            onConfirm={(input) => {
              const action = pendingAction;
              if (!action) {
                return;
              }
              if (action.kind === "cancel") {
                cancelMutation.mutate({ job: action.job, reason: input.reason });
              } else {
                retryMutation.mutate({ job: action.job, reason: input.reason });
              }
            }}
          />
        </>
      )}
    </Panel>
  );
}
