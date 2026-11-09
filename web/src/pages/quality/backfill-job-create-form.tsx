import { zodResolver } from "@hookform/resolvers/zod";
import { useState } from "react";
import { useForm } from "react-hook-form";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import type { BackfillJobCreateBody } from "@/api/endpoints/admin";
import {
  createBackfillJob,
  newIdempotencyKey,
} from "@/api/endpoints/backfill";
import { HttpError } from "@/api/http/types";
import { ConfirmDialog } from "@/components/confirm-dialog";
import {
  DisabledAction,
  DisabledReason as DisabledReasonNotice,
} from "@/components/disabled-reason";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import {
  BAR_INTERVALS,
  parseDateTimeInput,
  toQueryInstant,
} from "@/lib/time-range";
import type { DisabledReason } from "@/runtime/permissions";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import { useRetryAfter } from "@/runtime/use-retry-after";
import { formatRange, planTextForInterval } from "./quality-format";

interface BackfillCreateFormValues {
  instrumentId: string;
  interval: string;
  start: string;
  end: string;
  reason: string;
}

const backfillCreateSchema = z
  .object({
    instrumentId: z.string(),
    interval: z.string(),
    start: z.string(),
    end: z.string(),
    reason: z.string(),
  })
  .superRefine((values, ctx) => {
    const issue = (path: keyof BackfillCreateFormValues, message: string) =>
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: [path], message });

    if (!values.instrumentId.trim()) {
      issue("instrumentId", "请输入 InstrumentId");
    }
    if (!values.interval) {
      issue("interval", "请选择目标 Bar Interval");
    } else if (
      !(BAR_INTERVALS as readonly string[]).includes(values.interval)
    ) {
      issue("interval", "请选择有效的目标 Bar Interval");
    }

    const start = parseDateTimeInput(values.start, "utc");
    if (!values.start.trim()) {
      issue("start", "请输入开始时间");
    } else if (!start) {
      issue("start", "请输入有效的开始时间（UTC）");
    }

    const end = parseDateTimeInput(values.end, "utc");
    if (!values.end.trim()) {
      issue("end", "请输入结束时间");
    } else if (!end) {
      issue("end", "请输入有效的结束时间（UTC）");
    }

    if (start && end && end.getTime() <= start.getTime()) {
      issue("end", "结束时间必须晚于开始时间");
    }

    if (!values.reason.trim()) {
      issue("reason", "请输入操作原因");
    }
  });

interface CreateErrorDescription {
  tone: StatusTone;
  title: string;
  detail: string;
  requestId: string | null;
}

function describeCreateError(error: unknown): CreateErrorDescription {
  if (error instanceof HttpError) {
    const requestId = error.requestId;
    if (error.status === null) {
      return {
        tone: "unknown",
        title: "结果未知：未自动重试",
        detail:
          "请求未完成，无法确认服务端是否已创建 Job。请刷新 Job 列表确认，不要重复提交。",
        requestId,
      };
    }
    if (error.status === 401) {
      return {
        tone: "critical",
        title: "需要登录",
        detail: "会话已失效，本次请求未创建 Job。",
        requestId,
      };
    }
    if (error.status === 403) {
      return {
        tone: "critical",
        title: "没有权限",
        detail: "缺少创建 Backfill Job 的权限，本次未创建。",
        requestId,
      };
    }
    if (error.status === 409) {
      const activeJobId =
        typeof error.details.active_job_id === "string"
          ? error.details.active_job_id
          : null;
      return {
        tone: "warning",
        title: "冲突：本次未创建 Job",
        detail: activeJobId
          ? `同一 InstrumentId + Bar Interval 已有运行中的 Job：${activeJobId}。请等待或先取消该 Job。`
          : `${error.code}: ${error.message}`,
        requestId,
      };
    }
    if (error.status === 400) {
      return {
        tone: "critical",
        title: "请求无效：本次未创建 Job",
        detail: `${error.code}: ${error.message}`,
        requestId,
      };
    }
    return {
      tone: "critical",
      title: "创建失败",
      detail: `${error.code}: ${error.message}`,
      requestId,
    };
  }

  return {
    tone: "critical",
    title: "创建失败",
    detail: error instanceof Error ? error.message : "未知错误",
    requestId: null,
  };
}

function FieldError({
  id,
  message,
}: {
  id: string;
  message: string | undefined;
}) {
  if (!message) {
    return null;
  }
  return (
    <p id={id} role="alert" className="text-[11px] text-status-critical">
      {message}
    </p>
  );
}

export interface BackfillJobCreateFormProps {
  /** Non-null for Auditor or an unsupported capability; the trigger stays visible. */
  disabledReason: DisabledReason | null;
}

/**
 * Create form for a Backfill Job. Required: InstrumentId, target Bar Interval,
 * explicit start/end range, reason and confirmation. High intervals show the
 * truthful 1m + internal aggregation plan; there is no native high-interval
 * fetch. Writes are never auto-replayed and an unknown outcome is not success.
 */
export function BackfillJobCreateForm({
  disabledReason,
}: BackfillJobCreateFormProps) {
  const { session } = useRuntimeMode();
  const retryAfter = useRetryAfter();
  const queryClient = useQueryClient();
  const {
    register,
    handleSubmit,
    watch,
    formState: { errors },
  } = useForm<BackfillCreateFormValues>({
    resolver: zodResolver(backfillCreateSchema),
    mode: "onSubmit",
    defaultValues: {
      instrumentId: "",
      interval: "",
      start: "",
      end: "",
      reason: "",
    },
  });
  const interval = watch("interval");
  const [pendingBody, setPendingBody] = useState<BackfillJobCreateBody | null>(
    null,
  );
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [outcome, setOutcome] = useState<
    | { kind: "success"; jobId: string }
    | { kind: "error"; error: unknown }
    | null
  >(null);

  const mutation = useMutation({
    mutationFn: ({
      body,
      idempotencyKey,
    }: {
      body: BackfillJobCreateBody;
      idempotencyKey: string;
    }) =>
      createBackfillJob(body, {
        csrfToken: session?.csrfToken ?? null,
        idempotencyKey,
      }),
    onSuccess: (job) => {
      setConfirmOpen(false);
      setPendingBody(null);
      setOutcome({ kind: "success", jobId: job.job_id });
      void queryClient.invalidateQueries({
        queryKey: ["quality", "backfill-jobs"],
      });
      void queryClient.invalidateQueries({
        queryKey: ["quality", "backfill-gaps"],
      });
    },
    onError: (error) => {
      setConfirmOpen(false);
      setOutcome({ kind: "error", error });
    },
  });

  const submitForm = handleSubmit((values) => {
    if (retryAfter.blocked) {
      return;
    }
    const start = parseDateTimeInput(values.start, "utc");
    const end = parseDateTimeInput(values.end, "utc");
    if (!start || !end) {
      return;
    }

    setOutcome(null);
    setPendingBody({
      instrument_id: values.instrumentId.trim(),
      interval: values.interval,
      start: toQueryInstant(start),
      end: toQueryInstant(end),
      reason: values.reason.trim(),
      confirm: true,
    });
    setConfirmOpen(true);
  });

  function handleConfirm() {
    if (!pendingBody || mutation.isPending || retryAfter.blocked) {
      return;
    }
    mutation.mutate({
      body: pendingBody,
      idempotencyKey: newIdempotencyKey(),
    });
  }

  if (disabledReason) {
    return (
      <div className="mt-4 border border-border p-3" aria-label="触发 Backfill Job">
        <DisabledAction
          label="创建 Backfill Job"
          reason={disabledReason}
        />
        <p className="mt-2 text-xs text-muted-foreground">
          Auditor 只能查看质量数据；触发、取消与重试需要 SuperAdmin 或
          Operator。控制禁用不代表隐藏：服务端仍会拒绝越权请求。
        </p>
      </div>
    );
  }

  const errorDescription =
    outcome?.kind === "error" ? describeCreateError(outcome.error) : null;

  return (
    <form
      aria-label="创建 Backfill Job 表单"
      onSubmit={submitForm}
      noValidate
      className="mt-4 border border-border p-3"
    >
      <div className="flex flex-wrap items-start gap-3">
        <div className="flex w-52 flex-col gap-1">
          <Label htmlFor="backfill-create-instrument">InstrumentId</Label>
          <Input
            id="backfill-create-instrument"
            placeholder="binance:BTC/USDT"
            aria-invalid={Boolean(errors.instrumentId)}
            aria-describedby={
              errors.instrumentId ? "backfill-instrument-error" : undefined
            }
            {...register("instrumentId")}
          />
          <FieldError
            id="backfill-instrument-error"
            message={errors.instrumentId?.message}
          />
        </div>
        <div className="flex w-36 flex-col gap-1">
          <Label htmlFor="backfill-create-interval">目标 Bar Interval</Label>
          <Select
            id="backfill-create-interval"
            aria-invalid={Boolean(errors.interval)}
            aria-describedby={
              errors.interval ? "backfill-interval-error" : undefined
            }
            {...register("interval")}
          >
            <option value="">选择 Interval</option>
            {BAR_INTERVALS.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </Select>
          <FieldError
            id="backfill-interval-error"
            message={errors.interval?.message}
          />
        </div>
        <div className="flex w-52 flex-col gap-1">
          <Label htmlFor="backfill-create-start">
            开始时间（UTC，闭区间）
          </Label>
          <Input
            id="backfill-create-start"
            type="datetime-local"
            aria-invalid={Boolean(errors.start)}
            aria-describedby={
              errors.start ? "backfill-start-error" : undefined
            }
            {...register("start")}
          />
          <FieldError
            id="backfill-start-error"
            message={errors.start?.message}
          />
        </div>
        <div className="flex w-52 flex-col gap-1">
          <Label htmlFor="backfill-create-end">结束时间（UTC，开区间）</Label>
          <Input
            id="backfill-create-end"
            type="datetime-local"
            aria-invalid={Boolean(errors.end)}
            aria-describedby={errors.end ? "backfill-end-error" : undefined}
            {...register("end")}
          />
          <FieldError id="backfill-end-error" message={errors.end?.message} />
        </div>
        <div className="flex min-w-60 flex-1 flex-col gap-1">
          <Label htmlFor="backfill-create-reason">操作原因</Label>
          <Textarea
            id="backfill-create-reason"
            className="min-h-8"
            placeholder="说明本次补采原因（必填）"
            aria-invalid={Boolean(errors.reason)}
            aria-describedby={
              errors.reason ? "backfill-reason-error" : undefined
            }
            {...register("reason")}
          />
          <FieldError
            id="backfill-reason-error"
            message={errors.reason?.message}
          />
        </div>
        <Button
          type="submit"
          size="sm"
          disabled={mutation.isPending || retryAfter.blocked}
          aria-describedby={
            retryAfter.reason ? "backfill-create-retry-after" : undefined
          }
        >
          创建 Backfill Job
        </Button>
        <DisabledReasonNotice
          id="backfill-create-retry-after"
          reason={retryAfter.reason}
        />
      </div>

      {interval ? (
        <p
          data-testid="backfill-plan"
          className="mt-2 text-xs text-muted-foreground"
        >
          真实计划：{planTextForInterval(interval)}
          {interval === "1m" ? "" : "（不是调用原生高周期接口）"}。范围按 UTC
          提交，start 为闭区间、end 为开区间。
        </p>
      ) : null}

      {outcome?.kind === "success" ? (
        <div className="mt-3" role="status">
          <StatusBadge tone="good">
            已创建 Backfill Job {outcome.jobId}
          </StatusBadge>
        </div>
      ) : null}

      {errorDescription ? (
        <div
          className="mt-3 flex flex-col gap-1 border border-border p-2"
          role="alert"
        >
          <StatusBadge tone={errorDescription.tone}>
            {errorDescription.title}
          </StatusBadge>
          <p className="text-xs text-muted-foreground">
            {errorDescription.detail}
          </p>
          {errorDescription.requestId ? (
            <p className="font-mono text-[11px] text-faint-foreground">
              Request ID: {errorDescription.requestId}
            </p>
          ) : null}
        </div>
      ) : null}

      <ConfirmDialog
        open={confirmOpen}
        onOpenChange={setConfirmOpen}
        title="确认触发 Backfill Job"
        description={
          pendingBody ? (
            <span>
              InstrumentId <span className="font-mono">{pendingBody.instrument_id}</span>{" "}
              · Bar Interval <span className="font-mono">{pendingBody.interval}</span>。
              范围 {formatRange(pendingBody.start, pendingBody.end)}（start 闭、end
              开）。原因：{pendingBody.reason}。真实计划：
              {planTextForInterval(pendingBody.interval)}
              ，不是调用原生高周期接口。已写入的 Revision 不会因取消回退；
              提交后不自动重放。
            </span>
          ) : undefined
        }
        confirmLabel="确认创建 Job"
        submitting={mutation.isPending}
        requireReason={false}
        onConfirm={handleConfirm}
      />
    </form>
  );
}
