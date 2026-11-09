import type { StatusTone } from "@/components/status-badge";
import type { BackfillJobStatus } from "@/api/endpoints/admin";

/**
 * Quality & Backfill presentation helpers. They never invent values: missing
 * timestamps/ranges render as explicit dashes or "未提供".
 */

export function formatUtcTimestamp(value: string | null | undefined): string {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return `${date.toISOString().slice(0, 19).replace("T", " ")} UTC`;
}

export function formatRange(
  start: string | null | undefined,
  end: string | null | undefined,
): string {
  if (!start && !end) {
    return "未提供精确范围";
  }
  if (start && end) {
    return `${formatUtcTimestamp(start)} – ${formatUtcTimestamp(end)}`;
  }
  return start
    ? `自 ${formatUtcTimestamp(start)}（结束未提供）`
    : `至 ${formatUtcTimestamp(end)}（开始未提供）`;
}

export const GAP_REASON_LABELS: Record<string, string> = {
  history_unsupported: "历史数据不受支持",
  backfill_limit_exceeded: "超出补采窗口",
};

/**
 * Known Collector/Admin reason codes get a localized label; unknown codes stay
 * visible verbatim. The UI never branches on message prose.
 */
export function gapReasonLabel(reason: string): string {
  return GAP_REASON_LABELS[reason] ?? reason;
}

export interface JobStatusPresentation {
  label: string;
  tone: StatusTone;
  note?: string;
}

const JOB_STATUS_PRESENTATION: Record<BackfillJobStatus, JobStatusPresentation> =
  {
    queued: {
      label: "queued",
      tone: "info",
      note: "排队中，等待 Collector 执行",
    },
    running: {
      label: "running",
      tone: "info",
      note: "进度不确定：契约未提供百分比",
    },
    succeeded: { label: "succeeded", tone: "good" },
    partially_succeeded: {
      label: "partially_succeeded",
      tone: "serious",
      note: "部分成功，不代表全部缺口已解决",
    },
    failed: { label: "failed", tone: "critical" },
    cancelling: {
      label: "cancelling",
      tone: "warning",
      note: "协作式取消处理中",
    },
    cancelled: {
      label: "cancelled",
      tone: "unknown",
      note: "已写入的 Revision 不会撤回",
    },
  };

export function jobStatusPresentation(status: string): JobStatusPresentation {
  return (
    (JOB_STATUS_PRESENTATION as Record<string, JobStatusPresentation>)[
      status
    ] ?? { label: status, tone: "unknown" }
  );
}

export const ACTIVE_JOB_STATUSES: readonly BackfillJobStatus[] = [
  "queued",
  "running",
  "cancelling",
];

export const RETRYABLE_JOB_STATUSES: readonly BackfillJobStatus[] = [
  "failed",
  "partially_succeeded",
  "cancelled",
];

export function isJobCancellable(status: string): boolean {
  return status === "queued" || status === "running";
}

export function isJobRetryable(status: string): boolean {
  return (RETRYABLE_JOB_STATUSES as readonly string[]).includes(status);
}

/**
 * The real Backfill plan: only 1m is fetched natively; every higher interval
 * is produced by fetching 1m Bars and aggregating internally.
 */
export function planTextForInterval(interval: string): string {
  return interval === "1m"
    ? "原生 1m 补采"
    : "补采 1m + 内部 Bar Aggregation";
}
