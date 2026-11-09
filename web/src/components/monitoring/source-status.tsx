import { StatusBadge } from "@/components/status-badge";
import { formatUtcDateTime } from "@/lib/format";
import { cn } from "@/lib/utils";

export type SourceStatusState = "loading" | "ready" | "error" | "unknown";

export interface SourceStatusProps {
  source: string;
  window: string;
  state: SourceStatusState;
  lastSuccessAt: number | null;
  reason?: string | null;
  className?: string;
}

/**
 * Every data source reports the same three facts: which source it is, what
 * statistical window the values cover, and when it last succeeded. A failure
 * keeps the last-success time visible but never labels old data healthy.
 */
export function SourceStatus({
  source,
  window,
  state,
  lastSuccessAt,
  reason,
  className,
}: SourceStatusProps) {
  const badge =
    state === "ready" ? (
      <StatusBadge tone="good">正常</StatusBadge>
    ) : state === "loading" ? (
      <StatusBadge tone="unknown">加载中</StatusBadge>
    ) : state === "error" ? (
      <StatusBadge tone="critical">未知</StatusBadge>
    ) : (
      <StatusBadge tone="unknown">能力未知</StatusBadge>
    );

  return (
    <div
      data-slot="source-status"
      data-state={state}
      className={cn(
        "flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-border px-1 py-2 text-xs last:border-b-0",
        className,
      )}
    >
      {badge}
      <span className="font-medium text-foreground">数据源：{source}</span>
      <span className="text-muted-foreground">窗口：{window}</span>
      <span className="text-faint-foreground">
        最后成功：{formatUtcDateTime(lastSuccessAt)}
      </span>
      {reason ? (
        <span className="text-status-critical" role="note">
          {reason}
        </span>
      ) : null}
    </div>
  );
}
