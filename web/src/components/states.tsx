import {
  CircleAlert,
  CircleHelp,
  Inbox,
  LoaderCircle,
  ShieldAlert,
} from "lucide-react";
import type * as React from "react";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";

export interface EmptyStateProps {
  title: string;
  description?: React.ReactNode;
  action?: React.ReactNode;
  icon?: React.ReactNode;
  className?: string;
}

export function EmptyState({
  title,
  description,
  action,
  icon,
  className,
}: EmptyStateProps) {
  return (
    <div
      data-slot="empty-state"
      className={cn(
        "flex flex-col items-center justify-center gap-2 px-4 py-8 text-center",
        className,
      )}
    >
      <span className="text-faint-foreground" aria-hidden="true">
        {icon ?? <Inbox className="size-5" />}
      </span>
      <p className="text-sm font-medium text-foreground">{title}</p>
      {description ? (
        <p className="max-w-xl text-xs text-muted-foreground">{description}</p>
      ) : null}
      {action}
    </div>
  );
}

export interface ErrorStateProps {
  title?: string;
  description: React.ReactNode;
  requestId?: string | null;
  onRetry?: () => void;
  forbidden?: boolean;
  className?: string;
}

export function ErrorState({
  title,
  description,
  requestId,
  onRetry,
  forbidden = false,
  className,
}: ErrorStateProps) {
  return (
    <div
      data-slot="error-state"
      role="alert"
      className={cn(
        "flex flex-col items-start gap-2 border border-border bg-card px-4 py-4",
        className,
      )}
    >
      <div className="flex items-center gap-2 text-sm font-medium text-status-critical">
        {forbidden ? (
          <ShieldAlert className="size-4" aria-hidden="true" />
        ) : (
          <CircleAlert className="size-4" aria-hidden="true" />
        )}
        {title ?? (forbidden ? "没有权限" : "加载失败")}
      </div>
      <div className="text-xs text-muted-foreground">{description}</div>
      {requestId ? (
        <div className="font-mono text-[11px] text-faint-foreground">
          Request ID: {requestId}
        </div>
      ) : null}
      {onRetry ? (
        <Button size="sm" variant="outline" onClick={onRetry}>
          重试
        </Button>
      ) : null}
    </div>
  );
}

export interface LoadingStateProps {
  label?: string;
  className?: string;
  showSkeleton?: boolean;
}

export function LoadingState({
  label = "正在加载…",
  className,
  showSkeleton = false,
}: LoadingStateProps) {
  return (
    <div
      data-slot="loading-state"
      role="status"
      aria-live="polite"
      className={cn("flex flex-col gap-3 px-4 py-6", className)}
    >
      <span className="inline-flex items-center gap-2 text-xs text-muted-foreground">
        <LoaderCircle className="size-4 animate-spin" aria-hidden="true" />
        {label}
      </span>
      {showSkeleton ? (
        <div className="flex flex-col gap-2" aria-hidden="true">
          <Skeleton className="h-3 w-2/3" />
          <Skeleton className="h-3 w-1/2" />
          <Skeleton className="h-3 w-3/4" />
        </div>
      ) : null}
    </div>
  );
}

export type UnknownCause = "not-exposed" | "not-loaded" | "failed";

const UNKNOWN_COPY: Record<
  UnknownCause,
  { title: string; description: string }
> = {
  "not-exposed": {
    title: "契约未暴露",
    description: "当前 API 不提供该数据，不能用推断值补空白。",
  },
  "not-loaded": {
    title: "尚未加载",
    description: "数据尚未返回，请等待真实结果。",
  },
  failed: {
    title: "加载失败",
    description: "无法确认当前值，请重试或查看 Request ID。",
  },
};

export interface UnknownStateProps {
  cause: UnknownCause;
  reason?: React.ReactNode;
  className?: string;
}

/**
 * Three distinct unknown causes. None of them may silently become zero,
 * green, or a stale "healthy" value.
 */
export function UnknownState({ cause, reason, className }: UnknownStateProps) {
  const copy = UNKNOWN_COPY[cause];
  return (
    <div
      data-slot="unknown-state"
      data-cause={cause}
      className={cn(
        "flex items-start gap-2 px-4 py-3 text-xs text-muted-foreground",
        className,
      )}
    >
      <CircleHelp
        className="mt-0.5 size-4 shrink-0 text-faint-foreground"
        aria-hidden="true"
      />
      <div>
        <div className="font-medium text-foreground">{copy.title}</div>
        <div>{copy.description}</div>
        {reason ? <div className="mt-1">{reason}</div> : null}
      </div>
    </div>
  );
}
