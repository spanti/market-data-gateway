import { Database } from "lucide-react";
import type * as React from "react";
import {
  HelpTrigger,
  type HelpTriggerSide,
} from "@/components/help-trigger";
import {
  EmptyState,
  ErrorState,
  LoadingState,
  UnknownState,
} from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatUtcDateTime } from "@/lib/format";
import type { FeedFreshnessRow } from "@/lib/freshness";
import { cn } from "@/lib/utils";
import { IconTile } from "@/components/icon-tile";

export interface FreshnessMatrixHelp {
  label: string;
  content: React.ReactNode;
  side?: HelpTriggerSide;
}

export interface FreshnessMatrixProps {
  title?: string;
  description?: React.ReactNode;
  help?: FreshnessMatrixHelp;
  overallStatus?: React.ReactNode;
  rows: FeedFreshnessRow[];
  /** Overall source state. Unknown is a real capability gap. */
  state?: "loading" | "ready" | "error" | "unknown";
  unknownReason?: React.ReactNode;
  errorMessage?: string | null;
  lastSuccessAt?: number | null;
  className?: string;
}

function formatSeconds(value: number | null): string {
  if (value === null) {
    return "—";
  }
  return `${Math.round(value)}s`;
}

function remainingLabel(row: FeedFreshnessRow): string {
  if (row.ageS === null || row.thresholdS === null) {
    return "阈值未知";
  }
  const remaining = row.thresholdS - row.ageS;
  if (remaining <= 0) {
    return "已超出阈值";
  }
  return `剩余 ${Math.round(remaining)}s`;
}

function usagePercent(row: FeedFreshnessRow): number | null {
  if (row.ageS === null || row.thresholdS === null || row.thresholdS <= 0) {
    return null;
  }
  return Math.min(100, Math.max(0, (row.ageS / row.thresholdS) * 100));
}

function RowBadge({ stale }: { stale: boolean | null }) {
  if (stale === null) {
    return <StatusBadge tone="unknown">未暴露</StatusBadge>;
  }
  return stale ? (
    <StatusBadge tone="critical">失活</StatusBadge>
  ) : (
    <StatusBadge tone="good">正常</StatusBadge>
  );
}

/**
 * Variant D Feed Freshness triage card. Feed Freshness is separate from
 * Dataset Freshness and is never inferred from connection state. The server's
 * stale flag is authoritative.
 */
export function FreshnessMatrix({
  title = "Feed Freshness",
  description = "按 (feed, InstrumentId, Bar Interval) 展示服务端提供的 age_s / threshold_s / stale。",
  help,
  overallStatus,
  rows,
  state = "ready",
  unknownReason,
  errorMessage,
  lastSuccessAt = null,
  className,
}: FreshnessMatrixProps) {
  return (
    <Card
      data-slot="freshness-matrix"
      role="region"
      aria-label={title}
      className={cn("min-w-0 p-5", className)}
    >
      <CardHeader className="p-0">
        <div className="flex min-w-0 items-center gap-2">
          <IconTile icon={Database} />
          <CardTitle id="feed-freshness-title">{title}</CardTitle>
          {help ? (
            <HelpTrigger
              label={help.label}
              content={help.content}
              side={help.side ?? "bottom"}
            />
          ) : null}
        </div>
        {overallStatus ? (
          <div className="flex flex-wrap items-center gap-2">{overallStatus}</div>
        ) : null}
      </CardHeader>
      <CardContent className="p-0 pt-3">
        {state === "unknown" ? (
          <UnknownState cause="not-exposed" reason={unknownReason} />
        ) : state === "loading" ? (
          <LoadingState label="正在读取 Collector /status" showSkeleton />
        ) : state === "error" ? (
          <ErrorState
            description={
              <span>
                Collector /status 加载失败。最后成功{" "}
                {formatUtcDateTime(lastSuccessAt)}。不会继续把旧 Feed
                Freshness 标为健康。
                {errorMessage ? (
                  <>
                    <span className="text-muted-foreground"> Raw error </span>
                    <span data-raw-evidence="true">{errorMessage}</span>
                  </>
                ) : null}
              </span>
            }
          />
        ) : null}
        {state === "ready" && description ? (
          <p className="text-[11px] leading-relaxed text-muted-foreground">
            {description}
          </p>
        ) : null}
        {state !== "ready" ? null : rows.length === 0 ? (
          <EmptyState
            className="mt-3"
            title="暂无 Feed Freshness 跟踪项"
            description="没有真实跟踪项时保持空白，不生成健康记录。"
          />
        ) : (
          <div
            className="mt-3 max-h-[460px] space-y-2.5 overflow-y-auto pr-1"
            role="group"
            tabIndex={0}
            aria-label="Feed Freshness 跟踪项列表"
          >
            {rows.map((row) => {
              const fill = usagePercent(row);
              return (
                <div
                  key={`${row.feed}:${row.instrumentId}:${row.interval}`}
                  data-freshness-state={
                    row.stale === null
                      ? "unknown"
                      : row.stale
                        ? "stale"
                        : "healthy"
                  }
                  className={cn(
                    "rounded-lg p-3",
                    row.stale ? "bg-status-critical-bg" : "bg-surface-sunken",
                  )}
                >
                  <div className="flex flex-wrap items-start justify-between gap-x-3 gap-y-1.5">
                    <div className="min-w-0">
                      <div className="break-all font-mono text-xs font-medium">
                        {row.instrumentId}
                      </div>
                      <div className="mt-0.5 break-words text-[11px] text-faint-foreground">
                        {row.feed} / {row.interval}
                      </div>
                    </div>
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="font-mono text-[11px] tabular-nums text-muted-foreground">
                        age {formatSeconds(row.ageS)} / threshold{" "}
                        {formatSeconds(row.thresholdS)}
                      </span>
                      <RowBadge stale={row.stale} />
                    </div>
                  </div>
                  <div className="mt-2.5 flex flex-wrap items-center gap-x-3 gap-y-1.5">
                    {fill === null ? (
                      <span className="text-[11px] text-faint-foreground">
                        年龄或阈值未暴露
                      </span>
                    ) : (
                      <div
                        className="h-1.5 min-w-24 flex-1 overflow-hidden rounded-full bg-muted"
                        role="img"
                        aria-label={`${row.instrumentId} age ${formatSeconds(
                          row.ageS,
                        )}，threshold ${formatSeconds(row.thresholdS)}`}
                      >
                        <div
                          className={cn(
                            "h-full rounded-full",
                            row.stale ? "bg-status-critical" : "bg-chart-series-1",
                          )}
                          style={{ width: `${fill}%` }}
                        />
                      </div>
                    )}
                    <span
                      className={cn(
                        "shrink-0 text-[11px] tabular-nums",
                        row.stale
                          ? "font-medium text-status-critical-fg"
                          : "text-faint-foreground",
                      )}
                    >
                      {remainingLabel(row)}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
