import { Network } from "lucide-react";
import type * as React from "react";
import {
  HelpTrigger,
  type HelpTriggerSide,
} from "@/components/help-trigger";
import {
  EmptyState,
  ErrorState,
  LoadingState,
} from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { SourceStatusState } from "@/components/monitoring/source-status";
import { formatUtcDateTime } from "@/lib/format";
import type { VenueRow } from "@/lib/venues";
import { cn } from "@/lib/utils";

export interface UpstreamStatusProps {
  rows: VenueRow[];
  state: SourceStatusState;
  lastSuccessAt: number | null;
  errorMessage?: string | null;
  help?: { label: string; content: React.ReactNode; side?: HelpTriggerSide };
  className?: string;
}

function CapabilityGap({ label }: { label: string }) {
  return <StatusBadge tone="unknown">{label} 未暴露</StatusBadge>;
}

/**
 * Compact upstream triage card. Connection, heartbeat, reconnect count and
 * network capability are separate contract gaps and are never inferred from
 * Feed Freshness.
 */
export function UpstreamStatus({
  rows,
  state,
  lastSuccessAt,
  errorMessage,
  help,
  className,
}: UpstreamStatusProps) {
  return (
    <Card
      data-slot="upstream-status"
      role="region"
      aria-label="上游能力与 Freshness"
      className={cn("min-w-0 p-5", className)}
    >
      <CardHeader className="p-0">
        <div className="flex min-w-0 items-center gap-2">
          <span className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-accent-soft text-accent-strong">
            <Network className="size-4" aria-hidden="true" />
          </span>
          <CardTitle>上游状态</CardTitle>
          {help ? (
            <HelpTrigger
              label={help.label}
              content={help.content}
              side={help.side ?? "bottom"}
            />
          ) : null}
        </div>
        <span className="font-mono text-[11px] tabular-nums text-faint-foreground">
          {rows.length} 个 Venue
        </span>
      </CardHeader>
      <CardContent className="p-0 pt-3">
        {state === "loading" ? (
          <LoadingState label="正在读取上游观测" showSkeleton />
        ) : null}
        {state === "error" ? (
          <ErrorState
            description={
              <span>
                上游观测加载失败。最后成功{" "}
                {formatUtcDateTime(lastSuccessAt)}。没有真实 Instrument
                时不生成 Venue 行。
              </span>
            }
          />
        ) : null}
        {state === "unknown" ? (
          <ErrorState
            title="能力未知"
            description={errorMessage ?? "当前模式不提供上游观测能力。"}
          />
        ) : null}
        {state === "ready" && rows.length === 0 ? (
          <EmptyState
            title="暂无上游观测数据"
            description="没有真实 Instrument 时不生成 Venue 行。"
          />
        ) : null}
        {state === "ready" && rows.length > 0 ? (
          <div className="grid gap-2.5 sm:grid-cols-2">
            {rows.map((row) => (
              <div key={row.venue} className="rounded-lg bg-surface-sunken p-3">
                <div className="flex flex-wrap items-start justify-between gap-x-3 gap-y-1.5">
                  <div className="min-w-0">
                    <strong className="break-all font-mono text-sm font-semibold">
                      {row.venue}
                    </strong>
                    <div className="mt-0.5 break-words text-[11px] text-faint-foreground">
                      {row.feeds.length > 0
                        ? row.feeds.join(", ")
                        : "Feed 未暴露"}
                    </div>
                  </div>
                  {row.trackedCount === 0 ? (
                    <StatusBadge tone="unknown">Feed Freshness 未暴露</StatusBadge>
                  ) : (
                    <StatusBadge
                      tone={row.staleCount > 0 ? "critical" : "good"}
                    >
                      {row.staleCount > 0
                        ? `${row.staleCount} 项 Feed 失活`
                        : "Feed Freshness 正常"}
                    </StatusBadge>
                  )}
                </div>
                <div className="mt-2.5 flex flex-wrap gap-1.5">
                  <CapabilityGap label="连接" />
                  <CapabilityGap label="心跳" />
                  <CapabilityGap label="重连" />
                  <CapabilityGap label="网络" />
                </div>
                <div className="mt-2 text-[11px] text-faint-foreground">
                  最后事件 {formatUtcDateTime(row.lastEventTs)}
                </div>
              </div>
            ))}
          </div>
        ) : null}
        <p className="mt-3 text-[11px] leading-relaxed text-faint-foreground">
          Freshness 只表示最近事件年龄，不证明连接健康。上游连接、心跳、重连计数与网络能力未暴露，不从
          Freshness 推断。
        </p>
      </CardContent>
    </Card>
  );
}
