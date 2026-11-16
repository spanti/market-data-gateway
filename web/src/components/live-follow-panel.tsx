import { PlugZap, RotateCcw } from "lucide-react";
import { useId } from "react";
import { HelpTrigger } from "@/components/help-trigger";
import { StatusBadge } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import type { BarLiveFeedState, LiveFeedStatus } from "@/hooks/use-bar-live-feed";
import { cn } from "@/lib/utils";

const STATUS_COPY: Record<
  LiveFeedStatus,
  { tone: "good" | "warning" | "critical" | "unknown" | "info"; label: string }
> = {
  disabled: { tone: "unknown", label: "实时跟随未开启" },
  blocked: { tone: "warning", label: "实时跟随已阻止" },
  connecting: { tone: "info", label: "连接中" },
  receiving: { tone: "good", label: "接收中（新接收段）" },
  interrupted: { tone: "critical", label: "接收段已中断" },
};

export interface LiveFollowPanelProps {
  feed: BarLiveFeedState;
  onRestRefetch?: () => void;
  className?: string;
}

/**
 * Explicit live-follow control. It never auto-connects, never claims gap-free
 * recovery, and keeps server lag notices separate from anything local.
 */
export function LiveFollowPanel({
  feed,
  onRestRefetch,
  className,
}: LiveFollowPanelProps) {
  const toggleId = useId();
  const statusCopy = STATUS_COPY[feed.status];
  const toggleDisabled = !feed.rangeIncludesNow || !feed.canEnable;
  const canReconnect = feed.status === "interrupted" && feed.canEnable;
  const multiSegment = feed.segments.length > 1;

  return (
    <Card
      className={cn("mt-4", className)}
      role="region"
      aria-label="实时跟随"
    >
      <CardHeader>
        <div className="flex min-w-0 items-center gap-1.5">
          <CardTitle>实时跟随</CardTitle>
          <HelpTrigger
            label="实时跟随说明"
            content="只有用户明确开启且时间范围包含当前时间时才连接 market.bar.<interval>.<instrument_id>。WS 没有恢复位点。断线后建立新接收段并标记不连续，不宣称无缝恢复。REST 重新查询只补充已落盘历史，不修复 WS 缺口。"
            side="bottom"
          />
        </div>
        <StatusBadge tone={statusCopy.tone} live={feed.status === "receiving"}>
          {statusCopy.label}
        </StatusBadge>
      </CardHeader>

      <CardContent>
        <div className="flex flex-wrap items-center gap-3">
          <label htmlFor={toggleId} className="flex items-center gap-2 text-xs">
            <input
              id={toggleId}
              type="checkbox"
              className="size-4 rounded border-input accent-[var(--ring)]"
              checked={feed.enabled}
              disabled={toggleDisabled && !feed.enabled}
              onChange={(event) =>
                event.target.checked ? feed.enable() : feed.disable()
              }
            />
            实时跟随
          </label>
          {!feed.canEnable ? (
            <span className="text-xs text-status-warning">
              实时跟随不可用，{feed.unavailableReason ?? "能力未知，暂不可用"}
            </span>
          ) : toggleDisabled && !feed.enabled ? (
            <span className="text-xs text-faint-foreground">
              当前范围不包含现在，不能开启实时跟随；历史范围不连接 WS。
            </span>
          ) : null}
          {feed.blockedReason ? (
            <span className="text-xs text-status-warning">
              {feed.blockedReason}
            </span>
          ) : null}
          {canReconnect ? (
            <Button size="sm" variant="outline" onClick={feed.reconnect}>
              <RotateCcw aria-hidden="true" />
              重新连接（新建接收段）
            </Button>
          ) : null}
          {onRestRefetch ? (
            <Button size="sm" variant="outline" onClick={onRestRefetch}>
              <PlugZap aria-hidden="true" />
              重新查询 REST（只补充已落盘历史，不修复 WS 缺口）
            </Button>
          ) : null}
        </div>

        {feed.topic ? (
          <p className="mt-2 font-mono text-[11px] text-faint-foreground">
            Topic: {feed.topic}
          </p>
        ) : null}

        {multiSegment ? (
          <p
            role="status"
            className="mt-3 rounded-lg bg-status-warning-bg px-3 py-2 text-xs text-status-warning-fg"
          >
            存在多个接收段。断线后的新连接与上一段不连续，不能宣称无缝恢复。
          </p>
        ) : null}

        {feed.segments.length > 0 ? (
          <ol className="mt-3 flex flex-col gap-1 text-xs" aria-label="WS 接收段">
            {feed.segments.map((segment) => (
              <li key={segment.segment} className="flex flex-wrap gap-x-3">
                <span className="font-mono font-medium">
                  接收段 #{segment.segment}
                </span>
                <span
                  className={
                    segment.state === "receiving"
                      ? "text-status-good"
                      : "text-status-critical"
                  }
                >
                  {segment.state === "receiving" ? "接收中" : "已中断"}
                </span>
                <span className="text-muted-foreground">
                  始于 {segment.startedAt}
                </span>
                {segment.endedAt ? (
                  <span className="text-muted-foreground">
                    结束于 {segment.endedAt}
                  </span>
                ) : null}
                <span className="text-faint-foreground">
                  {segment.eventCount} 条事件
                </span>
              </li>
            ))}
          </ol>
        ) : null}

        {feed.lagNotices.length > 0 ? (
          <div className="mt-3 text-xs">
            <p className="font-medium text-status-warning">服务端 lag_notice</p>
            <ul className="mt-1 flex flex-col gap-0.5 text-muted-foreground">
              {feed.lagNotices.map((notice, index) => (
                <li key={`${notice.segment}-${index}`}>
                  <span className="font-mono font-medium">
                    接收段 #{notice.segment}
                  </span>{" "}
                  服务端丢弃 {notice.dropped} 条（自{" "}
                  <span className="font-mono">{notice.since}</span>
                  ，界面接收于 {notice.receivedAt}）
                </li>
              ))}
            </ul>
          </div>
        ) : null}

        {feed.protocolIssues.length > 0 ? (
          <div className="mt-3 text-xs">
            <p className="font-medium text-status-warning">协议问题</p>
            <ul className="mt-1 flex flex-col gap-0.5 text-muted-foreground">
              {feed.protocolIssues.map((issue, index) => (
                <li key={`${issue.segment}-${issue.code}-${index}`}>
                  <span className="font-mono font-medium">
                    接收段 #{issue.segment}
                  </span>{" "}
                  <span className="font-mono">{issue.code}</span>{" "}
                  <span data-raw-evidence="true">{issue.message}</span>
                </li>
              ))}
            </ul>
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
