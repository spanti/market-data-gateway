import { BellRing } from "lucide-react";
import type * as React from "react";
import {
  HelpTrigger,
  type HelpTriggerSide,
} from "@/components/help-trigger";
import { EmptyState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type {
  MonitoringAlert,
  MonitoringAlertTone,
} from "@/pages/monitoring/monitoring-alerts";
import { cn } from "@/lib/utils";

const TONE_LABEL: Record<MonitoringAlertTone, string> = {
  critical: "严重",
  warning: "警告",
  info: "信息",
};

export interface AlertsCardProps {
  alerts: MonitoringAlert[];
  help?: { label: string; content: React.ReactNode; side?: HelpTriggerSide };
  className?: string;
}

/**
 * In-page alert timeline. Every item is passed in from a real loaded
 * condition or a real query error; this component never fabricates one.
 */
export function AlertsCard({ alerts, help, className }: AlertsCardProps) {
  return (
    <Card
      data-slot="alerts-card"
      role="region"
      aria-label="页面内告警"
      className={cn("min-w-0 p-5", className)}
    >
      <CardHeader className="p-0">
        <div className="flex min-w-0 items-center gap-2">
          <span className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-accent-soft text-accent-strong">
            <BellRing className="size-4" aria-hidden="true" />
          </span>
          <CardTitle>页面内告警</CardTitle>
          {help ? (
            <HelpTrigger
              label={help.label}
              content={help.content}
              side={help.side ?? "bottom"}
            />
          ) : null}
        </div>
        <span className="font-mono text-[11px] tabular-nums text-faint-foreground">
          {alerts.length} 条
        </span>
      </CardHeader>
      <CardContent className="p-0 pt-3">
        {alerts.length === 0 ? (
          <EmptyState
            title="当前没有页面内告警"
            description="只显示已加载的真实异常与错误，不生成模拟告警。"
          />
        ) : (
          <div className="space-y-2.5">
            {alerts.map((alert) => (
              <div
                key={alert.id}
                data-alert-tone={alert.tone}
                className="rounded-lg bg-surface-sunken p-3"
              >
                <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
                  <StatusBadge tone={alert.tone}>
                    {TONE_LABEL[alert.tone]}
                  </StatusBadge>
                  {alert.evidence ? (
                    <span className="text-[11px] text-faint-foreground">
                      {alert.evidence}
                    </span>
                  ) : null}
                </div>
                <strong className="mt-1.5 block break-words text-xs font-semibold leading-snug text-foreground">
                  {alert.title}
                </strong>
                {alert.detail ? (
                  <p className="mt-1 break-words text-[11px] leading-relaxed text-muted-foreground">
                    {alert.detail}
                  </p>
                ) : null}
                {alert.rawError ? (
                  <p className="mt-1 break-words text-[11px] leading-relaxed text-status-critical-fg">
                    <span className="text-muted-foreground">Raw error </span>
                    <span data-raw-evidence="true">{alert.rawError}</span>
                  </p>
                ) : null}
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
