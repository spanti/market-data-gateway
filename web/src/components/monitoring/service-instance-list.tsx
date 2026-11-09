import { Activity, Radio } from "lucide-react";
import type { ServiceRole } from "@/api/endpoints/admin";
import { StatusBadge } from "@/components/status-badge";
import { PanelDescription, PanelTitle } from "@/components/ui/panel";
import { formatDurationSeconds, formatUtcDateTime } from "@/lib/format";
import { cn } from "@/lib/utils";

export interface ServiceInstanceRow {
  role: ServiceRole;
  service: string | null;
  ready: boolean | null;
  readyReason: string | null;
  uptimeS: number | null;
  statusState: "loading" | "ready" | "error";
  lastSuccessAt: number | null;
  details: string[];
  errorMessage?: string | null;
}

export interface ServiceInstanceListProps {
  rows: ServiceInstanceRow[];
  className?: string;
}

function readinessBadge(row: ServiceInstanceRow) {
  if (row.statusState === "loading") {
    return <StatusBadge tone="unknown">读取中</StatusBadge>;
  }
  if (row.statusState === "error" || row.ready === null) {
    return <StatusBadge tone="unknown">未知</StatusBadge>;
  }
  return row.ready ? (
    <StatusBadge tone="good" live>
      Ready
    </StatusBadge>
  ) : (
    <StatusBadge tone="warning" live>
      未就绪
    </StatusBadge>
  );
}

/**
 * Only the three real Service Roles. There is no cluster topology, leader,
 * sharding or failover data in the contract, so none is rendered.
 */
export function ServiceInstanceList({
  rows,
  className,
}: ServiceInstanceListProps) {
  return (
    <section
      aria-label="Service Instance ledger"
      className={cn("border-t-2 border-foreground bg-card px-1 py-4", className)}
    >
      <div className="flex items-start justify-between gap-3 px-3">
        <div>
          <PanelTitle>Service Instance ledger</PanelTitle>
          <PanelDescription>三个 Service Role · 无集群推断</PanelDescription>
        </div>
        <Activity className="size-4 text-faint-foreground" aria-hidden="true" />
      </div>
      <div className="mt-2 divide-y divide-border">
        {rows.map((row) => (
          <div
            key={row.role}
            data-slot="service-instance"
            data-role={row.role}
            className="flex items-start justify-between gap-4 px-3 py-3 first:pt-0 last:pb-0"
          >
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <Radio
                  className="size-4 shrink-0 text-faint-foreground"
                  aria-hidden="true"
                />
                <span className="font-mono text-sm font-medium text-foreground">
                  {row.role}
                </span>
              </div>
              {row.statusState === "error" ? (
                <p className="mt-1 text-xs text-status-critical">
                  状态未知{row.errorMessage ? `：${row.errorMessage}` : ""}
                </p>
              ) : row.statusState === "loading" ? (
                <p className="mt-1 text-xs text-muted-foreground">
                  正在读取真实 /status…
                </p>
              ) : (
                <p className="mt-1 text-xs text-muted-foreground">
                  {row.details.join(" · ")}
                </p>
              )}
              <p className="mt-0.5 text-[11px] text-faint-foreground">
                Uptime{" "}
                {row.statusState === "ready"
                  ? formatDurationSeconds(row.uptimeS)
                  : "—"}{" "}
                · 最后成功：{formatUtcDateTime(row.lastSuccessAt)}
              </p>
              {row.ready === false && row.readyReason ? (
                <p className="mt-0.5 text-[11px] text-faint-foreground">
                  {row.readyReason}
                </p>
              ) : null}
            </div>
            <div className="shrink-0 text-right">{readinessBadge(row)}</div>
          </div>
        ))}
      </div>
    </section>
  );
}
