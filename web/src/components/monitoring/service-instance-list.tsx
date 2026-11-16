import { Radio } from "lucide-react";
import type * as React from "react";
import type { ServiceRole } from "@/api/endpoints/admin";
import {
  HelpTrigger,
  type HelpTriggerSide,
} from "@/components/help-trigger";
import {
  ErrorState,
  LoadingState,
  UnknownState,
} from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatDurationSeconds, formatUtcDateTime } from "@/lib/format";
import { cn } from "@/lib/utils";
import { IconTile } from "@/components/icon-tile";

export interface ServiceInstanceRow {
  role: ServiceRole;
  /** Concrete instance id when the contract exposes one; null otherwise. */
  instanceId?: string | null;
  /** The process/service name reported by the role, never the instance id. */
  service: string | null;
  ready: boolean | null;
  readyReason: string | null;
  uptimeS: number | null;
  statusState: "loading" | "ready" | "error";
  lastSuccessAt: number | null;
  details: string[];
  errorMessage?: string | null;
}

/**
 * Current Contract exposes one process per Service Role; Admin API exposes
 * concrete Service Instances. The card must name the rows it actually has.
 */
export type ServiceInstanceSourceKind = "role" | "instance";

export interface ServiceInstanceListProps {
  sourceKind: ServiceInstanceSourceKind;
  rows: ServiceInstanceRow[];
  /** Overall source state. Unknown means a real capability gap, not an error. */
  state?: "loading" | "ready" | "error" | "unknown";
  unknownReason?: React.ReactNode;
  lastSuccessAt?: number | null;
  errorMessage?: string | null;
  overallStatus?: React.ReactNode;
  help?: { label: string; content: React.ReactNode; side?: HelpTriggerSide };
  className?: string;
}

function ReadinessBadge({ row }: { row: ServiceInstanceRow }) {
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
 * Service Instance ledger. Only the three real Service Roles exist in the
 * contract, and one Role is not one instance: Role and Instance are labelled
 * separately with no cluster, leader or failover inference.
 */
export function ServiceInstanceList({
  sourceKind,
  rows,
  state = "ready",
  unknownReason,
  lastSuccessAt = null,
  errorMessage,
  overallStatus,
  help,
  className,
}: ServiceInstanceListProps) {
  const subject = sourceKind === "role" ? "Service Role" : "Service Instance";
  return (
    <Card
      data-slot="service-instance-list"
      role="region"
      aria-label={subject}
      className={cn("min-w-0 p-5", className)}
    >
      <CardHeader className="p-0">
        <div className="flex min-w-0 items-center gap-2">
          <IconTile icon={Radio} />
          <CardTitle>{subject}</CardTitle>
          {help ? (
            <HelpTrigger
              label={help.label}
              content={help.content}
              side={help.side ?? "bottom"}
            />
          ) : null}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {overallStatus}
          <span className="font-mono text-[11px] tabular-nums text-faint-foreground">
            {rows.length} 个 {subject}
          </span>
        </div>
      </CardHeader>
      <CardContent className="p-0 pt-3">
        {state === "unknown" ? (
          <UnknownState cause="not-exposed" reason={unknownReason} />
        ) : state === "loading" ? (
          <LoadingState label={`正在读取 ${subject} 状态`} showSkeleton />
        ) : state === "error" ? (
          <ErrorState
            className="mb-3"
            description={
              <span>
                {subject} 状态加载失败。最后成功{" "}
                {formatUtcDateTime(lastSuccessAt)}。旧快照不会继续显示为 Ready。
              </span>
            }
          />
        ) : null}

        {state === "unknown" ? null : rows.length === 0 ? (
          <p className="text-xs text-muted-foreground">
            当前没有可展示的 {subject} 数据。
          </p>
        ) : (
          <div className="space-y-2.5">
            {rows.map((row) => (
              <div
                key={row.role}
                role="group"
                aria-label={
                  row.instanceId
                    ? `${row.instanceId} ${row.role} Service Instance`
                    : `${row.role} Service Role`
                }
                data-slot="service-instance"
                data-role={row.role}
                data-state={row.statusState}
                className="rounded-lg bg-surface-sunken p-3"
              >
                <div className="flex flex-wrap items-start justify-between gap-x-3 gap-y-2">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                      <strong className="font-mono text-xs font-semibold">
                        {row.role}
                      </strong>
                      <span className="text-[10px] uppercase tracking-[0.08em] text-faint-foreground">
                        Service Role
                      </span>
                    </div>
                    <p className="mt-1 break-all text-[11px] text-muted-foreground">
                      <span className="text-faint-foreground">Instance </span>
                      {row.instanceId ?? "未暴露"}
                    </p>
                    <p className="mt-0.5 break-all text-[11px] text-muted-foreground">
                      <span className="text-faint-foreground">Service </span>
                      {row.service ?? "未暴露"}
                    </p>
                    {row.statusState === "error" ? (
                      <p className="mt-1 break-words text-[11px] text-status-critical-fg">
                        状态未知
                        {row.errorMessage ? (
                          <>
                            <span className="text-muted-foreground">
                              {" "}
                              Reason{" "}
                            </span>
                            <span data-raw-evidence="true">{row.errorMessage}</span>
                          </>
                        ) : null}
                      </p>
                    ) : row.statusState === "loading" ? (
                      <p className="mt-1 text-[11px] text-muted-foreground">
                        正在读取真实状态
                      </p>
                    ) : row.details.length > 0 ? (
                      <ul className="mt-1 space-y-0.5 text-[11px] text-muted-foreground">
                        {row.details.map((detail) => (
                          <li key={detail} className="break-words">
                            {detail}
                          </li>
                        ))}
                      </ul>
                    ) : null}
                    <div className="mt-1.5 flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-faint-foreground">
                      <span>
                        Uptime{" "}
                        {row.statusState === "ready"
                          ? formatDurationSeconds(row.uptimeS)
                          : "—"}
                      </span>
                      <span>最后成功 {formatUtcDateTime(row.lastSuccessAt)}</span>
                    </div>
                    {row.ready === false && row.readyReason ? (
                      <p className="mt-1 break-words text-[11px] text-status-warning-fg">
                        {row.readyReason}
                      </p>
                    ) : null}
                  </div>
                  <div className="shrink-0">
                    <ReadinessBadge row={row} />
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}

        {state === "error" && errorMessage ? (
          <p className="mt-2 break-words text-[11px] text-faint-foreground">
            <span className="text-muted-foreground">Raw error </span>
            <span data-raw-evidence="true">{errorMessage}</span>
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}
