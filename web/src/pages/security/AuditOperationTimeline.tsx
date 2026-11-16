import { History } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { fetchAuditTimeline } from "@/api/endpoints/operator-audit";
import { HelpTrigger } from "@/components/help-trigger";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { AuditDiff } from "@/pages/security/AuditDiff";
import {
  AUDIT_RESULT_LABEL,
  AUDIT_RESULT_TONE,
  formatAuditTimestamp,
} from "@/pages/security/audit-presentation";

function hasDiffSide(value: unknown): boolean {
  return value !== null && value !== undefined;
}

/**
 * Append-only operation timeline. Every event is its own entry ordered by
 * sequence; a terminal event never rewrites the earlier `requested` entry.
 */
export function AuditOperationTimeline({
  operationId,
}: {
  operationId: string;
}) {
  const query = useQuery({
    queryKey: ["admin", "audit-timeline", operationId],
    queryFn: () => fetchAuditTimeline(operationId),
  });

  return (
    <Card
      role="region"
      aria-label="操作时间线"
      data-testid="audit-operation-timeline"
      className="mt-4"
    >
      <CardHeader className="p-4">
        <div className="flex min-w-0 items-start gap-2.5">
          <span className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-accent-soft text-accent-strong">
            <History className="size-4" aria-hidden="true" />
          </span>
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-1.5">
              <CardTitle>操作时间线</CardTitle>
              <HelpTrigger
                label="操作时间线说明"
                content="按 sequence 升序展示追加事件，不在原事件上原地更新。每个事件保持自己的快照。"
                side="bottom"
              />
            </div>
            <p className="mt-1 break-all text-[11px] text-muted-foreground">
              Operation ID <code className="font-mono">{operationId}</code>
            </p>
          </div>
        </div>
        <Button
          size="sm"
          variant="outline"
          onClick={() => void query.refetch()}
          disabled={query.isFetching}
        >
          刷新时间线
        </Button>
      </CardHeader>

      <CardContent className="p-4 pt-0">
        {query.isPending ? (
          <LoadingState label="正在加载操作时间线…" />
        ) : query.isError ? (
          <ErrorState
            title="操作时间线加载失败"
            description={
              query.error instanceof Error ? (
                <span data-raw-evidence="true">{query.error.message}</span>
              ) : (
                "无法确认关联事件，请重试。"
              )
            }
            onRetry={() => void query.refetch()}
          />
        ) : query.data.length === 0 ? (
          <EmptyState title="没有关联的审计事件" />
        ) : (
          <ol aria-label="操作事件序列" className="flex flex-col gap-2">
            {query.data.map((event, index) => (
              <li
                key={event.event_id}
                data-testid="audit-timeline-item"
                className="rounded-lg bg-surface-sunken p-3"
              >
                <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                  <span className="font-mono text-[11px] text-faint-foreground">
                    #{index + 1}
                  </span>
                  <span className="font-mono text-[11px]">
                    seq {event.sequence}
                  </span>
                  <time
                    dateTime={event.occurred_at}
                    className="tabular-nums text-[11px] text-faint-foreground"
                  >
                    {formatAuditTimestamp(event.occurred_at)}
                  </time>
                  <StatusBadge tone={AUDIT_RESULT_TONE[event.result]}>
                    {AUDIT_RESULT_LABEL[event.result]}
                  </StatusBadge>
                  <code className="break-all font-mono text-xs">
                    {event.action}
                  </code>
                </div>
                <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
                  <span>{event.actor.display_name}</span>
                  <span>
                    Access Role {event.actor.access_role ?? "无 Access Role"}
                  </span>
                </div>
                {event.reason ? (
                  <div className="mt-1 text-xs text-muted-foreground">
                    <span className="font-medium">原因</span>{" "}
                    <span data-raw-evidence="true" className="break-words">
                      {event.reason}
                    </span>
                  </div>
                ) : null}
                {hasDiffSide(event.diff.before) ||
                hasDiffSide(event.diff.after) ? (
                  <AuditDiff
                    before={event.diff.before}
                    after={event.diff.after}
                    redactedPaths={event.diff.redacted_paths}
                  />
                ) : null}
              </li>
            ))}
          </ol>
        )}
      </CardContent>
    </Card>
  );
}
