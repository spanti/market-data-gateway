import { useQuery } from "@tanstack/react-query";
import { fetchAuditTimeline } from "@/api/endpoints/audit";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import {
  Panel,
  PanelDescription,
  PanelHeader,
  PanelTitle,
} from "@/components/ui/panel";
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
    <Panel
      className="mt-4 p-4"
      aria-label="操作时间线"
      data-testid="audit-operation-timeline"
    >
      <PanelHeader>
        <div>
          <PanelTitle>操作时间线</PanelTitle>
          <PanelDescription>
            Operation ID <code className="font-mono">{operationId}</code>
            ；按 sequence 升序展示追加事件，不在原事件上原地更新。
          </PanelDescription>
        </div>
        <Button
          size="sm"
          variant="outline"
          onClick={() => void query.refetch()}
          disabled={query.isFetching}
        >
          刷新时间线
        </Button>
      </PanelHeader>

      <div className="mt-3">
        {query.isPending ? (
          <LoadingState label="正在加载操作时间线…" />
        ) : query.isError ? (
          <ErrorState
            title="操作时间线加载失败"
            description={
              query.error instanceof Error
                ? query.error.message
                : "无法确认关联事件，请重试。"
            }
            onRetry={() => void query.refetch()}
          />
        ) : query.data.length === 0 ? (
          <EmptyState title="没有关联的审计事件" />
        ) : (
          <ol aria-label="操作事件序列" className="flex flex-col">
            {query.data.map((event, index) => (
              <li
                key={event.event_id}
                data-testid="audit-timeline-item"
                className="border-t border-border py-3 first:border-t-0 first:pt-0 last:pb-0"
              >
                <div className="flex flex-wrap items-center gap-2">
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
                  <code className="font-mono text-xs">{event.action}</code>
                </div>
                <div className="mt-1 text-xs text-muted-foreground">
                  {event.actor.display_name} ·{" "}
                  {event.actor.access_role ?? "无 Access Role"}
                  {event.reason ? ` · 原因：${event.reason}` : ""}
                </div>
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
      </div>
    </Panel>
  );
}
