import { ShieldCheck } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { fetchOperatorAuditEvent } from "@/api/endpoints/operator-audit";
import { HttpError } from "@/api/http/types";
import { HelpTrigger } from "@/components/help-trigger";
import { ErrorState, LoadingState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { AuditDiff } from "@/pages/security/AuditDiff";
import { AuditOperationTimeline } from "@/pages/security/AuditOperationTimeline";
import {
  AUDIT_RESULT_LABEL,
  AUDIT_RESULT_TONE,
  formatAuditTimestamp,
} from "@/pages/security/audit-presentation";

function Field({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="min-w-0">
      <dt className="text-faint-foreground">{label}</dt>
      <dd className="mt-1 break-words">{children}</dd>
    </div>
  );
}

function DetailError({
  error,
  onRetry,
}: {
  error: unknown;
  onRetry: () => void;
}) {
  if (!(error instanceof HttpError)) {
    return (
      <ErrorState
        title="事件详情加载失败"
        description="无法确认事件详情，请重试。"
        onRetry={onRetry}
      />
    );
  }
  if (error.status === 403) {
    const required =
      typeof error.details.required_role === "string"
        ? `需要 ${error.details.required_role} 角色`
        : "缺少读取该事件的权限";
    return (
      <ErrorState
        forbidden
        title="没有权限"
        description={
          <>
            <div>缺少权限，需要 {required}。</div>
            <div className="mt-1">
              会话仍然有效。前端禁用只是界面约束，服务端授权才是安全边界。
            </div>
          </>
        }
        requestId={error.requestId}
        onRetry={onRetry}
      />
    );
  }
  if (error.code === "unexpected_audit_payload") {
    return (
      <ErrorState
        title="事件详情加载失败"
        description="事件详情响应不符合契约，未渲染部分数据。"
        requestId={error.requestId}
        onRetry={onRetry}
      />
    );
  }
  return (
    <ErrorState
      title="事件详情加载失败"
      description={
        <span data-raw-evidence="true">{error.message}</span>
      }
      requestId={error.requestId}
      onRetry={onRetry}
    />
  );
}

/**
 * Immutable audit event snapshot. Hash-chain fields are metadata only; the UI
 * makes no WORM claim and exposes no edit/delete/export action.
 */
export function OperatorAuditEventDetail({
  eventId,
  onClose,
}: {
  eventId: string;
  onClose: () => void;
}) {
  const query = useQuery({
    queryKey: ["admin", "audit-event", eventId],
    queryFn: () => fetchOperatorAuditEvent(eventId),
  });

  return (
    <Card
      role="region"
      aria-label="事件详情"
      className="mt-4"
    >
      <CardHeader className="p-5 pb-0">
        <div className="flex min-w-0 items-start gap-2.5">
          <span className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-accent-soft text-accent-strong">
            <ShieldCheck className="size-4" aria-hidden="true" />
          </span>
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-1.5">
              <CardTitle>事件详情</CardTitle>
              <HelpTrigger
                label="事件详情说明"
                content="不可变审计事件快照。脱敏值固定为 [REDACTED]，前端不重建隐藏值。哈希链仅作为元数据展示，不宣称不可篡改存储；首版不提供哈希校验 UI。"
                side="bottom"
              />
            </div>
          </div>
        </div>
        <Button size="sm" variant="outline" onClick={onClose}>
          关闭详情
        </Button>
      </CardHeader>

      <CardContent className="p-5 pt-4">
        {query.isPending ? (
          <LoadingState label="正在加载事件详情…" />
        ) : query.isError ? (
          <DetailError error={query.error} onRetry={() => void query.refetch()} />
        ) : (
          <>
            <dl className="grid grid-cols-1 gap-x-6 gap-y-3 text-xs sm:grid-cols-2 lg:grid-cols-3">
              <Field label="Sequence">
                <span className="tabular-nums">{query.data.sequence}</span>
              </Field>
              <Field label="事件时间">
                <span className="tabular-nums">
                  {formatAuditTimestamp(query.data.occurred_at)}
                </span>
              </Field>
              <Field label="结果">
                <StatusBadge tone={AUDIT_RESULT_TONE[query.data.result]}>
                  {AUDIT_RESULT_LABEL[query.data.result]}
                </StatusBadge>
              </Field>
              <Field label="操作者">
                <div className="font-medium">
                  {query.data.actor.display_name}
                </div>
                <div className="font-mono text-[11px] text-faint-foreground">
                  {query.data.actor.subject}
                </div>
              </Field>
              <Field label="Access Role">
                {query.data.actor.access_role ?? "—"}
              </Field>
              <Field label="动作">
                <code className="font-mono break-all">{query.data.action}</code>
              </Field>
              <Field label="对象">
                <div className="font-mono text-[11px] break-all">
                  {query.data.target.type}:{query.data.target.id}
                </div>
                <div className="text-faint-foreground">
                  {query.data.target.display_name}
                </div>
              </Field>
              <Field label="原因">
                {query.data.reason === null ? (
                  "—"
                ) : (
                  <span
                    data-raw-evidence="true"
                    className="break-words"
                  >
                    {query.data.reason}
                  </span>
                )}
              </Field>
              <Field label="失败原因">
                {query.data.failure_reason === null ? (
                  "—"
                ) : (
                  <span
                    data-raw-evidence="true"
                    className="break-words"
                  >
                    {query.data.failure_reason}
                  </span>
                )}
              </Field>
              <Field label="Request ID">
                <span className="font-mono text-[11px] break-all">
                  {query.data.request_id}
                </span>
              </Field>
              <Field label="Operation ID">
                <span className="font-mono text-[11px] break-all">
                  {query.data.operation_id ?? "—"}
                </span>
              </Field>
              <Field label="Job ID">
                <span className="font-mono text-[11px] break-all">
                  {query.data.job_id ?? "—"}
                </span>
              </Field>
              <Field label="Instance ID">
                <span className="font-mono text-[11px] break-all">
                  {query.data.instance_id ?? "—"}
                </span>
              </Field>
            </dl>

            <section
              className="mt-5 rounded-lg bg-surface-sunken p-3"
              aria-label="哈希链元数据"
            >
              <h3 className="text-xs font-semibold text-foreground">
                哈希链元数据
              </h3>
              <dl className="mt-2 grid grid-cols-1 gap-x-6 gap-y-2 text-[11px] sm:grid-cols-3">
                <Field label="算法">
                  <span className="font-mono">
                    {query.data.hash_chain.algorithm}
                  </span>
                </Field>
                <Field label="上一事件哈希">
                  <span className="font-mono break-all">
                    {query.data.hash_chain.previous_event_hash ?? "null"}
                  </span>
                </Field>
                <Field label="事件哈希">
                  <span className="font-mono break-all">
                    {query.data.hash_chain.event_hash}
                  </span>
                </Field>
              </dl>
              <p className="mt-2 text-[11px] text-faint-foreground">
                哈希链仅作为元数据展示，不宣称不可篡改存储；首版不提供哈希校验
                UI。
              </p>
            </section>

            <section
              className="mt-4 rounded-lg bg-surface-sunken p-3"
              aria-label="变更 Diff"
            >
              <h3 className="text-xs font-semibold text-foreground">
                变更 Diff
              </h3>
              <AuditDiff
                before={query.data.diff.before}
                after={query.data.diff.after}
                redactedPaths={query.data.diff.redacted_paths}
              />
            </section>

            {query.data.operation_id ? (
              <AuditOperationTimeline operationId={query.data.operation_id} />
            ) : null}
          </>
        )}
      </CardContent>
    </Card>
  );
}
