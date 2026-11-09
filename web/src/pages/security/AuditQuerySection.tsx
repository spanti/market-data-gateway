import { useQuery } from "@tanstack/react-query";
import { useMemo, useState, type FormEvent } from "react";
import {
  AUDIT_PAGE_DEFAULT,
  AUDIT_RESULTS,
  EMPTY_AUDIT_FILTERS,
  fetchAuditEvents,
  type AuditFilters,
} from "@/api/endpoints/audit";
import type { AuditEvent, AuditResult } from "@/api/endpoints/admin";
import { classifyHttpError } from "@/api/http/errors";
import { HttpError } from "@/api/http/types";
import { ErrorState, LoadingState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Pagination } from "@/components/pagination";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Panel,
  PanelDescription,
  PanelHeader,
  PanelTitle,
} from "@/components/ui/panel";
import { Select } from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  parseDateTimeInput,
  toQueryInstant,
} from "@/lib/time-range";
import { AuditEventDetail } from "@/pages/security/AuditEventDetail";
import {
  AUDIT_RESULT_LABEL,
  AUDIT_RESULT_TONE,
  formatAuditTimestamp,
} from "@/pages/security/audit-presentation";
import { ACCESS_ROLES, isAccessRole } from "@/runtime/roles";

const AUDIT_QUERY_ROOT = ["admin", "audit-events"] as const;
const MAX_CURSOR_HISTORY = 50;

interface FilterFormState {
  from: string;
  to: string;
  actorSubject: string;
  accessRole: string;
  action: string;
  targetType: string;
  targetId: string;
  result: string;
  requestId: string;
}

const EMPTY_FORM: FilterFormState = {
  from: "",
  to: "",
  actorSubject: "",
  accessRole: "",
  action: "",
  targetType: "",
  targetId: "",
  result: "",
  requestId: "",
};

function clean(value: string): string | null {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function buildFilters(form: FilterFormState): {
  filters: AuditFilters | null;
  error: string | null;
} {
  const from = form.from ? parseDateTimeInput(form.from, "utc") : null;
  const to = form.to ? parseDateTimeInput(form.to, "utc") : null;
  if (form.from && !from) {
    return { filters: null, error: "开始时间无效，请输入 UTC 时间。" };
  }
  if (form.to && !to) {
    return { filters: null, error: "结束时间无效，请输入 UTC 时间。" };
  }
  if (from && to && from.getTime() >= to.getTime()) {
    return { filters: null, error: "开始时间必须早于结束时间。" };
  }
  const role = form.accessRole.trim();
  const result = form.result.trim();
  return {
    filters: {
      from: from ? toQueryInstant(from) : null,
      to: to ? toQueryInstant(to) : null,
      actorSubject: clean(form.actorSubject),
      accessRole: isAccessRole(role) ? role : null,
      action: clean(form.action),
      targetType: clean(form.targetType),
      targetId: clean(form.targetId),
      result: (AUDIT_RESULTS as readonly string[]).includes(result)
        ? (result as AuditResult)
        : null,
      requestId: clean(form.requestId),
    },
    error: null,
  };
}

function AuditQueryError({
  error,
  onRetry,
}: {
  error: unknown;
  onRetry: () => void;
}) {
  if (!(error instanceof HttpError)) {
    return (
      <ErrorState
        title="审计查询失败"
        description="无法确认查询结果，请重试。"
        onRetry={onRetry}
      />
    );
  }

  const directive = classifyHttpError(error);
  if (directive.action === "permission-denied") {
    const missing = directive.requiredRole
      ? `缺少权限：需要 ${directive.requiredRole} 角色`
      : "缺少执行该操作的权限";
    return (
      <ErrorState
        forbidden
        title="没有权限"
        description={
          <>
            <div>{missing}。</div>
            <div className="mt-1">
              会话仍然有效，未退出登录。前端禁用只是界面约束，服务端授权才是安全边界。
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
        title="审计查询失败"
        description="审计查询响应不符合契约，未渲染任何部分数据。"
        requestId={error.requestId}
        onRetry={onRetry}
      />
    );
  }

  if (error.code === "audit_unavailable") {
    return (
      <ErrorState
        title="审计不可用"
        description="审计存储当前不可用；查询未执行，请稍后重试。"
        requestId={error.requestId}
        onRetry={onRetry}
      />
    );
  }

  return (
    <ErrorState
      title="审计查询失败"
      description={
        error.code === "invalid_parameter"
          ? "筛选参数无效，请检查时间范围、Access Role 与结果值。"
          : error.message
      }
      requestId={error.requestId}
      onRetry={onRetry}
    />
  );
}

/**
 * Server-side filtered Operator Audit Event query. No edit, delete or export
 * controls exist in v1; pagination is cursor-based and bounded.
 */
export function AuditQuerySection() {
  const [form, setForm] = useState<FilterFormState>(EMPTY_FORM);
  const [filters, setFilters] = useState<AuditFilters>(EMPTY_AUDIT_FILTERS);
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [formError, setFormError] = useState<string | null>(null);
  const [queryVersion, setQueryVersion] = useState(0);
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null);

  const cursor = cursors[cursors.length - 1] ?? null;
  const queryKey = useMemo(
    () => [...AUDIT_QUERY_ROOT, filters, cursor, queryVersion],
    [filters, cursor, queryVersion],
  );

  const query = useQuery({
    queryKey,
    queryFn: () =>
      fetchAuditEvents(filters, {
        cursor,
        limit: AUDIT_PAGE_DEFAULT,
        order: "desc",
      }),
  });

  function update<K extends keyof FilterFormState>(
    key: K,
    value: FilterFormState[K],
  ) {
    setForm((current) => ({ ...current, [key]: value }));
  }

  function apply(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const result = buildFilters(form);
    if (!result.filters) {
      setFormError(result.error);
      return;
    }
    setFormError(null);
    setFilters(result.filters);
    setCursors([null]);
    setSelectedEventId(null);
    setQueryVersion((version) => version + 1);
  }

  function reset() {
    setForm(EMPTY_FORM);
    setFormError(null);
    setFilters(EMPTY_AUDIT_FILTERS);
    setCursors([null]);
    setSelectedEventId(null);
    setQueryVersion((version) => version + 1);
  }

  const page = query.data ?? null;
  const items: AuditEvent[] = page?.items ?? [];

  return (
    <Panel className="mt-6 p-5" aria-label="Operator Audit Event 查询">
      <PanelHeader>
        <div>
          <PanelTitle>Operator Audit Event 查询</PanelTitle>
          <PanelDescription>
            服务端筛选与游标分页；from 为闭区间、to 为开区间，时间按 UTC
            解释。首版不提供编辑、删除或导出。
          </PanelDescription>
        </div>
        <span className="text-[11px] text-faint-foreground">
          只读审计记录 · 请求 ID 可关联技术支持
        </span>
      </PanelHeader>

      <form
        onSubmit={apply}
        aria-label="审计筛选条件"
        className="mt-4 grid grid-cols-1 gap-3 border-y border-border py-4 sm:grid-cols-2 lg:grid-cols-4"
      >
        <div className="flex flex-col gap-1">
          <Label htmlFor="audit-from">开始时间（UTC，含）</Label>
          <Input
            id="audit-from"
            type="datetime-local"
            value={form.from}
            onChange={(event) => update("from", event.target.value)}
          />
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="audit-to">结束时间（UTC，不含）</Label>
          <Input
            id="audit-to"
            type="datetime-local"
            value={form.to}
            onChange={(event) => update("to", event.target.value)}
          />
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="audit-actor">操作者 Subject</Label>
          <Input
            id="audit-actor"
            value={form.actorSubject}
            onChange={(event) => update("actorSubject", event.target.value)}
            placeholder="oidc-subject-123"
            autoComplete="off"
            spellCheck={false}
          />
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="audit-role">Access Role（筛选）</Label>
          <Select
            id="audit-role"
            value={form.accessRole}
            onChange={(event) => update("accessRole", event.target.value)}
          >
            <option value="">全部角色</option>
            {ACCESS_ROLES.map((role) => (
              <option key={role} value={role}>
                {role}
              </option>
            ))}
          </Select>
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="audit-action">动作</Label>
          <Input
            id="audit-action"
            value={form.action}
            onChange={(event) => update("action", event.target.value)}
            placeholder="feed_subscription.update"
            autoComplete="off"
            spellCheck={false}
          />
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="audit-target-type">对象类型</Label>
          <Input
            id="audit-target-type"
            value={form.targetType}
            onChange={(event) => update("targetType", event.target.value)}
            placeholder="feed_subscription"
            autoComplete="off"
            spellCheck={false}
          />
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="audit-target-id">对象 ID</Label>
          <Input
            id="audit-target-id"
            value={form.targetId}
            onChange={(event) => update("targetId", event.target.value)}
            placeholder="fsub_01JABCXYZ"
            autoComplete="off"
            spellCheck={false}
          />
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="audit-result">结果</Label>
          <Select
            id="audit-result"
            value={form.result}
            onChange={(event) => update("result", event.target.value)}
          >
            <option value="">全部结果</option>
            {AUDIT_RESULTS.map((result) => (
              <option key={result} value={result}>
                {AUDIT_RESULT_LABEL[result]}
              </option>
            ))}
          </Select>
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="audit-request-id">Request ID</Label>
          <Input
            id="audit-request-id"
            value={form.requestId}
            onChange={(event) => update("requestId", event.target.value)}
            placeholder="req_01JABCXYZ"
            autoComplete="off"
            spellCheck={false}
          />
        </div>
        <div className="flex items-end gap-2">
          <Button type="submit" size="sm">
            查询
          </Button>
          <Button type="button" size="sm" variant="outline" onClick={reset}>
            重置
          </Button>
        </div>
        {formError ? (
          <p role="alert" className="text-xs text-status-critical sm:col-span-2 lg:col-span-4">
            {formError}
          </p>
        ) : null}
      </form>

      <div className="mt-4">
        {query.isPending ? (
          <LoadingState label="正在查询 Operator Audit Event…" />
        ) : query.isError ? (
          <AuditQueryError error={query.error} onRetry={() => void query.refetch()} />
        ) : items.length === 0 ? (
          <div className="px-4 py-8 text-center text-xs text-muted-foreground">
            没有匹配的事件
          </div>
        ) : (
          <>
            <div data-testid="audit-event-table">
              <Table>
                <caption className="sr-only">
                  Operator Audit Event 查询结果，按 sequence 降序
                </caption>
                <TableHeader>
                  <TableRow>
                    <TableHead className="text-right">Sequence</TableHead>
                    <TableHead>时间（UTC）</TableHead>
                    <TableHead>操作者</TableHead>
                    <TableHead>Access Role</TableHead>
                    <TableHead>动作</TableHead>
                    <TableHead>对象</TableHead>
                    <TableHead>结果</TableHead>
                    <TableHead>Request ID</TableHead>
                    <TableHead>详情</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {items.map((event) => (
                    <TableRow key={event.event_id}>
                      <TableCell className="text-right tabular-nums">
                        {event.sequence}
                      </TableCell>
                      <TableCell className="whitespace-nowrap tabular-nums">
                        {formatAuditTimestamp(event.occurred_at)}
                      </TableCell>
                      <TableCell>
                        <div className="font-medium">
                          {event.actor.display_name}
                        </div>
                        <div className="font-mono text-[11px] text-faint-foreground">
                          {event.actor.subject}
                        </div>
                      </TableCell>
                      <TableCell>{event.actor.access_role ?? "—"}</TableCell>
                      <TableCell className="font-mono">{event.action}</TableCell>
                      <TableCell>
                        <div className="font-mono text-[11px]">
                          {event.target.type}:{event.target.id}
                        </div>
                        <div className="text-faint-foreground">
                          {event.target.display_name}
                        </div>
                      </TableCell>
                      <TableCell>
                        <StatusBadge tone={AUDIT_RESULT_TONE[event.result]}>
                          {AUDIT_RESULT_LABEL[event.result]}
                        </StatusBadge>
                      </TableCell>
                      <TableCell className="font-mono text-[11px]">
                        {event.request_id}
                      </TableCell>
                      <TableCell>
                        <Button
                          size="sm"
                          variant="outline"
                          aria-label={`查看详情 ${event.event_id}`}
                          onClick={() => setSelectedEventId(event.event_id)}
                        >
                          查看详情
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>

            <div className="mt-3 flex flex-wrap items-center justify-between gap-3">
              <Pagination
                hasMore={Boolean(page?.page.has_more && page.page.next_cursor)}
                hasPrevious={cursors.length > 1}
                onNext={() => {
                  const next = page?.page.next_cursor;
                  if (!next) {
                    return;
                  }
                  setCursors((current) =>
                    [...current.slice(-(MAX_CURSOR_HISTORY - 1)), next],
                  );
                }}
                onPrevious={() => {
                  setCursors((current) =>
                    current.length > 1 ? current.slice(0, -1) : current,
                  );
                }}
                label={`本页 ${items.length} 条 · 游标分页，不提供总数`}
              />
              {page?.page.has_more && !page.page.next_cursor ? (
                <span className="text-[11px] text-status-critical">
                  服务端声明还有更多事件但未返回 next_cursor，已停止分页。
                </span>
              ) : null}
            </div>
          </>
        )}
      </div>

      {selectedEventId ? (
        <AuditEventDetail
          eventId={selectedEventId}
          onClose={() => setSelectedEventId(null)}
        />
      ) : null}
    </Panel>
  );
}
