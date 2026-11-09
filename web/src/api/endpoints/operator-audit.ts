import { ADMIN_PATHS } from "@/api/endpoints/admin";
import type {
  OperatorAuditEvent,
  AuditResult,
  AdminCursorPage,
} from "@/api/endpoints/admin";
import { apiClient } from "@/api/http/client";
import { HttpError } from "@/api/http/types";
import { isAccessRole } from "@/runtime/roles";
import type { AccessRole } from "@/runtime/roles";

/**
 * CONTRACT TARGET — NOT IMPLEMENTED BY THE CURRENT BACKEND.
 *
 * Operator Audit Event query adapter. Filters are server-side, pagination is
 * opaque and bounded, and every event is immutable. Only contract tests are
 * possible until the Admin API exists.
 */

export const AUDIT_PAGE_DEFAULT = 50;
export const AUDIT_PAGE_MAX = 200;
export const MAX_TIMELINE_PAGES = 5;

export const AUDIT_RESULTS: readonly AuditResult[] = [
  "requested",
  "succeeded",
  "failed",
  "denied",
];

export interface AuditFilters {
  /** UTC ISO-8601; from is inclusive, to is exclusive. */
  from: string | null;
  to: string | null;
  actorSubject: string | null;
  accessRole: AccessRole | null;
  action: string | null;
  targetType: string | null;
  targetId: string | null;
  result: AuditResult | null;
  requestId: string | null;
}

export const EMPTY_AUDIT_FILTERS: AuditFilters = {
  from: null,
  to: null,
  actorSubject: null,
  accessRole: null,
  action: null,
  targetType: null,
  targetId: null,
  result: null,
  requestId: null,
};

export interface AuditQueryOptions {
  cursor?: string | null;
  limit?: number;
  order?: "asc" | "desc";
}

function clean(value: string | null | undefined): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

export function clampAuditLimit(limit: number | undefined): number {
  const raw = Number.isFinite(limit) ? Math.trunc(limit as number) : AUDIT_PAGE_DEFAULT;
  return Math.min(AUDIT_PAGE_MAX, Math.max(1, raw));
}

/**
 * Maps UI filter names to the contract query names. Blank values are omitted;
 * the cursor stays opaque and is never parsed by the frontend.
 */
export function buildAuditQueryParams(
  filters: AuditFilters,
  options: AuditQueryOptions = {},
): URLSearchParams {
  const params = new URLSearchParams();

  const entries: Array<[string, string | null]> = [
    ["from", clean(filters.from)],
    ["to", clean(filters.to)],
    ["actor_subject", clean(filters.actorSubject)],
    ["access_role", filters.accessRole],
    ["action", clean(filters.action)],
    ["target_type", clean(filters.targetType)],
    ["target_id", clean(filters.targetId)],
    ["result", filters.result],
    ["request_id", clean(filters.requestId)],
  ];
  for (const [key, value] of entries) {
    if (value) {
      params.set(key, value);
    }
  }

  if (options.order) {
    params.set("order", options.order);
  }
  const cursor = clean(options.cursor);
  if (cursor) {
    params.set("cursor", cursor);
  }
  params.set("limit", String(clampAuditLimit(options.limit)));
  return params;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nullableString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function isAuditResult(value: unknown): value is AuditResult {
  return (
    typeof value === "string" &&
    (AUDIT_RESULTS as readonly string[]).includes(value)
  );
}

function parseEvent(value: unknown): OperatorAuditEvent | null {
  if (!isRecord(value)) {
    return null;
  }
  const actor = value.actor;
  const target = value.target;
  const diff = value.diff;
  const hashChain = value.hash_chain;

  if (
    typeof value.event_id !== "string" ||
    typeof value.sequence !== "number" ||
    !Number.isFinite(value.sequence) ||
    typeof value.occurred_at !== "string" ||
    typeof value.action !== "string" ||
    !isAuditResult(value.result) ||
    typeof value.request_id !== "string" ||
    !isRecord(actor) ||
    typeof actor.subject !== "string" ||
    !isRecord(target) ||
    typeof target.type !== "string" ||
    typeof target.id !== "string" ||
    !isRecord(diff) ||
    !isRecord(hashChain) ||
    typeof hashChain.algorithm !== "string" ||
    typeof hashChain.event_hash !== "string"
  ) {
    return null;
  }

  const redactedPaths = Array.isArray(diff.redacted_paths)
    ? diff.redacted_paths.filter(
        (path): path is string => typeof path === "string",
      )
    : [];

  return {
    event_id: value.event_id,
    sequence: value.sequence,
    occurred_at: value.occurred_at,
    actor: {
      subject: actor.subject,
      display_name:
        typeof actor.display_name === "string"
          ? actor.display_name
          : actor.subject,
      access_role: isAccessRole(actor.access_role)
        ? actor.access_role
        : null,
    },
    action: value.action,
    target: {
      type: target.type,
      id: target.id,
      display_name:
        typeof target.display_name === "string"
          ? target.display_name
          : target.id,
    },
    result: value.result,
    failure_reason: nullableString(value.failure_reason),
    reason: nullableString(value.reason),
    request_id: value.request_id,
    operation_id: nullableString(value.operation_id),
    job_id: nullableString(value.job_id),
    instance_id: nullableString(value.instance_id),
    diff: {
      before: diff.before ?? null,
      after: diff.after ?? null,
      redacted_paths: redactedPaths,
    },
    hash_chain: {
      algorithm: hashChain.algorithm,
      previous_event_hash: nullableString(hashChain.previous_event_hash),
      event_hash: hashChain.event_hash,
    },
  };
}

export interface OperatorAuditEventPage {
  items: OperatorAuditEvent[];
  page: AdminCursorPage;
}

export function parseOperatorAuditEventPage(data: unknown): OperatorAuditEventPage | null {
  if (!isRecord(data) || !Array.isArray(data.items) || !isRecord(data.page)) {
    return null;
  }
  const items: OperatorAuditEvent[] = [];
  for (const item of data.items) {
    const parsed = parseEvent(item);
    if (!parsed) {
      return null;
    }
    items.push(parsed);
  }
  const nextCursor = data.page.next_cursor;
  if (
    nextCursor !== null &&
    nextCursor !== undefined &&
    typeof nextCursor !== "string"
  ) {
    return null;
  }
  if (typeof data.page.has_more !== "boolean") {
    return null;
  }
  return {
    items,
    page: {
      next_cursor: nextCursor ?? null,
      has_more: data.page.has_more,
    },
  };
}

function payloadError(message: string): HttpError {
  return new HttpError({
    kind: "rest",
    status: null,
    code: "unexpected_audit_payload",
    message,
    details: {},
    requestId: null,
  });
}

export async function fetchOperatorAuditEvents(
  filters: AuditFilters,
  options: AuditQueryOptions = {},
): Promise<OperatorAuditEventPage> {
  const params = buildAuditQueryParams(filters, options);
  const response = await apiClient.get<unknown>(
    `${ADMIN_PATHS.auditEvents}?${params.toString()}`,
  );
  const page = parseOperatorAuditEventPage(response.data);
  if (!page) {
    throw payloadError("Operator Audit Event 响应不符合契约");
  }
  return page;
}

export async function fetchOperatorAuditEvent(eventId: string): Promise<OperatorAuditEvent> {
  const response = await apiClient.get<unknown>(
    `${ADMIN_PATHS.auditEvents}/${encodeURIComponent(eventId)}`,
  );
  const event = parseEvent(response.data);
  if (!event) {
    throw payloadError("Operator Audit Event 详情不符合契约");
  }
  return event;
}

/**
 * Assembled operation timeline: append-only events ordered by their monotonic
 * sequence. A repeated event_id is kept once (first append wins); an event is
 * never rendered as an in-place update of an earlier one.
 */
export function assembleTimeline(events: readonly OperatorAuditEvent[]): OperatorAuditEvent[] {
  const byId = new Map<string, OperatorAuditEvent>();
  for (const event of events) {
    if (!byId.has(event.event_id)) {
      byId.set(event.event_id, event);
    }
  }
  return [...byId.values()].sort(
    (a, b) =>
      a.sequence - b.sequence ||
      a.occurred_at.localeCompare(b.occurred_at) ||
      a.event_id.localeCompare(b.event_id),
  );
}

/**
 * Fetches one operation's events in ascending order, following at most
 * MAX_TIMELINE_PAGES opaque cursors. It fails explicitly instead of looping
 * when the server never stops reporting a next page.
 */
export async function fetchAuditTimeline(
  operationId: string,
): Promise<OperatorAuditEvent[]> {
  const collected: OperatorAuditEvent[] = [];
  let cursor: string | null = null;

  for (let pageIndex = 0; pageIndex < MAX_TIMELINE_PAGES; pageIndex += 1) {
    const params = new URLSearchParams({
      operation_id: operationId,
      order: "asc",
      limit: String(AUDIT_PAGE_MAX),
    });
    if (cursor) {
      params.set("cursor", cursor);
    }
    const response = await apiClient.get<unknown>(
      `${ADMIN_PATHS.auditEvents}?${params.toString()}`,
    );
    const page = parseOperatorAuditEventPage(response.data);
    if (!page) {
      throw payloadError("Operator Audit Event 时间线响应不符合契约");
    }
    collected.push(...page.items);
    if (!page.page.has_more) {
      return assembleTimeline(collected);
    }
    cursor = page.page.next_cursor;
  }

  throw new HttpError({
    kind: "rest",
    status: null,
    code: "timeline_page_limit",
    message: `操作时间线超过 ${MAX_TIMELINE_PAGES} 页，已停止以避免无限分页`,
    details: { operation_id: operationId },
    requestId: null,
  });
}
