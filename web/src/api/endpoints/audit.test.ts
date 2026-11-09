import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import { server } from "@/test/server";
import {
  AUDIT_PAGE_DEFAULT,
  AUDIT_PAGE_MAX,
  buildAuditQueryParams,
  fetchAuditEvent,
  fetchAuditEvents,
  fetchAuditTimeline,
  parseAuditEventPage,
  MAX_TIMELINE_PAGES,
  type AuditFilters,
} from "@/api/endpoints/audit";

const fullFilters: AuditFilters = {
  from: "2026-11-09T00:00:00.000Z",
  to: "2026-11-09T12:00:00.000Z",
  actorSubject: "oidc-subject-123",
  accessRole: "Operator",
  action: "feed_subscription.update",
  targetType: "feed_subscription",
  targetId: "fsub_01JABCXYZ",
  result: "succeeded",
  requestId: "req_01JABCXYZ",
};

export function auditEvent(overrides: Record<string, unknown> = {}) {
  return {
    event_id: "evt_01JABCXYZ",
    sequence: 1042,
    occurred_at: "2026-11-09T12:00:00.000000000Z",
    actor: {
      subject: "oidc-subject-123",
      display_name: "Ada Operator",
      access_role: "Operator",
    },
    action: "feed_subscription.update",
    target: {
      type: "feed_subscription",
      id: "fsub_01JABCXYZ",
      display_name: "binance:BTC/USDT 1m",
    },
    result: "succeeded",
    failure_reason: null,
    reason: "add BTC 1m subscription",
    request_id: "req_01JABCXYZ",
    operation_id: "op_01JABCXYZ",
    job_id: null,
    instance_id: null,
    diff: {
      before: { enabled: false, version: 2 },
      after: { enabled: true, version: 3 },
      redacted_paths: [],
    },
    hash_chain: {
      algorithm: "sha256",
      previous_event_hash: "abc",
      event_hash: "def",
    },
    ...overrides,
  };
}

describe("audit query serialization", () => {
  it("serializes every required filter with the contract parameter names", () => {
    const params = buildAuditQueryParams(fullFilters, {
      cursor: "next-page",
      limit: 100,
      order: "asc",
    });

    expect(Object.fromEntries(params)).toEqual({
      from: "2026-11-09T00:00:00.000Z",
      to: "2026-11-09T12:00:00.000Z",
      actor_subject: "oidc-subject-123",
      access_role: "Operator",
      action: "feed_subscription.update",
      target_type: "feed_subscription",
      target_id: "fsub_01JABCXYZ",
      result: "succeeded",
      request_id: "req_01JABCXYZ",
      cursor: "next-page",
      limit: "100",
      order: "asc",
    });
  });

  it("omits blank filters and never sends an undefined key", () => {
    const params = buildAuditQueryParams({
      from: null,
      to: "  ",
      actorSubject: "",
      accessRole: null,
      action: " ",
      targetType: null,
      targetId: null,
      result: null,
      requestId: "",
    });

    expect(Object.fromEntries(params)).toEqual({
      limit: String(AUDIT_PAGE_DEFAULT),
    });
  });

  it("clamps pagination to the contract bounds", () => {
    expect(
      buildAuditQueryParams(fullFilters, { limit: 0 }).get("limit"),
    ).toBe("1");
    expect(
      buildAuditQueryParams(fullFilters, { limit: 10_000 }).get("limit"),
    ).toBe(String(AUDIT_PAGE_MAX));
    expect(AUDIT_PAGE_DEFAULT).toBe(50);
    expect(AUDIT_PAGE_MAX).toBe(200);
  });

  it("does not emit a cursor when none is supplied", () => {
    const params = buildAuditQueryParams(fullFilters, {
      cursor: null,
      order: "desc",
    });

    expect(params.has("cursor")).toBe(false);
    expect(params.get("order")).toBe("desc");
  });
});

describe("audit response parsing", () => {
  it("parses a bounded page of events", () => {
    const page = parseAuditEventPage({
      items: [auditEvent()],
      page: { next_cursor: "cursor-2", has_more: true },
    });

    expect(page?.items).toHaveLength(1);
    expect(page?.items[0]?.event_id).toBe("evt_01JABCXYZ");
    expect(page?.items[0]?.diff.before).toEqual({ enabled: false, version: 2 });
    expect(page?.page).toEqual({ next_cursor: "cursor-2", has_more: true });
  });

  it("fails closed on malformed list payloads", () => {
    expect(parseAuditEventPage(null)).toBeNull();
    expect(parseAuditEventPage({ items: "nope", page: {} })).toBeNull();
    expect(
      parseAuditEventPage({
        items: [{ not: "an event" }],
        page: { next_cursor: null, has_more: false },
      }),
    ).toBeNull();
  });

  it("fetches a filtered page through the Admin API adapter", async () => {
    const seen: URL[] = [];
    server.use(
      http.get("*/api/v1/admin/audit-events", ({ request }) => {
        seen.push(new URL(request.url));
        return HttpResponse.json({
          items: [auditEvent()],
          page: { next_cursor: null, has_more: false },
        });
      }),
    );

    const page = await fetchAuditEvents(fullFilters, { limit: 25 });

    expect(page.items).toHaveLength(1);
    expect(seen[0]?.searchParams.get("actor_subject")).toBe("oidc-subject-123");
    expect(seen[0]?.searchParams.get("limit")).toBe("25");
  });

  it("fetches a single event by opaque id", async () => {
    let seenPath = "";
    server.use(
      http.get("*/api/v1/admin/audit-events/:eventId", ({ request, params }) => {
        seenPath = new URL(request.url).pathname;
        expect(params.eventId).toBe("evt_01JABCXYZ");
        return HttpResponse.json(auditEvent());
      }),
    );

    const event = await fetchAuditEvent("evt_01JABCXYZ");

    expect(event.event_id).toBe("evt_01JABCXYZ");
    expect(seenPath).toBe("/api/v1/admin/audit-events/evt_01JABCXYZ");
  });

  it("rejects a malformed event detail instead of rendering partial data", async () => {
    server.use(
      http.get("*/api/v1/admin/audit-events/:eventId", () =>
        HttpResponse.json({ event_id: "evt_only" }),
      ),
    );

    await expect(fetchAuditEvent("evt_only")).rejects.toMatchObject({
      code: "unexpected_audit_payload",
    });
  });
});

describe("operation timeline assembly", () => {
  it("orders appended events by sequence and follows the opaque cursor", async () => {
    const terminal = auditEvent({
      event_id: "evt_terminal",
      sequence: 1043,
      result: "succeeded",
      occurred_at: "2026-11-09T12:00:05.000000000Z",
      diff: { before: null, after: null, redacted_paths: [] },
    });
    const requested = auditEvent({
      event_id: "evt_requested",
      sequence: 1042,
      result: "requested",
    });
    const seen: Array<Record<string, string>> = [];

    server.use(
      http.get("*/api/v1/admin/audit-events", ({ request }) => {
        const url = new URL(request.url);
        seen.push(Object.fromEntries(url.searchParams));
        if (url.searchParams.get("cursor") === "cursor-2") {
          return HttpResponse.json({
            items: [terminal],
            page: { next_cursor: null, has_more: false },
          });
        }
        return HttpResponse.json({
          items: [terminal, requested],
          page: { next_cursor: "cursor-2", has_more: true },
        });
      }),
    );

    const timeline = await fetchAuditTimeline("op_01JABCXYZ");

    expect(seen).toHaveLength(2);
    expect(seen[0]).toMatchObject({
      operation_id: "op_01JABCXYZ",
      order: "asc",
    });
    expect(seen[1]).toMatchObject({ cursor: "cursor-2" });
    expect(timeline.map((event) => event.event_id)).toEqual([
      "evt_requested",
      "evt_terminal",
    ]);
  });

  it("stops after the bounded number of pages and fails rather than looping", async () => {
    let calls = 0;
    server.use(
      http.get("*/api/v1/admin/audit-events", () => {
        calls += 1;
        return HttpResponse.json({
          items: [auditEvent({ event_id: `evt_${calls}` })],
          page: { next_cursor: "never-advances", has_more: true },
        });
      }),
    );

    await expect(fetchAuditTimeline("op_loop")).rejects.toMatchObject({
      code: "timeline_page_limit",
    });
    expect(calls).toBe(MAX_TIMELINE_PAGES);
  });
});
