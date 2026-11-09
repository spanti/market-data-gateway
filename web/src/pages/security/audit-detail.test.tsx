import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import { renderApp } from "@/test/render-app";
import { server } from "@/test/server";

const capabilityDocument = {
  schema_version: 1,
  server_time: "2026-11-09T12:00:00.000000000Z",
  version: { admin_api: "1.0.0", contract: 1, supported_contracts: [1] },
  auth: {
    required: true,
    mode: "oidc",
    roles: ["SuperAdmin", "Operator", "Auditor"],
  },
  adapters: [],
  capabilities: {
    oidc_session: { state: "enabled" },
    operator_audit: { state: "enabled", export: false, edit: false, delete: false },
  },
};

function sessionPayload() {
  const now = Date.now();
  return {
    authenticated: true,
    server_time: new Date(now).toISOString(),
    user: {
      subject: "oidc-subject-123",
      display_name: "Ada Operator",
      email: "ada@example.com",
    },
    access_role: "Operator",
    session: {
      issued_at: new Date(now - 60_000).toISOString(),
      last_activity_at: new Date(now).toISOString(),
      idle_expires_at: new Date(now + 30 * 60_000).toISOString(),
      absolute_expires_at: new Date(now + 8 * 3_600_000).toISOString(),
    },
    csrf: { header_name: "X-CSRF-Token", token: "csrf-token" },
  };
}

function baseEvent(overrides: Record<string, unknown> = {}) {
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
      before: { enabled: false, version: 2, client_secret: "[REDACTED]" },
      after: { enabled: true, version: 3, client_secret: "[REDACTED]" },
      redacted_paths: ["/client_secret"],
    },
    hash_chain: {
      algorithm: "sha256",
      previous_event_hash: "prev-hash",
      event_hash: "event-hash",
    },
    ...overrides,
  };
}

function baseHandlers() {
  return [
    http.get("*/api/v1/admin/capabilities", () =>
      HttpResponse.json(capabilityDocument),
    ),
    http.get("*/api/v1/admin/auth/session", () =>
      HttpResponse.json(sessionPayload()),
    ),
  ];
}

function listHandlers(event = baseEvent()) {
  return [
    ...baseHandlers(),
    http.get("*/api/v1/admin/audit-events", () =>
      HttpResponse.json({
        items: [event],
        page: { next_cursor: null, has_more: false },
      }),
    ),
  ];
}

async function openDetail(event = baseEvent()) {
  const user = userEvent.setup();
  renderApp({ initialEntries: ["/security"] });
  const rowButton = await screen.findByRole("button", {
    name: `查看详情 ${String(event.event_id)}`,
  });
  await user.click(rowButton);
  return user;
}

describe("Operator Audit Event detail", () => {
  it("renders the event metadata, redacted diff and hash-chain caveat", async () => {
    const event = baseEvent();
    server.use(
      ...listHandlers(event),
      http.get("*/api/v1/admin/audit-events/:eventId", () =>
        HttpResponse.json(event),
      ),
    );

    await openDetail(event);

    const detail = await screen.findByRole("region", { name: "事件详情" });
    expect(
      (await within(detail).findAllByText("feed_subscription.update")).length,
    ).toBeGreaterThan(0);
    expect(within(detail).getByText("req_01JABCXYZ")).toBeInTheDocument();
    expect(
      within(detail).getAllByText("add BTC 1m subscription").length,
    ).toBeGreaterThan(0);
    expect(within(detail).getAllByText(/\[REDACTED\]/).length).toBeGreaterThanOrEqual(2);
    expect(within(detail).getAllByText("/client_secret").length).toBeGreaterThan(0);
    expect(within(detail).getByText("sha256")).toBeInTheDocument();
    expect(within(detail).getByText(/不宣称不可篡改存储/)).toBeInTheDocument();

    expect(
      within(detail).queryByRole("button", { name: /编辑|删除|导出/ }),
    ).not.toBeInTheDocument();
    expect(within(detail).queryByRole("link", { name: /编辑|删除|导出/ })).not.toBeInTheDocument();
  });

  it("groups requested and terminal events by Operation ID as appended timeline entries", async () => {
    const user = userEvent.setup();
    const requested = baseEvent({
      event_id: "evt_requested",
      sequence: 1042,
      result: "requested",
      occurred_at: "2026-11-09T12:00:00.000000000Z",
    });
    const terminal = baseEvent({
      event_id: "evt_terminal",
      sequence: 1043,
      result: "succeeded",
      occurred_at: "2026-11-09T12:00:05.000000000Z",
    });

    let phase: "requested" | "both" = "requested";
    const timelineRequests: URL[] = [];

    server.use(
      ...baseHandlers(),
      http.get("*/api/v1/admin/audit-events/:eventId", () =>
        HttpResponse.json(requested),
      ),
      http.get("*/api/v1/admin/audit-events", ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.has("operation_id")) {
          timelineRequests.push(url);
          return HttpResponse.json({
            items: phase === "requested" ? [requested] : [requested, terminal],
            page: { next_cursor: null, has_more: false },
          });
        }
        return HttpResponse.json({
          items: [requested],
          page: { next_cursor: null, has_more: false },
        });
      }),
    );

    renderApp({ initialEntries: ["/security"] });
    await user.click(
      await screen.findByRole("button", { name: "查看详情 evt_requested" }),
    );

    const timeline = await screen.findByTestId("audit-operation-timeline");
    await waitFor(() => {
      expect(
        within(timeline).getAllByTestId("audit-timeline-item"),
      ).toHaveLength(1);
    });
    expect(timelineRequests[0]?.searchParams.get("operation_id")).toBe(
      "op_01JABCXYZ",
    );
    expect(timelineRequests[0]?.searchParams.get("order")).toBe("asc");

    phase = "both";
    await user.click(
      within(timeline).getByRole("button", { name: "刷新时间线" }),
    );

    await waitFor(() => {
      expect(
        within(timeline).getAllByTestId("audit-timeline-item"),
      ).toHaveLength(2);
    });
    const items = within(timeline).getAllByTestId("audit-timeline-item");
    expect(items[0]).toHaveTextContent("requested");
    expect(items[1]).toHaveTextContent("succeeded");
    // The earlier requested event remains its own appended entry; the
    // terminal event does not overwrite it in place.
    expect(items[0]).toHaveTextContent("1042");
    expect(items[1]).toHaveTextContent("1043");
    expect(items[0]).toHaveTextContent('"version": 2');
  });

  it("fails closed on a malformed event detail payload", async () => {
    const event = baseEvent();
    server.use(
      ...listHandlers(event),
      http.get("*/api/v1/admin/audit-events/:eventId", () =>
        HttpResponse.json({ event_id: event.event_id }),
      ),
    );

    await openDetail(event);

    expect(
      await screen.findByText(/事件详情响应不符合契约/),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("audit-diff-before")).not.toBeInTheDocument();
  });
});
