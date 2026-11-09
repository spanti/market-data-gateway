import { fireEvent, screen, waitFor, within } from "@testing-library/react";
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
  limits: { page_default: 50, page_max: 200 },
};

function sessionPayload(role = "Operator") {
  const now = Date.now();
  return {
    authenticated: true,
    server_time: new Date(now).toISOString(),
    user: {
      subject: "oidc-subject-123",
      display_name: "Ada Operator",
      email: "ada@example.com",
    },
    access_role: role,
    session: {
      issued_at: new Date(now - 60_000).toISOString(),
      last_activity_at: new Date(now).toISOString(),
      idle_expires_at: new Date(now + 30 * 60_000).toISOString(),
      absolute_expires_at: new Date(now + 8 * 3_600_000).toISOString(),
    },
    csrf: { header_name: "X-CSRF-Token", token: "csrf-token" },
  };
}

function eventPayload(overrides: Record<string, unknown> = {}) {
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

async function openSecurity() {
  renderApp({ initialEntries: ["/security"] });
  const section = await screen.findByRole("region", { name: "Operator Audit Event 查询" });
  return section;
}

describe("Operator Audit Event query", () => {
  it("serializes all seven filters and bounded pagination to the Admin API", async () => {
    const user = userEvent.setup();
    const seen: URL[] = [];
    server.use(
      ...baseHandlers(),
      http.get("*/api/v1/admin/audit-events", ({ request }) => {
        seen.push(new URL(request.url));
        return HttpResponse.json({
          items: [eventPayload()],
          page: { next_cursor: null, has_more: false },
        });
      }),
    );

    const section = await openSecurity();
    await waitFor(() => expect(seen.length).toBeGreaterThanOrEqual(1));

    fireEvent.change(within(section).getByLabelText("开始时间（UTC，含）"), {
      target: { value: "2026-11-09T00:00" },
    });
    fireEvent.change(within(section).getByLabelText("结束时间（UTC，不含）"), {
      target: { value: "2026-11-09T12:00" },
    });
    await user.type(
      within(section).getByLabelText("操作者 Subject"),
      "oidc-subject-123",
    );
    await user.selectOptions(
      within(section).getByLabelText("Access Role（筛选）"),
      "Operator",
    );
    await user.type(
      within(section).getByLabelText("动作"),
      "feed_subscription.update",
    );
    await user.type(
      within(section).getByLabelText("对象类型"),
      "feed_subscription",
    );
    await user.type(within(section).getByLabelText("对象 ID"), "fsub_01JABCXYZ");
    await user.selectOptions(within(section).getByLabelText("结果"), "succeeded");
    await user.type(
      within(section).getByLabelText("Request ID"),
      "req_01JABCXYZ",
    );
    await user.click(within(section).getByRole("button", { name: "查询" }));

    await waitFor(() => expect(seen.length).toBeGreaterThanOrEqual(2));
    expect(Object.fromEntries(seen.at(-1)?.searchParams ?? new URLSearchParams()))
      .toMatchObject({
        from: "2026-11-09T00:00:00.000Z",
        to: "2026-11-09T12:00:00.000Z",
        actor_subject: "oidc-subject-123",
        access_role: "Operator",
        action: "feed_subscription.update",
        target_type: "feed_subscription",
        target_id: "fsub_01JABCXYZ",
        result: "succeeded",
        request_id: "req_01JABCXYZ",
        limit: "50",
        order: "desc",
      });
  });

  it("renders the event table and walks forward and back with opaque cursors", async () => {
    const user = userEvent.setup();
    const seen: URL[] = [];
    server.use(
      ...baseHandlers(),
      http.get("*/api/v1/admin/audit-events", ({ request }) => {
        const url = new URL(request.url);
        seen.push(url);
        if (url.searchParams.get("cursor") === "cursor-2") {
          return HttpResponse.json({
            items: [
              eventPayload({
                event_id: "evt_page_two",
                sequence: 1041,
                action: "service_instance.reload",
              }),
            ],
            page: { next_cursor: null, has_more: false },
          });
        }
        return HttpResponse.json({
          items: [eventPayload()],
          page: { next_cursor: "cursor-2", has_more: true },
        });
      }),
    );

    const section = await openSecurity();

    const table = await within(section).findByTestId("audit-event-table");
    expect(
      await within(table).findByText("feed_subscription.update"),
    ).toBeInTheDocument();
    expect(within(table).getByText("req_01JABCXYZ")).toBeInTheDocument();

    const next = within(section).getByRole("button", { name: /下一页/ });
    await user.click(next);

    expect(
      await within(section).findByText("service_instance.reload"),
    ).toBeInTheDocument();
    expect(seen.at(-1)?.searchParams.get("cursor")).toBe("cursor-2");
    expect(within(section).getByRole("button", { name: /下一页/ })).toBeDisabled();

    await user.click(within(section).getByRole("button", { name: /上一页/ }));

    await waitFor(() =>
      expect(
        within(section).getByText("feed_subscription.update"),
      ).toBeInTheDocument(),
    );
    expect(
      within(section).getByRole("button", { name: /下一页/ }),
    ).toBeEnabled();
    // Returning to the first page reuses the already-fetched cursor-less page;
    // the forward cursor is never replayed on the way back.
    expect(
      seen.filter((url) => url.searchParams.get("cursor") === "cursor-2"),
    ).toHaveLength(1);
  });

  it("does not offer edit, delete or export controls for audit events", async () => {
    server.use(
      ...baseHandlers(),
      http.get("*/api/v1/admin/audit-events", () =>
        HttpResponse.json({
          items: [eventPayload()],
          page: { next_cursor: null, has_more: false },
        }),
      ),
    );

    const section = await openSecurity();
    await within(section).findByTestId("audit-event-table");

    expect(
      within(section).queryByRole("button", { name: /编辑|删除|导出/ }),
    ).not.toBeInTheDocument();
    expect(within(section).queryByText(/导出审计/)).not.toBeInTheDocument();
  });

  it("fails closed on a malformed audit response instead of rendering partial rows", async () => {
    server.use(
      ...baseHandlers(),
      http.get("*/api/v1/admin/audit-events", () =>
        HttpResponse.json({ items: "not-an-array" }),
      ),
    );

    const section = await openSecurity();

    expect(
      await within(section).findByText(/审计查询响应不符合契约/),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("audit-event-table")).not.toBeInTheDocument();
  });

  it("keeps the session on a 403 and names the missing role", async () => {
    server.use(
      ...baseHandlers(),
      http.get("*/api/v1/admin/audit-events", () =>
        HttpResponse.json(
          {
            error: {
              code: "forbidden",
              message: "forbidden",
              details: { reason: "role", required_role: "SuperAdmin" },
              request_id: "req-audit-403",
            },
          },
          { status: 403, headers: { "X-Request-ID": "req-audit-403" } },
        ),
      ),
    );

    const section = await openSecurity();

    expect(
      await within(section).findByText(/缺少权限：需要 SuperAdmin 角色/),
    ).toBeInTheDocument();
    expect(within(section).getByText(/服务端授权才是安全边界/)).toBeInTheDocument();
    expect(within(section).getByText(/req-audit-403/)).toBeInTheDocument();

    const banner = screen.getByTestId("runtime-mode-banner");
    expect(within(banner).getByText("Operator")).toBeInTheDocument();
    expect(within(banner).queryByRole("link", { name: "登录" })).not.toBeInTheDocument();
  });

  it("stops writes and returns to the login entry on a 401", async () => {
    server.use(
      ...baseHandlers(),
      http.get("*/api/v1/admin/audit-events", () =>
        HttpResponse.json(
          {
            error: {
              code: "unauthenticated",
              message: "unauthenticated",
              details: { reason: "idle_timeout" },
              request_id: "req-audit-401",
            },
          },
          { status: 401, headers: { "X-Request-ID": "req-audit-401" } },
        ),
      ),
    );

    await openSecurity();

    const banner = screen.getByTestId("runtime-mode-banner");
    expect(await within(banner).findByRole("link", { name: "登录" })).toBeInTheDocument();
    expect(
      await screen.findByTestId("global-http-notice"),
    ).toHaveTextContent(/写操作已停止/);
    expect(within(banner).queryByText("Operator")).not.toBeInTheDocument();
  });

  it("exposes filter controls but still no role switcher in Admin mode", async () => {
    const user = userEvent.setup();
    server.use(
      ...baseHandlers(),
      http.get("*/api/v1/admin/audit-events", () =>
        HttpResponse.json({
          items: [],
          page: { next_cursor: null, has_more: false },
        }),
      ),
    );

    const section = await openSecurity();
    await within(section).findByText("没有匹配的事件");

    const comboboxes = screen.getAllByRole("combobox");
    expect(comboboxes).toHaveLength(2); // Access Role filter and result filter
    expect(screen.queryByText(/切换角色/)).not.toBeInTheDocument();
    expect(
      screen.queryByRole("combobox", { name: /切换.*角色|角色切换/ }),
    ).not.toBeInTheDocument();

    // The Access Role filter is explicitly a query filter, not identity.
    expect(
      within(section).getByLabelText("Access Role（筛选）"),
    ).toBeInTheDocument();
    void user;
  });
});
