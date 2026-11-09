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

interface SessionOptions {
  idleInMs?: number;
  absoluteInMs?: number;
  role?: string;
  csrfToken?: string;
}

function sessionPayload({
  idleInMs = 30 * 60_000,
  absoluteInMs = 8 * 3_600_000,
  role = "Operator",
  csrfToken = "memory-csrf-token",
}: SessionOptions = {}) {
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
      idle_expires_at: new Date(now + idleInMs).toISOString(),
      absolute_expires_at: new Date(now + absoluteInMs).toISOString(),
    },
    csrf: { header_name: "X-CSRF-Token", token: csrfToken },
  };
}

type Resolver = Parameters<typeof http.post>[1];

function adminHandlers(
  session: ReturnType<typeof sessionPayload>,
  overrides: {
    refresh?: Resolver;
    logout?: Resolver;
  } = {},
) {
  return [
    http.get("*/api/v1/admin/capabilities", () =>
      HttpResponse.json(capabilityDocument),
    ),
    http.get("*/api/v1/admin/auth/session", () => HttpResponse.json(session)),
    http.post(
      "*/api/v1/admin/auth/refresh",
      overrides.refresh ?? (() => HttpResponse.json(sessionPayload())),
    ),
    http.post(
      "*/api/v1/admin/auth/logout",
      overrides.logout ?? (() => new HttpResponse(null, { status: 204 })),
    ),
  ];
}

function errorEnvelope(
  code: string,
  status: number,
  details: Record<string, unknown> = {},
) {
  return HttpResponse.json(
    {
      error: {
        code,
        message: code,
        details,
        request_id: `req-${code}`,
      },
    },
    { status, headers: { "X-Request-ID": `req-${code}` } },
  );
}

describe("Admin session lifecycle", () => {
  it("warns five minutes before expiry and extends only on explicit 继续", async () => {
    const user = userEvent.setup();
    const initial = sessionPayload({ idleInMs: 4 * 60_000 });
    let refreshCalls = 0;
    let receivedCsrf: string | null = null;

    server.use(
      ...adminHandlers(initial, {
        refresh: ({ request }) => {
          refreshCalls += 1;
          receivedCsrf = request.headers.get("X-CSRF-Token");
          return HttpResponse.json(sessionPayload({ idleInMs: 30 * 60_000 }));
        },
      }),
    );

    renderApp({ initialEntries: ["/monitoring"] });

    const notice = await screen.findByTestId("session-expiry-notice");
    expect(within(notice).getByText(/会话将在 4 分钟后过期/)).toBeInTheDocument();
    expect(within(notice).getByText(/30 分钟空闲限制/)).toBeInTheDocument();

    // Merely sitting on the page never manufactures activity or refreshes.
    expect(refreshCalls).toBe(0);

    await user.click(within(notice).getByRole("button", { name: "继续" }));

    await waitFor(() => {
      expect(screen.queryByTestId("session-expiry-notice")).not.toBeInTheDocument();
    });
    expect(refreshCalls).toBe(1);
    expect(receivedCsrf).toBe("memory-csrf-token");
    expect(
      within(await screen.findByTestId("runtime-mode-banner")).getByText(
        "Operator",
      ),
    ).toBeInTheDocument();
  });

  it("warns when the absolute eight-hour limit is the earlier bound", async () => {
    server.use(
      ...adminHandlers(
        sessionPayload({ idleInMs: 30 * 60_000, absoluteInMs: 3 * 60_000 }),
      ),
    );

    renderApp({ initialEntries: ["/monitoring"] });

    const notice = await screen.findByTestId("session-expiry-notice");
    expect(within(notice).getByText(/会话将在 3 分钟后过期/)).toBeInTheDocument();
    expect(within(notice).getByText(/8 小时最长会话限制/)).toBeInTheDocument();
    // The absolute limit can never be extended, so "继续" must not exist.
    expect(
      within(notice).queryByRole("button", { name: "继续" }),
    ).not.toBeInTheDocument();
    expect(within(notice).getByRole("button", { name: "退出" })).toBeInTheDocument();
  });

  it("clears identity, stops writes and returns to the login entry on a 401", async () => {
    const user = userEvent.setup();
    server.use(
      ...adminHandlers(sessionPayload({ idleInMs: 4 * 60_000 }), {
        refresh: () =>
          errorEnvelope("unauthenticated", 401, {
            reason: "idle_timeout",
          }) as unknown as Response,
      }),
    );

    renderApp({ initialEntries: ["/monitoring"] });

    const notice = await screen.findByTestId("session-expiry-notice");
    await user.click(within(notice).getByRole("button", { name: "继续" }));

    const banner = await screen.findByTestId("runtime-mode-banner");
    const loginLink = await within(banner).findByRole("link", { name: "登录" });
    expect(loginLink).toHaveAttribute(
      "href",
      "/api/v1/admin/auth/login?return_to=%2Fmonitoring",
    );
    expect(within(banner).queryByText("Operator")).not.toBeInTheDocument();
    expect(
      await screen.findByTestId("global-http-notice"),
    ).toHaveTextContent(/会话已失效或未登录，写操作已停止/);
    expect(screen.queryByTestId("session-expiry-notice")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "继续" })).not.toBeInTheDocument();
  });

  it("does not log out on a 403 and names the missing role", async () => {
    const user = userEvent.setup();
    server.use(
      ...adminHandlers(sessionPayload({ idleInMs: 4 * 60_000 }), {
        refresh: () =>
          errorEnvelope("forbidden", 403, {
            reason: "role",
            required_role: "SuperAdmin",
          }) as unknown as Response,
      }),
    );

    renderApp({ initialEntries: ["/monitoring"] });

    const notice = await screen.findByTestId("session-expiry-notice");
    await user.click(within(notice).getByRole("button", { name: "继续" }));

    const banner = screen.getByTestId("runtime-mode-banner");
    expect(within(banner).getByText("Operator")).toBeInTheDocument();
    expect(within(banner).queryByRole("link", { name: "登录" })).not.toBeInTheDocument();
    expect(await screen.findByTestId("global-http-notice")).toHaveTextContent(
      /缺少权限：需要 SuperAdmin 角色/,
    );
    expect(screen.getByTestId("session-expiry-notice")).toBeInTheDocument();
  });

  it("logs out through the CSRF-protected POST and shows the login entry", async () => {
    const user = userEvent.setup();
    let receivedCsrf: string | null = null;
    server.use(
      ...adminHandlers(sessionPayload(), {
        logout: ({ request }) => {
          receivedCsrf = request.headers.get("X-CSRF-Token");
          return new HttpResponse(null, { status: 204 });
        },
      }),
    );

    renderApp({ initialEntries: ["/monitoring"] });

    const banner = await screen.findByTestId("runtime-mode-banner");
    await within(banner).findByText("Operator");
    await user.click(within(banner).getByRole("button", { name: "注销" }));

    expect(await within(banner).findByRole("link", { name: "登录" })).toBeInTheDocument();
    expect(receivedCsrf).toBe("memory-csrf-token");
    expect(within(banner).queryByText("Operator")).not.toBeInTheDocument();
    expect(within(banner).getByText("未登录")).toBeInTheDocument();
  });

  it("invalidates an already expired session without pretending it is usable", async () => {
    server.use(
      ...adminHandlers(sessionPayload({ idleInMs: -1_000 })),
    );

    renderApp({ initialEntries: ["/monitoring"] });

    const banner = await screen.findByTestId("runtime-mode-banner");
    expect(await within(banner).findByRole("link", { name: "登录" })).toBeInTheDocument();
    expect(within(banner).getByText(/会话已失效/)).toBeInTheDocument();
    expect(within(banner).queryByText("Operator")).not.toBeInTheDocument();
  });

  it("never persists the CSRF token to web storage or a URL", async () => {
    const user = userEvent.setup();
    server.use(...adminHandlers(sessionPayload({ idleInMs: 4 * 60_000 })));

    renderApp({ initialEntries: ["/monitoring"] });

    const banner = await screen.findByTestId("runtime-mode-banner");
    await within(banner).findByText("Operator");

    const notice = screen.getByTestId("session-expiry-notice");
    await user.click(within(notice).getByRole("button", { name: "继续" }));

    await waitFor(() => {
      expect(screen.queryByTestId("session-expiry-notice")).not.toBeInTheDocument();
    });

    expect(window.localStorage.length).toBe(0);
    expect(window.sessionStorage.length).toBe(0);
    expect(window.location.href).not.toContain("memory-csrf-token");
    expect(document.cookie).not.toContain("memory-csrf-token");
  });
});
