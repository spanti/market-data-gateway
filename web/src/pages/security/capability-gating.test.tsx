import { screen, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import { renderApp } from "@/test/render-app";
import { server } from "@/test/server";

function capabilityDocument(overrides: Record<string, unknown> = {}) {
  return {
    schema_version: 1,
    server_time: "2026-11-09T12:00:00.000000000Z",
    version: { admin_api: "1.0.0", contract: 1, supported_contracts: [1] },
    auth: {
      required: true,
      mode: "oidc",
      state: "available",
      roles: ["SuperAdmin", "Operator", "Auditor"],
    },
    adapters: [],
    capabilities: {
      oidc_session: { state: "enabled" },
      operator_audit: {
        state: "enabled",
        export: false,
        edit: false,
        delete: false,
      },
    },
    ...overrides,
  };
}

function authenticatedSession() {
  const now = Date.now();
  return {
    authenticated: true,
    server_time: new Date(now).toISOString(),
    user: {
      subject: "oidc-subject-auditor",
      display_name: "Ada Auditor",
      email: "auditor@example.com",
    },
    access_role: "Auditor",
    session: {
      issued_at: new Date(now - 60_000).toISOString(),
      last_activity_at: new Date(now).toISOString(),
      idle_expires_at: new Date(now + 30 * 60_000).toISOString(),
      absolute_expires_at: new Date(now + 8 * 3_600_000).toISOString(),
    },
    csrf: { header_name: "X-CSRF-Token", token: "csrf-auditor" },
  };
}

const ANONYMOUS = {
  authenticated: false,
  server_time: new Date().toISOString(),
  user: null,
  access_role: null,
  session: null,
  csrf: null,
};

describe("capability gating for login and audit", () => {
  it("disables the login entry when OIDC is explicitly unavailable", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(
          capabilityDocument({
            auth: {
              required: true,
              mode: "oidc",
              state: "unavailable",
              roles: ["SuperAdmin", "Operator", "Auditor"],
            },
          }),
        ),
      ),
      http.get("*/api/v1/admin/auth/session", () =>
        HttpResponse.json(ANONYMOUS),
      ),
    );

    renderApp({ initialEntries: ["/security"] });

    const panel = await screen.findByRole("region", { name: "当前身份" });
    const login = await within(panel).findByRole("button", {
      name: "登录 OIDC",
    });
    expect(login).toBeDisabled();
    expect(within(panel).getByText("OIDC 暂不可用")).toBeInTheDocument();
    expect(
      screen.queryByRole("link", { name: "登录 OIDC" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "登录" })).not.toBeInTheDocument();
  });

  it("disables login when the oidc_session capability is unsupported", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(
          capabilityDocument({
            capabilities: {
              oidc_session: { state: "unsupported" },
              operator_audit: { state: "enabled" },
            },
          }),
        ),
      ),
      http.get("*/api/v1/admin/auth/session", () =>
        HttpResponse.json(ANONYMOUS),
      ),
    );

    renderApp({ initialEntries: ["/security"] });

    const panel = await screen.findByRole("region", { name: "当前身份" });
    expect(
      await within(panel).findByText("后端未实现"),
    ).toBeInTheDocument();
    expect(
      within(panel).getByRole("button", { name: "登录 OIDC" }),
    ).toBeDisabled();
  });

  it("disables the audit query when the audit capability is adapter_unavailable", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(
          capabilityDocument({
            capabilities: {
              oidc_session: { state: "enabled" },
              operator_audit: { state: "adapter_unavailable" },
            },
          }),
        ),
      ),
      http.get("*/api/v1/admin/auth/session", () =>
        HttpResponse.json(authenticatedSession()),
      ),
    );

    renderApp({ initialEntries: ["/security"] });

    expect(await screen.findByText("适配器不可用")).toBeInTheDocument();
    expect(screen.queryByTestId("audit-event-table")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("region", { name: "Operator Audit Event 查询" }),
    ).not.toBeInTheDocument();
  });

  it("fails closed when the operator_audit capability key is absent", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(
          capabilityDocument({
            capabilities: { oidc_session: { state: "enabled" } },
          }),
        ),
      ),
      http.get("*/api/v1/admin/auth/session", () =>
        HttpResponse.json(authenticatedSession()),
      ),
    );

    renderApp({ initialEntries: ["/security"] });

    expect(await screen.findByText("后端未实现")).toBeInTheDocument();
    expect(screen.queryByTestId("audit-event-table")).not.toBeInTheDocument();
  });

  it("disables Admin features on an incompatible contract version", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(
          capabilityDocument({
            version: {
              admin_api: "2.0.0",
              contract: 2,
              supported_contracts: [2],
            },
          }),
        ),
      ),
      http.get("*/api/v1/admin/auth/session", () =>
        HttpResponse.json(authenticatedSession()),
      ),
    );

    renderApp({ initialEntries: ["/security"] });

    expect(
      await screen.findAllByText("Admin API 版本不兼容"),
    ).not.toHaveLength(0);
    expect(screen.queryByTestId("audit-event-table")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("region", { name: "Operator Audit Event 查询" }),
    ).not.toBeInTheDocument();
  });
});
