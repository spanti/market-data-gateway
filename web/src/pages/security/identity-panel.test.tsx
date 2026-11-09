import { screen, waitFor, within } from "@testing-library/react";
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

function sessionPayload(overrides: Record<string, unknown> = {}) {
  const now = Date.now();
  return {
    authenticated: true,
    server_time: new Date(now).toISOString(),
    user: {
      subject: "oidc-subject-456",
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
    ...overrides,
  };
}

function adminHandlers(session: Record<string, unknown>) {
  return [
    http.get("*/api/v1/admin/capabilities", () =>
      HttpResponse.json(capabilityDocument),
    ),
    http.get("*/api/v1/admin/auth/session", () =>
      HttpResponse.json(session),
    ),
  ];
}

export function formatUtcForTest(value: string): string {
  return `${new Date(value).toISOString().replace("T", " ").slice(0, 19)} UTC`;
}

describe("Security identity panel in Current Contract", () => {
  it("shows anonymous read-only with no role, no session and no fake login", async () => {
    renderApp({ initialEntries: ["/security"] });

    const panel = await screen.findByRole("region", { name: "当前身份" });
    expect((await within(panel).findAllByText("匿名只读")).length).toBeGreaterThan(0);
    expect(
      within(panel).getByText("Current Contract（无 OIDC 会话）"),
    ).toBeInTheDocument();
    expect(
      within(panel).getByText("不分配（Current Contract）"),
    ).toBeInTheDocument();
    expect(within(panel).getAllByText("—").length).toBeGreaterThanOrEqual(3);

    const login = within(panel).getByRole("button", { name: "登录 OIDC" });
    expect(login).toBeDisabled();
    expect(within(panel).getByText(/需要 Admin API/)).toBeInTheDocument();
  });

  it("keeps the audit query unavailable with a visible reason and no mock rows", async () => {
    renderApp({ initialEntries: ["/security"] });

    const banner = await screen.findByTestId("runtime-mode-banner");
    await waitFor(() =>
      expect(banner).toHaveAttribute("data-mode", "current-contract"),
    );
    expect(
      await screen.findByText("没有权限", { selector: "div" }),
    ).toBeInTheDocument();
    expect(screen.getByText("需要 Admin API")).toBeInTheDocument();
    expect(screen.queryByTestId("audit-event-table")).not.toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("has no role selector or privilege-elevation control anywhere", async () => {
    renderApp({ initialEntries: ["/security"] });

    await screen.findByRole("region", { name: "当前身份" });
    expect(screen.queryByTestId("role-switcher")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("combobox", { name: /切换.*角色|角色切换/ }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText(/切换角色/)).not.toBeInTheDocument();
    expect(screen.queryByText(/提升权限/)).not.toBeInTheDocument();
    expect(screen.queryAllByRole("combobox")).toHaveLength(0);
  });
});

describe("Security identity panel in Admin API mode", () => {
  it("offers the backend OIDC login entry when anonymous", async () => {
    server.use(
      ...adminHandlers({
        authenticated: false,
        server_time: new Date().toISOString(),
        user: null,
        access_role: null,
        session: null,
        csrf: null,
      }),
    );

    renderApp({ initialEntries: ["/security?tab=audit"] });

    const panel = await screen.findByRole("region", { name: "当前身份" });
    expect((await within(panel).findAllByText("未登录")).length).toBeGreaterThan(0);
    expect(within(panel).getByText("OIDC（Admin API）")).toBeInTheDocument();
    expect(within(panel).getByText("无")).toBeInTheDocument();

    const login = within(panel).getByRole("link", { name: "登录 OIDC" });
    expect(login).toHaveAttribute(
      "href",
      "/api/v1/admin/auth/login?return_to=%2Fsecurity%3Ftab%3Daudit",
    );
    expect(await screen.findByText("需要登录")).toBeInTheDocument();
  });

  it("renders the server-assigned role and all session times for an operator", async () => {
    const session = sessionPayload();
    server.use(...adminHandlers(session));

    renderApp({ initialEntries: ["/security"] });

    const panel = await screen.findByRole("region", { name: "当前身份" });
    expect(await within(panel).findByText("Ada Auditor")).toBeInTheDocument();
    expect(within(panel).getByText("oidc-subject-456")).toBeInTheDocument();
    expect(within(panel).getAllByText("Auditor").length).toBeGreaterThan(0);
    expect(
      within(panel).getByText(
        "OIDC + 服务端会话（30 分钟空闲 / 8 小时绝对）",
      ),
    ).toBeInTheDocument();

    const sessionTimes = session.session as Record<string, string>;
    for (const key of [
      "issued_at",
      "idle_expires_at",
      "absolute_expires_at",
    ] as const) {
      expect(
        within(panel).getByText(formatUtcForTest(sessionTimes[key] ?? "")),
      ).toBeInTheDocument();
    }

    // The CSRF token is never rendered as identity material.
    expect(within(panel).queryByText(/csrf-auditor/)).not.toBeInTheDocument();
    expect(screen.queryByTestId("role-switcher")).not.toBeInTheDocument();
  });

  it("does not infer an identity when capability discovery is unknown", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(
          {
            error: {
              code: "internal_error",
              message: "boom",
              details: {},
              request_id: "cap-boom",
            },
          },
          { status: 500 },
        ),
      ),
    );

    renderApp({ initialEntries: ["/security"] });

    const panel = await screen.findByRole("region", { name: "当前身份" });
    expect(
      await within(panel).findByText("未知（能力发现失败）"),
    ).toBeInTheDocument();
    expect(within(panel).getAllByText("能力未知").length).toBeGreaterThan(0);
    expect(
      screen.getAllByText("能力未知，暂不可用", { selector: "span" }).length,
    ).toBeGreaterThan(0);
  });
});
