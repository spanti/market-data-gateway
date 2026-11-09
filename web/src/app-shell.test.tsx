import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import { renderApp } from "@/test/render-app";
import { server } from "@/test/server";

const capabilityDocument = {
  schema_version: 1,
  server_time: "2026-11-09T12:00:00.000000000Z",
  version: { admin_api: "1.0.0", contract: 1, supported_contracts: [1] },
  auth: { required: true, mode: "oidc", roles: ["SuperAdmin", "Operator", "Auditor"] },
  adapters: [],
  capabilities: { feed_subscription: { state: "enabled" } },
};

describe("Operator Console shell", () => {
  it("redirects the root route to /monitoring", async () => {
    renderApp({ initialEntries: ["/"] });

    expect(
      await screen.findByRole("heading", { name: "监控分析工作台" }),
    ).toBeInTheDocument();
    const banner = await screen.findByTestId("runtime-mode-banner");
    expect(banner).toHaveAttribute("data-mode", "current-contract");
  });

  it("renders all five module destinations", async () => {
    const user = userEvent.setup();
    renderApp();

    await screen.findByTestId("runtime-mode-banner");
    const nav = screen.getByRole("navigation", { name: "一级模块导航" });

    const destinations = [
      { label: "监控", heading: "监控分析工作台", path: "/monitoring" },
      { label: "Bar 图与导出", heading: "Bar 图与导出", path: "/bars" },
      { label: "Gateway Console", heading: "Gateway Console", path: "/console" },
      {
        label: "Quality & Backfill",
        heading: "Quality & Backfill",
        path: "/quality",
      },
      {
        label: "Security & Audit",
        heading: "Security & Audit",
        path: "/security",
      },
    ];

    for (const destination of destinations) {
      const link = within(nav).getByRole("link", {
        name: destination.label,
      });
      expect(link).toHaveAttribute("href", destination.path);
      await user.click(link);
      expect(
        await screen.findByRole("heading", {
          name: destination.heading,
          level: 1,
        }),
      ).toBeInTheDocument();
    }
  });

  it("shows Current Contract as anonymous read-only and has no role switcher", async () => {
    renderApp();

    const banner = await screen.findByTestId("runtime-mode-banner");
    expect(banner).toHaveAttribute("data-mode", "current-contract");
    expect(within(banner).getByText("Current Contract")).toBeInTheDocument();
    expect(within(banner).getByText("匿名只读")).toBeInTheDocument();

    expect(screen.queryByTestId("role-switcher")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("combobox", { name: /角色/ }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText(/切换角色/)).not.toBeInTheDocument();
    expect(screen.queryByText(/登录/)).not.toBeInTheDocument();
  });

  it("shows Admin API with the server-assigned Access Role", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(capabilityDocument),
      ),
      http.get("*/api/v1/admin/auth/session", () =>
        HttpResponse.json({
          authenticated: true,
          server_time: "2026-11-09T12:00:00.000000000Z",
          user: {
            subject: "oidc-subject-123",
            display_name: "Ada Operator",
            email: "ada@example.com",
          },
          access_role: "Operator",
          session: null,
          csrf: null,
        }),
      ),
    );

    renderApp();

    const banner = await screen.findByTestId("runtime-mode-banner");
    expect(banner).toHaveAttribute("data-mode", "admin-api");
    expect(within(banner).getByText("Admin API")).toBeInTheDocument();
    expect(await within(banner).findByText("Operator")).toBeInTheDocument();
    expect(within(banner).getByText("Ada Operator")).toBeInTheDocument();
    expect(within(banner).queryByText("登录")).not.toBeInTheDocument();
  });

  it("shows capabilities-unknown without pretending to be read-only", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(
          {
            error: {
              code: "internal_error",
              message: "internal server error",
              details: {},
              request_id: "capability-boom",
            },
          },
          { status: 500 },
        ),
      ),
    );

    renderApp();

    const banner = await screen.findByTestId("runtime-mode-banner");
    expect(banner).toHaveAttribute("data-mode", "capabilities-unknown");
    expect(within(banner).getByText("能力未知")).toBeInTheDocument();
    expect(screen.queryByText("匿名只读")).not.toBeInTheDocument();
    expect(within(banner).getByRole("button", { name: "重试" })).toBeInTheDocument();
  });

  it("keeps gated content visible with a disabled reason instead of hiding it", async () => {
    renderApp({ initialEntries: ["/security"] });

    await screen.findByTestId("runtime-mode-banner");
    expect(
      await screen.findByText("没有权限", { selector: "div" }),
    ).toBeInTheDocument();
    expect(screen.getByText("需要 Admin API")).toBeInTheDocument();
  });
});
