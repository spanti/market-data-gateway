import { screen, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import { renderApp } from "@/test/render-app";
import { server } from "@/test/server";

/**
 * Console mode copy is dynamic: it must not say "Current Contract" while the
 * app is in Admin API or capability-unknown mode.
 */

function adminCapabilityDocument() {
  const now = new Date().toISOString();
  return {
    schema_version: 1,
    server_time: now,
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
      ws_admin_session: { state: "enabled", path: "/api/v1/admin/ws" },
      feed_subscription: { state: "unsupported" },
      feed_flow_control: { state: "unsupported" },
      service_lifecycle: { state: "unsupported" },
    },
  };
}

function futureSession() {
  const now = Date.now();
  return {
    authenticated: true,
    server_time: new Date(now).toISOString(),
    user: {
      subject: "oidc-subject-1",
      display_name: "Ada Admin",
      email: null,
    },
    access_role: "SuperAdmin",
    session: {
      issued_at: new Date(now - 60_000).toISOString(),
      last_activity_at: new Date(now).toISOString(),
      idle_expires_at: new Date(now + 30 * 60_000).toISOString(),
      absolute_expires_at: new Date(now + 8 * 3_600_000).toISOString(),
    },
    csrf: { header_name: "X-CSRF-Token", token: "csrf" },
  };
}

function consoleHeader() {
  const heading = screen.getByRole("heading", {
    name: "Gateway Console",
    level: 1,
  });
  const header = heading.closest('[data-slot="page-header"]');
  if (!(header instanceof HTMLElement)) {
    throw new Error("Console page header not found");
  }
  return within(header);
}

async function findConsoleHeader() {
  await screen.findByRole("heading", { name: "Gateway Console", level: 1 });
  return consoleHeader();
}

describe("Console shell mode status", () => {
  it("describes Current Contract real status and the public /ws", async () => {
    renderApp({ initialEntries: ["/console"] });

    const header = await findConsoleHeader();
    expect(await header.findByText("Current Contract")).toBeInTheDocument();
    expect(
      header.getByText(/真实状态与公开 \/ws 可用/),
    ).toBeInTheDocument();
    expect(header.getByText(/管理写需要 Admin API/)).toBeInTheDocument();
    expect(
      header.getByRole("button", { name: "Gateway Console 说明" }),
    ).toBeInTheDocument();
  });

  it("describes the Admin service-instance seam and the Admin WS", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(adminCapabilityDocument()),
      ),
      http.get("*/api/v1/admin/auth/session", () =>
        HttpResponse.json(futureSession()),
      ),
    );

    renderApp({ initialEntries: ["/console"] });

    const header = await findConsoleHeader();
    expect(await header.findByText("Admin API")).toBeInTheDocument();
    expect(header.getByText(/service-instance 接缝/)).toBeInTheDocument();
    expect(header.getByText(/仅使用 \/api\/v1\/admin\/ws/)).toBeInTheDocument();
    expect(header.queryByText("Current Contract")).not.toBeInTheDocument();
  });

  it("says unknown and no connection when capabilities are unknown", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(
          {
            error: {
              code: "internal_error",
              message: "discovery failed",
              details: {},
              request_id: "cap-500",
            },
          },
          { status: 500 },
        ),
      ),
    );

    renderApp({ initialEntries: ["/console"] });

    const header = await findConsoleHeader();
    expect(await header.findByText("能力未知")).toBeInTheDocument();
    expect(
      header.getByText(/不连接任何 WebSocket，管理写保持禁用/),
    ).toBeInTheDocument();
    expect(header.queryByText("Current Contract")).not.toBeInTheDocument();
  });
});
