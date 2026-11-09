import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { server } from "@/test/server";
import {
  buildLoginUrl,
  logoutSession,
  refreshAdminSession,
} from "@/runtime/auth";
import { clearCsrfCredential, getCsrfCredential, setCsrfCredential } from "@/runtime/csrf";
import { resetWriteGate } from "@/runtime/write-gate";

beforeEach(() => {
  clearCsrfCredential();
  resetWriteGate();
});

describe("OIDC login entry", () => {
  it("builds a same-origin browser navigation with return_to", () => {
    expect(buildLoginUrl("/security?foo=bar")).toBe(
      "/api/v1/admin/auth/login?return_to=%2Fsecurity%3Ffoo%3Dbar",
    );
  });

  it("refuses absolute, protocol-relative and control-character return paths", () => {
    for (const unsafe of [
      "https://evil.example/steal",
      "//evil.example/steal",
      "javascript:alert(1)",
      "/monitoring\u0000",
      "",
    ]) {
      expect(buildLoginUrl(unsafe)).toBe(
        "/api/v1/admin/auth/login?return_to=%2Fmonitoring",
      );
    }
  });
});

describe("session lifecycle requests", () => {
  it("logs out with the in-memory CSRF token on the unsafe POST", async () => {
    setCsrfCredential({ header_name: "X-CSRF-Token", token: "csrf-123" });
    let receivedToken: string | null = null;
    server.use(
      http.post("*/api/v1/admin/auth/logout", ({ request }) => {
        receivedToken = request.headers.get("X-CSRF-Token");
        return new HttpResponse(null, { status: 204 });
      }),
    );

    await logoutSession();

    expect(receivedToken).toBe("csrf-123");
    expect(getCsrfCredential()).toBeNull();
  });

  it("refreshes the session and updates the in-memory CSRF credential", async () => {
    setCsrfCredential({ header_name: "X-CSRF-Token", token: "old-token" });
    server.use(
      http.post("*/api/v1/admin/auth/refresh", ({ request }) => {
        expect(request.headers.get("X-CSRF-Token")).toBe("old-token");
        return HttpResponse.json({
          authenticated: true,
          server_time: "2026-11-09T12:05:00.000000000Z",
          user: {
            subject: "oidc-subject-123",
            display_name: "Ada Operator",
            email: "ada@example.com",
          },
          access_role: "Operator",
          session: {
            issued_at: "2026-11-09T09:00:00.000000000Z",
            last_activity_at: "2026-11-09T12:05:00.000000000Z",
            idle_expires_at: "2026-11-09T12:35:00.000000000Z",
            absolute_expires_at: "2026-11-09T17:00:00.000000000Z",
          },
          csrf: { header_name: "X-CSRF-Token", token: "new-token" },
        });
      }),
    );

    const session = await refreshAdminSession();

    expect(session.access_role).toBe("Operator");
    expect(getCsrfCredential()).toEqual({
      header_name: "X-CSRF-Token",
      token: "new-token",
    });
  });

  it("rejects a refresh response that does not match the session contract", async () => {
    server.use(
      http.post("*/api/v1/admin/auth/refresh", () =>
        HttpResponse.json({ authenticated: "sometimes" }),
      ),
    );

    await expect(refreshAdminSession()).rejects.toMatchObject({
      code: "unexpected_session_payload",
    });
  });
});
