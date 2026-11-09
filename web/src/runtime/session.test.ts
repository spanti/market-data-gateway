import { describe, expect, it } from "vitest";
import { ANONYMOUS_SESSION, parseAdminSession } from "@/runtime/session";

const authenticatedPayload = {
  authenticated: true,
  server_time: "2026-11-09T12:00:00.000000000Z",
  user: {
    subject: "oidc-subject-123",
    display_name: "Ada Operator",
    email: "ada@example.com",
  },
  access_role: "Operator",
  session: {
    issued_at: "2026-11-09T09:00:00.000000000Z",
    last_activity_at: "2026-11-09T11:58:00.000000000Z",
    idle_expires_at: "2026-11-09T12:28:00.000000000Z",
    absolute_expires_at: "2026-11-09T17:00:00.000000000Z",
  },
  csrf: {
    header_name: "X-CSRF-Token",
    token: "opaque-random-token",
  },
};

describe("Admin session parsing", () => {
  it("parses identity, role, session times and the in-memory CSRF credential", () => {
    const session = parseAdminSession(authenticatedPayload);

    expect(session).not.toBeNull();
    expect(session?.authenticated).toBe(true);
    expect(session?.access_role).toBe("Operator");
    expect(session?.user).toEqual({
      subject: "oidc-subject-123",
      displayName: "Ada Operator",
      email: "ada@example.com",
    });
    expect(session?.session).toEqual({
      issued_at: "2026-11-09T09:00:00.000000000Z",
      last_activity_at: "2026-11-09T11:58:00.000000000Z",
      idle_expires_at: "2026-11-09T12:28:00.000000000Z",
      absolute_expires_at: "2026-11-09T17:00:00.000000000Z",
    });
    expect(session?.csrf).toEqual({
      header_name: "X-CSRF-Token",
      token: "opaque-random-token",
    });
    expect(session?.csrfToken).toBe("opaque-random-token");
  });

  it("defaults the CSRF header name when the server omits it", () => {
    const session = parseAdminSession({
      ...authenticatedPayload,
      csrf: { token: "token-only" },
    });

    expect(session?.csrf).toEqual({
      header_name: "X-CSRF-Token",
      token: "token-only",
    });
  });

  it("parses the anonymous response without granting a role or token", () => {
    const session = parseAdminSession({
      authenticated: false,
      server_time: "2026-11-09T12:00:00.000000000Z",
      user: null,
      access_role: null,
      session: null,
      csrf: null,
    });

    expect(session).toMatchObject({
      authenticated: false,
      access_role: null,
      user: null,
      session: null,
      csrf: null,
    });
  });

  it("treats a missing or unknown Access Role as no permission, never a default role", () => {
    for (const role of [undefined, null, "Root", "operator"]) {
      const session = parseAdminSession({
        ...authenticatedPayload,
        access_role: role,
      });

      expect(session?.authenticated).toBe(true);
      expect(session?.access_role).toBeNull();
    }
  });

  it("drops identity, session times and CSRF token from an unauthenticated payload", () => {
    const session = parseAdminSession({
      ...authenticatedPayload,
      authenticated: false,
    });

    expect(session?.access_role).toBeNull();
    expect(session?.user).toBeNull();
    expect(session?.session).toBeNull();
    expect(session?.csrf).toBeNull();
  });

  it("requires the authenticated boolean and rejects malformed payloads", () => {
    expect(parseAdminSession(null)).toBeNull();
    expect(parseAdminSession([])).toBeNull();
    expect(parseAdminSession({ user: {} })).toBeNull();
    expect(parseAdminSession({ authenticated: "yes" })).toBeNull();
  });

  it("uses an explicit anonymous value that carries no material", () => {
    expect(ANONYMOUS_SESSION.authenticated).toBe(false);
    expect(ANONYMOUS_SESSION.access_role).toBeNull();
    expect(ANONYMOUS_SESSION.csrf).toBeNull();
    expect(ANONYMOUS_SESSION.csrfToken).toBeNull();
    expect(ANONYMOUS_SESSION.session).toBeNull();
  });
});
