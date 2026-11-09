import { describe, expect, it } from "vitest";
import {
  ANONYMOUS_SESSION,
  type AdminSession,
} from "@/runtime/session";
import {
  evaluateSessionExpiry,
  SESSION_WARNING_SECONDS,
} from "@/runtime/session-expiry";

const RECEIVED_AT = Date.parse("2026-11-09T12:00:00.000Z");

function sessionAt(init: {
  idleInMs: number;
  absoluteInMs: number;
  serverTimeOffsetMs?: number;
}): AdminSession {
  const offset = init.serverTimeOffsetMs ?? 0;
  return {
    authenticated: true,
    access_role: "Operator",
    user: { subject: "subject", displayName: "Ada", email: null },
    server_time: new Date(RECEIVED_AT + offset).toISOString(),
    session: {
      issued_at: new Date(RECEIVED_AT - 60_000).toISOString(),
      last_activity_at: new Date(RECEIVED_AT).toISOString(),
      idle_expires_at: new Date(RECEIVED_AT + init.idleInMs).toISOString(),
      absolute_expires_at: new Date(RECEIVED_AT + init.absoluteInMs).toISOString(),
    },
    csrf: { header_name: "X-CSRF-Token", token: "token" },
    received_at: RECEIVED_AT,
  };
}

describe("session expiry evaluation", () => {
  it("reports none while both limits are beyond the five-minute warning", () => {
    const state = evaluateSessionExpiry(
      sessionAt({ idleInMs: 10 * 60_000, absoluteInMs: 8 * 3_600_000 }),
      RECEIVED_AT,
    );

    expect(SESSION_WARNING_SECONDS).toBe(300);
    expect(state.state).toBe("none");
    expect(state.bound).toBeNull();
  });

  it("warns inside five minutes of the idle expiry", () => {
    const state = evaluateSessionExpiry(
      sessionAt({ idleInMs: 4 * 60_000, absoluteInMs: 8 * 3_600_000 }),
      RECEIVED_AT,
    );

    expect(state.state).toBe("warning");
    expect(state.bound).toBe("idle");
    expect(state.remainingSeconds).toBe(240);
  });

  it("warns when the absolute limit is the earlier bound", () => {
    const state = evaluateSessionExpiry(
      sessionAt({ idleInMs: 30 * 60_000, absoluteInMs: 2 * 60_000 }),
      RECEIVED_AT,
    );

    expect(state.state).toBe("warning");
    expect(state.bound).toBe("absolute");
    expect(state.remainingSeconds).toBe(120);
  });

  it("reports expiry once the idle limit has passed", () => {
    const state = evaluateSessionExpiry(
      sessionAt({ idleInMs: -1_000, absoluteInMs: 8 * 3_600_000 }),
      RECEIVED_AT,
    );

    expect(state.state).toBe("expired");
    expect(state.bound).toBe("idle");
  });

  it("reports expiry on the absolute limit even with recent idle activity", () => {
    const state = evaluateSessionExpiry(
      sessionAt({ idleInMs: 30 * 60_000, absoluteInMs: -1_000 }),
      RECEIVED_AT,
    );

    expect(state.state).toBe("expired");
    expect(state.bound).toBe("absolute");
  });

  it("corrects the countdown using server_time against the client receive time", () => {
    const skew = 60_000; // server clock is one minute ahead of the browser
    const shifted = sessionAt({
      idleInMs: 5 * 60_000 + 30_000,
      absoluteInMs: 8 * 3_600_000,
      serverTimeOffsetMs: skew,
    });

    const state = evaluateSessionExpiry(shifted, RECEIVED_AT);

    expect(state.state).toBe("warning");
    expect(state.bound).toBe("idle");
    expect(state.remainingSeconds).toBe(270);
  });

  it("never evaluates an unknown identity as an active session", () => {
    expect(evaluateSessionExpiry(null, RECEIVED_AT).state).toBe("none");
    expect(evaluateSessionExpiry(ANONYMOUS_SESSION, RECEIVED_AT).state).toBe(
      "none",
    );
  });
});
