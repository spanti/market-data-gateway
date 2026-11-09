import type { AdminSession } from "@/runtime/session";

/**
 * Session lifetime is authoritative on the server: 30 minutes idle and
 * 8 hours absolute. The UI only warns five minutes before the earlier bound
 * and never invents activity to extend a session.
 */

export const SESSION_WARNING_SECONDS = 300;

export type SessionExpiryBound = "idle" | "absolute";

export interface SessionExpiryState {
  state: "none" | "warning" | "expired";
  bound: SessionExpiryBound | null;
  remainingMs: number | null;
  remainingSeconds: number | null;
}

const NO_EXPIRY: SessionExpiryState = {
  state: "none",
  bound: null,
  remainingMs: null,
  remainingSeconds: null,
};

function parseInstant(value: string | null | undefined): number | null {
  if (typeof value !== "string") {
    return null;
  }
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? null : ms;
}

/**
 * `nowMs` is the client clock. When the session carries `server_time`, the
 * difference from the client receive time corrects the countdown for clock
 * skew without trusting a locally persisted value.
 */
export function evaluateSessionExpiry(
  session: AdminSession | null,
  nowMs: number,
  warningSeconds: number = SESSION_WARNING_SECONDS,
): SessionExpiryState {
  if (!session?.authenticated || !session.session) {
    return NO_EXPIRY;
  }

  const idleAt = parseInstant(session.session.idle_expires_at);
  const absoluteAt = parseInstant(session.session.absolute_expires_at);
  if (idleAt === null || absoluteAt === null) {
    return NO_EXPIRY;
  }

  const serverTime = parseInstant(session.server_time);
  const skew =
    serverTime !== null && session.received_at > 0
      ? serverTime - session.received_at
      : 0;
  const adjustedNow = nowMs + skew;

  const idleRemaining = idleAt - adjustedNow;
  const absoluteRemaining = absoluteAt - adjustedNow;
  const bound: SessionExpiryBound =
    idleRemaining <= absoluteRemaining ? "idle" : "absolute";
  const remainingMs = Math.min(idleRemaining, absoluteRemaining);

  let state: SessionExpiryState["state"] = "none";
  if (remainingMs <= 0) {
    state = "expired";
  } else if (remainingMs <= warningSeconds * 1000) {
    state = "warning";
  }

  return {
    state,
    bound: state === "none" ? null : bound,
    remainingMs,
    remainingSeconds: Math.ceil(remainingMs / 1000),
  };
}

export type SessionIssueReason =
  | "idle_timeout"
  | "absolute_timeout"
  | "invalid"
  | "missing"
  | "idp_error";

export function sessionIssueFromExpiry(
  expiry: SessionExpiryState,
): SessionIssueReason | null {
  if (expiry.state !== "expired" || expiry.bound === null) {
    return null;
  }
  return expiry.bound === "absolute" ? "absolute_timeout" : "idle_timeout";
}
