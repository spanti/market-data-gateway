import { apiClient } from "@/api/http/client";
import { isAccessRole, type AccessRole } from "@/runtime/roles";
import { clearCsrfCredential, setCsrfCredential } from "@/runtime/csrf";
import { allowWrites } from "@/runtime/write-gate";

/**
 * Admin session (contract target). The browser only ever receives an opaque
 * HttpOnly session cookie; the CSRF token below is held in memory and never
 * written to localStorage/sessionStorage or a URL.
 */

export interface AdminSessionUser {
  subject: string;
  displayName: string;
  email: string | null;
}

export interface AdminSessionTimes {
  issued_at: string;
  last_activity_at: string;
  idle_expires_at: string;
  absolute_expires_at: string;
}

export interface AdminSessionCsrf {
  header_name: string;
  token: string;
}

export interface AdminSession {
  authenticated: boolean;
  access_role: AccessRole | null;
  user: AdminSessionUser | null;
  server_time: string | null;
  session: AdminSessionTimes | null;
  csrf: AdminSessionCsrf | null;
  /**
   * Compatibility projection of `csrf.token` for in-memory header adapters
   * (Backfill/Export write helpers). Same memory-only value; never persisted.
   */
  csrfToken: string | null;
  /** Client clock at parse time, used to correct `server_time` skew. */
  received_at: number;
}

export const ANONYMOUS_SESSION: AdminSession = {
  authenticated: false,
  access_role: null,
  user: null,
  server_time: null,
  session: null,
  csrf: null,
  csrfToken: null,
  received_at: 0,
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseSessionTimes(value: unknown): AdminSessionTimes | null {
  if (!isRecord(value)) {
    return null;
  }
  const { issued_at, last_activity_at, idle_expires_at, absolute_expires_at } =
    value;
  if (
    typeof issued_at !== "string" ||
    typeof last_activity_at !== "string" ||
    typeof idle_expires_at !== "string" ||
    typeof absolute_expires_at !== "string"
  ) {
    return null;
  }
  return {
    issued_at,
    last_activity_at,
    idle_expires_at,
    absolute_expires_at,
  };
}

export function parseAdminSession(
  data: unknown,
  receivedAt: number = Date.now(),
): AdminSession | null {
  if (!isRecord(data) || typeof data.authenticated !== "boolean") {
    return null;
  }

  // Missing/unknown role means no permission, never a default role.
  const role = isAccessRole(data.access_role) ? data.access_role : null;

  const userRecord = isRecord(data.user) ? data.user : null;
  const user =
    userRecord && typeof userRecord.subject === "string"
      ? {
          subject: userRecord.subject,
          displayName:
            typeof userRecord.display_name === "string"
              ? userRecord.display_name
              : userRecord.subject,
          email: typeof userRecord.email === "string" ? userRecord.email : null,
        }
      : null;

  const csrfRecord = isRecord(data.csrf) ? data.csrf : null;
  const csrf: AdminSessionCsrf | null =
    csrfRecord && typeof csrfRecord.token === "string" && csrfRecord.token
      ? {
          header_name:
            typeof csrfRecord.header_name === "string" &&
            csrfRecord.header_name
              ? csrfRecord.header_name
              : "X-CSRF-Token",
          token: csrfRecord.token,
        }
      : null;

  return {
    authenticated: data.authenticated,
    access_role: data.authenticated ? role : null,
    user: data.authenticated ? user : null,
    server_time:
      typeof data.server_time === "string" ? data.server_time : null,
    session: data.authenticated ? parseSessionTimes(data.session) : null,
    csrf: data.authenticated ? csrf : null,
    csrfToken: data.authenticated && csrf ? csrf.token : null,
    received_at: receivedAt,
  };
}

/**
 * Probes the session endpoint. Side effects stay limited to in-memory auth
 * state: a valid authenticated payload enables the CSRF credential and write
 * path; an anonymous payload drops the credential.
 */
export async function fetchAdminSession(): Promise<AdminSession> {
  try {
    const response = await apiClient.get<unknown>(
      "/api/v1/admin/auth/session",
    );
    const session = parseAdminSession(response.data);
    if (!session) {
      return ANONYMOUS_SESSION;
    }
    if (session.authenticated) {
      setCsrfCredential(session.csrf);
      allowWrites();
    } else {
      clearCsrfCredential();
    }
    return session;
  } catch {
    // A missing/failed session probe grants nothing.
    clearCsrfCredential();
    return ANONYMOUS_SESSION;
  }
}
