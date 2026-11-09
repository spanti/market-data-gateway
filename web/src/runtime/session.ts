import { apiClient } from "@/api/http/client";
import { isAccessRole, type AccessRole } from "@/runtime/roles";

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

export interface AdminSession {
  authenticated: boolean;
  access_role: AccessRole | null;
  user: AdminSessionUser | null;
  server_time: string | null;
  csrfToken: string | null;
}

export const ANONYMOUS_SESSION: AdminSession = {
  authenticated: false,
  access_role: null,
  user: null,
  server_time: null,
  csrfToken: null,
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function parseAdminSession(data: unknown): AdminSession | null {
  if (!isRecord(data) || typeof data.authenticated !== "boolean") {
    return null;
  }

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
  const csrfToken =
    csrfRecord && typeof csrfRecord.token === "string"
      ? csrfRecord.token
      : null;

  return {
    authenticated: data.authenticated,
    // Missing/unknown role means no permission, never a default role.
    access_role: data.authenticated ? role : null,
    user: data.authenticated ? user : null,
    server_time:
      typeof data.server_time === "string" ? data.server_time : null,
    csrfToken: data.authenticated ? csrfToken : null,
  };
}

export async function fetchAdminSession(): Promise<AdminSession> {
  try {
    const response = await apiClient.get<unknown>(
      "/api/v1/admin/auth/session",
    );
    return parseAdminSession(response.data) ?? ANONYMOUS_SESSION;
  } catch {
    // A missing/failed session probe grants nothing.
    return ANONYMOUS_SESSION;
  }
}
