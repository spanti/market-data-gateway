import { ADMIN_PATHS } from "@/api/endpoints/admin";
import { apiClient } from "@/api/http/client";
import { HttpError } from "@/api/http/types";
import { clearCsrfCredential, setCsrfCredential } from "@/runtime/csrf";
import { parseAdminSession, type AdminSession } from "@/runtime/session";
import { allowWrites } from "@/runtime/write-gate";

/**
 * OIDC Authorization Code + PKCE runs server-side. The SPA only navigates the
 * browser to the backend login entry; it never handles tokens or passwords.
 */
export const DEFAULT_RETURN_TO = "/monitoring";

function hasControlCharacters(value: string): boolean {
  for (const character of value) {
    const code = character.charCodeAt(0);
    if (code <= 0x1f || code === 0x7f) {
      return true;
    }
  }
  return false;
}

/**
 * `return_to` must be a same-origin relative SPA path. Absolute URLs,
 * protocol-relative hosts and control characters fall back to the default
 * route rather than being sent to the backend.
 */
export function sanitizeReturnTo(returnTo: string): string {
  const trimmed = returnTo.trim();
  if (!trimmed.startsWith("/") || trimmed.startsWith("//")) {
    return DEFAULT_RETURN_TO;
  }
  if (hasControlCharacters(trimmed)) {
    return DEFAULT_RETURN_TO;
  }
  return trimmed;
}

export function buildLoginUrl(returnTo: string): string {
  return `${ADMIN_PATHS.login}?return_to=${encodeURIComponent(
    sanitizeReturnTo(returnTo),
  )}`;
}

/** Server-side session invalidation. The POST requires the CSRF header. */
export async function logoutSession(): Promise<void> {
  await apiClient.post(ADMIN_PATHS.logout);
  clearCsrfCredential();
}

/**
 * Extends only the idle expiry (the server never extends the absolute limit)
 * and returns the fresh session payload including a new CSRF credential.
 */
export async function refreshAdminSession(): Promise<AdminSession> {
  const response = await apiClient.post<unknown>(ADMIN_PATHS.refresh);
  const session = parseAdminSession(response.data);
  if (!session) {
    throw new HttpError({
      kind: "rest",
      status: response.status,
      code: "unexpected_session_payload",
      message: "会话响应不符合契约",
      details: {},
      requestId: null,
    });
  }
  if (session.authenticated) {
    setCsrfCredential(session.csrf);
    allowWrites();
  } else {
    clearCsrfCredential();
  }
  return session;
}
