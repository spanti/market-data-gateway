import type { CapabilityState } from "@/api/capabilities";
import {
  capabilityDisabledReason,
  type DisabledReason,
} from "@/runtime/permissions";
import { deriveWsUrl, type WsLocationLike } from "@/api/ws/transport";

/**
 * Selects the WebSocket transport for the active runtime mode.
 *
 * - Current Contract (and capability-unknown) uses the public same-origin
 *   `/ws` Streaming Interface.
 * - Admin API uses only the authenticated Admin WebSocket path declared by
 *   `ws_admin_session`. It never falls back to the unauthenticated public
 *   `/ws`, which Admin-mode Nginx denies anyway.
 * - An unsupported/unavailable Admin WS is a capability gap, not a silent
 *   downgrade.
 */
export type WsTargetSource = "current-contract" | "admin-api";

export interface WsTarget {
  url: string | null;
  reason: DisabledReason | null;
  source: WsTargetSource;
}

export const DEFAULT_ADMIN_WS_PATH = "/api/v1/admin/ws";

function adminWsUrl(location: WsLocationLike, path: string): string {
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const normalized = path.startsWith("/") ? path : `/${path}`;
  return `${protocol}//${location.host}${normalized}`;
}

export function resolveWsTarget(
  capability: CapabilityState | null,
  location: WsLocationLike = typeof window === "undefined"
    ? { protocol: "http:", host: "localhost" }
    : window.location,
): WsTarget {
  if (!capability || capability.mode !== "admin-api") {
    return {
      url: deriveWsUrl(location, undefined),
      reason: null,
      source: "current-contract",
    };
  }

  if (capability.compatible === false) {
    return {
      url: null,
      reason: { code: "backend-unsupported", message: "Admin API 版本不兼容" },
      source: "admin-api",
    };
  }
  if (!capability.document) {
    return {
      url: null,
      reason: { code: "requires-login", message: "需要登录" },
      source: "admin-api",
    };
  }

  const entry = capability.document.capabilities.ws_admin_session;
  const reason = capabilityDisabledReason(entry);
  if (reason) {
    return { url: null, reason, source: "admin-api" };
  }
  const declaredPath =
    typeof entry?.path === "string" && entry.path.length > 0
      ? entry.path
      : DEFAULT_ADMIN_WS_PATH;
  return {
    url: adminWsUrl(location, declaredPath),
    reason: null,
    source: "admin-api",
  };
}
