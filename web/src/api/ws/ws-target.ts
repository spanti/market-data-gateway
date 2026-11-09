import type { CapabilityState } from "@/api/capabilities";
import {
  adminCapabilityAvailability,
  CAPABILITY_KEYS,
  capabilitiesUnknownReason,
  type CapabilityKey,
} from "@/runtime/capability-gates";
import type { DisabledReason } from "@/runtime/permissions";
import { deriveWsUrl, type WsLocationLike } from "@/api/ws/transport";

/**
 * Selects the WebSocket transport for the active runtime mode.
 *
 * - Current Contract uses the public same-origin `/ws` Streaming Interface.
 * - Admin API uses only the authenticated Admin WebSocket path declared by
 *   `ws_admin_session`. It never falls back to the unauthenticated public
 *   `/ws`, which Admin-mode Nginx denies anyway.
 * - Pending/unknown capability discovery and an unsupported/unavailable Admin
 *   WS are capability gaps: no socket is opened and no silent downgrade
 *   happens.
 */
export type WsTargetSource = "current-contract" | "admin-api" | "unknown";

export interface WsTarget {
  url: string | null;
  reason: DisabledReason | null;
  source: WsTargetSource;
}

export const DEFAULT_ADMIN_WS_PATH = "/api/v1/admin/ws";

const WS_ADMIN_SESSION_KEY: CapabilityKey = CAPABILITY_KEYS.wsAdminSession;

function adminWsUrl(location: WsLocationLike, path: string): string {
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const normalized = path.startsWith("/") ? path : `/${path}`;
  return `${protocol}//${location.host}${normalized}`;
}

function unavailable(source: WsTargetSource, reason: DisabledReason): WsTarget {
  return { url: null, reason, source };
}

export function resolveWsTarget(
  capability: CapabilityState | null,
  location: WsLocationLike = typeof window === "undefined"
    ? { protocol: "http:", host: "localhost" }
    : window.location,
): WsTarget {
  if (capability === null || capability.mode === "capabilities-unknown") {
    return unavailable("unknown", capabilitiesUnknownReason());
  }

  if (capability.mode === "current-contract") {
    return {
      url: deriveWsUrl(location, undefined),
      reason: null,
      source: "current-contract",
    };
  }

  const availability = adminCapabilityAvailability(
    capability,
    WS_ADMIN_SESSION_KEY,
  );
  if (availability.kind !== "enabled") {
    // `not-admin` is impossible here because the mode was checked above.
    return unavailable(
      "admin-api",
      availability.kind === "disabled"
        ? availability.reason
        : capabilitiesUnknownReason(),
    );
  }

  const entry = capability.document?.capabilities.ws_admin_session;
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
