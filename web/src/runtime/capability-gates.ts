import type { CapabilityState } from "@/api/capabilities";
import {
  capabilityDisabledReason,
  disabledReasonFor,
  type Action,
  type DisabledReason,
  type PermissionSubject,
} from "@/runtime/permissions";

/**
 * Single source of truth for the mode -> compatibility -> capability gate.
 *
 * Typed capability keys prevent the same strings from being copied across
 * screens. The gate fails closed: a missing capability document, an unknown
 * capability state or an incompatible Admin contract never enables a control.
 */

export const CAPABILITY_KEYS = {
  oidcSession: "oidc_session",
  wsAdminSession: "ws_admin_session",
  serviceLifecycle: "service_lifecycle",
  feedSubscription: "feed_subscription",
  feedFlowControl: "feed_flow_control",
  monitoringProxy: "monitoring_proxy",
  exportJob: "export_job",
  backfillJob: "backfill_job",
  persistentBackfillGaps: "persistent_backfill_gaps",
  operatorAudit: "operator_audit",
} as const;

export type CapabilityKey =
  (typeof CAPABILITY_KEYS)[keyof typeof CAPABILITY_KEYS];

export const ADMIN_API_INCOMPATIBLE_MESSAGE = "Admin API 版本不兼容";
export const CAPABILITIES_UNKNOWN_MESSAGE = "能力未知，暂不可用";
export const REQUIRES_LOGIN_MESSAGE = "需要登录";
export const REQUIRES_ADMIN_API_MESSAGE = "需要 Admin API";

export function capabilitiesUnknownReason(): DisabledReason {
  return { code: "capabilities-unknown", message: CAPABILITIES_UNKNOWN_MESSAGE };
}

export function requiresLoginReason(): DisabledReason {
  return { code: "requires-login", message: REQUIRES_LOGIN_MESSAGE };
}

export function requiresAdminApiReason(): DisabledReason {
  return { code: "requires-admin-api", message: REQUIRES_ADMIN_API_MESSAGE };
}

/** An Admin API that announced a contract version the console cannot use. */
export function adminCompatibilityReason(
  capability: CapabilityState | null,
): DisabledReason | null {
  if (capability?.mode !== "admin-api" || capability.compatible !== false) {
    return null;
  }
  return {
    code: "backend-unsupported",
    message: ADMIN_API_INCOMPATIBLE_MESSAGE,
  };
}

export type AdminCapabilityAvailability =
  | { kind: "not-admin" }
  | { kind: "disabled"; reason: DisabledReason }
  | { kind: "enabled" };

/**
 * Resolves an Admin capability by mode, version and capability key.
 *
 * `not-admin` means the active mode is Current Contract; callers decide
 * whether that is a real source (direct ops) or a `requires-admin-api` gap.
 */
export function adminCapabilityAvailability(
  capability: CapabilityState | null,
  key: CapabilityKey,
): AdminCapabilityAvailability {
  if (capability === null || capability.mode === "capabilities-unknown") {
    return { kind: "disabled", reason: capabilitiesUnknownReason() };
  }
  if (capability.mode === "current-contract") {
    return { kind: "not-admin" };
  }
  const incompatible = adminCompatibilityReason(capability);
  if (incompatible) {
    return { kind: "disabled", reason: incompatible };
  }
  if (!capability.document) {
    return { kind: "disabled", reason: requiresLoginReason() };
  }
  const capabilityReason = capabilityDisabledReason(
    capability.document.capabilities[key],
  );
  return capabilityReason
    ? { kind: "disabled", reason: capabilityReason }
    : { kind: "enabled" };
}

export interface ActionCapabilityGate {
  enabled: boolean;
  reason: DisabledReason | null;
}

/**
 * Combines the Access Role matrix with the Admin capability document. The role
 * gate always wins, so an Auditor can never enable a write control even when
 * the backend supports it.
 */
export function resolveActionCapabilityGate(
  subject: PermissionSubject,
  capability: CapabilityState | null,
  action: Action,
  key: CapabilityKey,
): ActionCapabilityGate {
  const roleReason = disabledReasonFor(subject, action);
  if (roleReason) {
    return { enabled: false, reason: roleReason };
  }

  const availability = adminCapabilityAvailability(capability, key);
  if (availability.kind === "enabled") {
    return { enabled: true, reason: null };
  }
  if (availability.kind === "not-admin") {
    // Current Contract has no Admin capability document; never assume one.
    return { enabled: false, reason: capabilitiesUnknownReason() };
  }
  return { enabled: false, reason: availability.reason };
}
