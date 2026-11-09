import type { CapabilityState } from "@/api/capabilities";
import {
  adminCapabilityAvailability,
  CAPABILITY_KEYS,
} from "@/runtime/capability-gates";
import type { DisabledReason } from "@/runtime/permissions";

/**
 * Chooses the only truthful Service Instance data source for the active
 * runtime mode.
 *
 * - Current Contract: the real per-Role `/status` + `/readyz` endpoints.
 * - Admin API: the authenticated `/api/v1/admin/service-instances` seam, and
 *   only when the `service_lifecycle` capability is enabled.
 * - Anything else: an explicit capability gap. Admin mode never falls back to
 *   unauthenticated `/status` or `/ops/*` reads.
 */
export type ServiceInstanceSource =
  | { kind: "direct" }
  | { kind: "admin" }
  | { kind: "gap"; reason: DisabledReason };

export const ADMIN_SERVICE_INSTANCE_GAP =
  "需要 Admin API 服务实例接口（未实现）";

function gap(code: DisabledReason["code"], message: string): ServiceInstanceSource {
  return { kind: "gap", reason: { code, message } };
}

export type ServiceLifecycleSupervisorState =
  | "available"
  | "unavailable"
  | "unsupported";

/**
 * The Supervisor lifecycle adapter gates both the Service Instance seam and
 * the lifecycle buttons. Modules map its state to their own copy, but they do
 * not each re-scan the adapter list.
 */
export function serviceLifecycleSupervisorState(
  capability: CapabilityState | null,
): ServiceLifecycleSupervisorState {
  if (capability?.mode !== "admin-api" || !capability.document) {
    return "available";
  }
  const supervisor = capability.document.adapters?.find(
    (adapter) =>
      adapter.name === "supervisor" && adapter.kind === "service_lifecycle",
  );
  if (supervisor?.state === "unavailable") {
    return "unavailable";
  }
  if (supervisor?.state === "unsupported") {
    return "unsupported";
  }
  return "available";
}

export function serviceInstanceSource(
  capability: CapabilityState | null,
): ServiceInstanceSource {
  const availability = adminCapabilityAvailability(
    capability,
    CAPABILITY_KEYS.serviceLifecycle,
  );

  if (availability.kind === "not-admin") {
    return { kind: "direct" };
  }
  if (availability.kind === "disabled") {
    // The generic capability gate reports a missing key as "后端未实现";
    // this seam has a more specific, operator-facing gap message.
    if (
      availability.reason.code === "backend-unsupported" &&
      availability.reason.message === "后端未实现"
    ) {
      return gap("backend-unsupported", ADMIN_SERVICE_INSTANCE_GAP);
    }
    return gap(availability.reason.code, availability.reason.message);
  }

  const supervisor = serviceLifecycleSupervisorState(capability);
  if (supervisor === "unavailable") {
    return gap("adapter-unavailable", "适配器不可用");
  }
  if (supervisor === "unsupported") {
    return gap("backend-unsupported", ADMIN_SERVICE_INSTANCE_GAP);
  }
  return { kind: "admin" };
}
