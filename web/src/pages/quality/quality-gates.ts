import type { CapabilityState } from "@/api/capabilities";
import {
  capabilityDisabledReason,
  disabledReasonFor,
  type Action,
  type DisabledReason,
  type PermissionSubject,
} from "@/runtime/permissions";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

export interface QualityGate {
  enabled: boolean;
  reason: DisabledReason | null;
}

/**
 * Combines the foundation Access Role matrix with the server capability
 * document. A missing capability fails closed; a role gate always wins so an
 * Auditor never sees an enabled write control even when the backend supports it.
 */
export function resolveQualityGate(
  subject: PermissionSubject,
  capability: CapabilityState | null,
  action: Action,
  capabilityKey: string,
): QualityGate {
  const roleReason = disabledReasonFor(subject, action);
  if (roleReason) {
    return { enabled: false, reason: roleReason };
  }

  if (capability?.mode !== "admin-api" || !capability.document) {
    // A 401 discovery response proves an Admin API exists but not its
    // capabilities. Nothing may be assumed.
    return {
      enabled: false,
      reason: { code: "capabilities-unknown", message: "能力未知，暂不可用" },
    };
  }

  const entry = capability.document.capabilities[capabilityKey];
  const capabilityReason = capabilityDisabledReason(entry);
  if (capabilityReason) {
    return { enabled: false, reason: capabilityReason };
  }

  return { enabled: true, reason: null };
}

export function useQualityGate(
  action: Action,
  capabilityKey: string,
): QualityGate {
  const { subject, capability } = useRuntimeMode();
  return resolveQualityGate(subject, capability, action, capabilityKey);
}
