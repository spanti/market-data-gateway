import type { CapabilityState } from "@/api/capabilities";
import type { CapabilityKey } from "@/runtime/capability-gates";
import {
  resolveActionCapabilityGate,
  type ActionCapabilityGate,
} from "@/runtime/capability-gates";
import type { Action } from "@/runtime/permissions";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

export type QualityGate = ActionCapabilityGate;

/**
 * Combines the foundation Access Role matrix with the server capability
 * document through the shared capability gate. A missing capability fails
 * closed; a role gate always wins.
 */
export function resolveQualityGate(
  subject: Parameters<typeof resolveActionCapabilityGate>[0],
  capability: CapabilityState | null,
  action: Action,
  capabilityKey: CapabilityKey,
): QualityGate {
  return resolveActionCapabilityGate(
    subject,
    capability,
    action,
    capabilityKey,
  );
}

export function useQualityGate(
  action: Action,
  capabilityKey: CapabilityKey,
): QualityGate {
  const { subject, capability } = useRuntimeMode();
  return resolveQualityGate(subject, capability, action, capabilityKey);
}
