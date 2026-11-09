import {
  can,
  disabledReasonFor,
  type Action,
  type DisabledReason,
  type PermissionSubject,
} from "@/runtime/permissions";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

export function usePermissionSubject(): PermissionSubject {
  return useRuntimeMode().subject;
}

export function useActionReason(action: Action): DisabledReason | null {
  return disabledReasonFor(usePermissionSubject(), action);
}

export function useCan(action: Action): boolean {
  return can(usePermissionSubject(), action);
}
