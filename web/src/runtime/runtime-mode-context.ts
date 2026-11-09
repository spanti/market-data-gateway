import { createContext, useContext } from "react";
import type { CapabilityState } from "@/api/capabilities";
import type { PermissionSubject } from "@/runtime/permissions";
import type { AccessRole } from "@/runtime/roles";
import type { AdminSession } from "@/runtime/session";

export interface RuntimeModeContextValue {
  /** null while capability discovery is still in flight. */
  capability: CapabilityState | null;
  isLoading: boolean;
  session: AdminSession | null;
  sessionLoading: boolean;
  role: AccessRole | null;
  subject: PermissionSubject;
  refreshCapabilities: () => void;
}

export const RuntimeModeContext =
  createContext<RuntimeModeContextValue | null>(null);

export function useRuntimeMode(): RuntimeModeContextValue {
  const value = useContext(RuntimeModeContext);
  if (!value) {
    throw new Error("useRuntimeMode must be used within RuntimeModeProvider");
  }
  return value;
}
