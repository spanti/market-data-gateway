import { createContext, useContext } from "react";
import type { CapabilityState } from "@/api/capabilities";
import type { PermissionSubject } from "@/runtime/permissions";
import type { AccessRole } from "@/runtime/roles";
import type { AdminSession } from "@/runtime/session";
import type {
  SessionExpiryState,
  SessionIssueReason,
} from "@/runtime/session-expiry";

export interface RuntimeModeContextValue {
  /** null while capability discovery is still in flight. */
  capability: CapabilityState | null;
  isLoading: boolean;
  session: AdminSession | null;
  sessionLoading: boolean;
  role: AccessRole | null;
  subject: PermissionSubject;
  refreshCapabilities: () => void;
  /** Set after a 401 or a locally detected expiry; never on a 403. */
  sessionIssue: SessionIssueReason | null;
  sessionExpiry: SessionExpiryState;
  /** True when the write gate blocks mutations until a new session exists. */
  writesStopped: boolean;
  authPending: boolean;
  logout: () => Promise<void>;
  /** Extends the idle limit only, via `POST /auth/refresh`. */
  refreshSession: () => Promise<void>;
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
