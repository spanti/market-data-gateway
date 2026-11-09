import { useQuery } from "@tanstack/react-query";
import { useCallback, useMemo, type ReactNode } from "react";
import { discoverCapabilities } from "@/api/capabilities";
import type { PermissionSubject } from "@/runtime/permissions";
import { RuntimeModeContext } from "@/runtime/runtime-mode-context";
import { fetchAdminSession } from "@/runtime/session";

export const CAPABILITIES_QUERY_KEY = ["runtime", "capabilities"] as const;
export const SESSION_QUERY_KEY = ["runtime", "admin-session"] as const;

/**
 * Boots capability discovery, then probes the Admin session only when an
 * Admin API actually exists. A missing role never defaults to a privileged
 * role; Current Contract never calls the session endpoint.
 */
export function RuntimeModeProvider({ children }: { children: ReactNode }) {
  const capabilitiesQuery = useQuery({
    queryKey: CAPABILITIES_QUERY_KEY,
    queryFn: discoverCapabilities,
    retry: false,
    staleTime: Infinity,
    gcTime: Infinity,
    refetchOnWindowFocus: false,
  });

  const capability = capabilitiesQuery.data ?? null;
  const isAdminMode = capability?.mode === "admin-api";

  const sessionQuery = useQuery({
    queryKey: SESSION_QUERY_KEY,
    queryFn: fetchAdminSession,
    enabled: isAdminMode,
    retry: false,
    staleTime: 30_000,
    refetchOnWindowFocus: false,
  });

  const session = isAdminMode ? (sessionQuery.data ?? null) : null;
  const role =
    session?.authenticated === true && session.access_role !== null
      ? session.access_role
      : null;

  const subject = useMemo<PermissionSubject>(() => {
    if (!capability) {
      return { kind: "unknown" };
    }
    switch (capability.mode) {
      case "current-contract":
        return { kind: "anonymous" };
      case "capabilities-unknown":
        return { kind: "unknown" };
      case "admin-api":
        return role ? { kind: "role", role } : { kind: "unauthenticated" };
    }
  }, [capability, role]);

  const refetchCapabilities = capabilitiesQuery.refetch;
  const refreshCapabilities = useCallback(() => {
    void refetchCapabilities();
  }, [refetchCapabilities]);

  const value = useMemo(
    () => ({
      capability,
      isLoading: capabilitiesQuery.isPending,
      session,
      sessionLoading: isAdminMode && sessionQuery.isPending,
      role,
      subject,
      refreshCapabilities,
    }),
    [
      capability,
      capabilitiesQuery.isPending,
      session,
      isAdminMode,
      sessionQuery.isPending,
      role,
      subject,
      refreshCapabilities,
    ],
  );

  return (
    <RuntimeModeContext.Provider value={value}>
      {children}
    </RuntimeModeContext.Provider>
  );
}
