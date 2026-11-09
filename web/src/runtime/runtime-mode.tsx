import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { discoverCapabilities } from "@/api/capabilities";
import { logoutSession, refreshAdminSession } from "@/runtime/auth";
import { clearCsrfCredential } from "@/runtime/csrf";
import { subscribeHttpErrors } from "@/runtime/http-error-bus";
import type { PermissionSubject } from "@/runtime/permissions";
import { RuntimeModeContext } from "@/runtime/runtime-mode-context";
import {
  ANONYMOUS_SESSION,
  fetchAdminSession,
  type AdminSession,
} from "@/runtime/session";
import {
  evaluateSessionExpiry,
  sessionIssueFromExpiry,
  type SessionIssueReason,
} from "@/runtime/session-expiry";
import {
  allowWrites,
  stopWrites,
  writesAreStopped,
} from "@/runtime/write-gate";

export const CAPABILITIES_QUERY_KEY = ["runtime", "capabilities"] as const;
export const SESSION_QUERY_KEY = ["runtime", "admin-session"] as const;

const SESSION_CLOCK_TICK_MS = 15_000;

function normalizeIssueReason(value: unknown): SessionIssueReason {
  switch (value) {
    case "idle_timeout":
    case "absolute_timeout":
    case "missing":
    case "idp_error":
      return value;
    default:
      return "invalid";
  }
}

/**
 * Boots capability discovery, then probes the Admin session only when an
 * Admin API actually exists. A missing role never defaults to a privileged
 * role; Current Contract never calls the session endpoint.
 *
 * A 401 clears the in-memory identity and CSRF token, stops all writes and
 * returns the shell to the login entry. A 403 does neither: it only names the
 * missing permission while the session stays valid.
 */
export function RuntimeModeProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const [sessionIssue, setSessionIssue] = useState<SessionIssueReason | null>(
    null,
  );
  const [authPending, setAuthPending] = useState(false);
  const [writesStopped, setWritesStopped] = useState(writesAreStopped);
  const [clockTick, setClockTick] = useState(0);

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

  const invalidateSession = useCallback(
    (reason: SessionIssueReason) => {
      clearCsrfCredential();
      stopWrites();
      setWritesStopped(true);
      setSessionIssue(reason);
      queryClient.setQueryData<AdminSession>(
        SESSION_QUERY_KEY,
        ANONYMOUS_SESSION,
      );
    },
    [queryClient],
  );

  // A 401 proves the session is gone. Ignore it when there was no
  // authenticated session to lose (for example, capability discovery itself
  // answered 401 because the Admin API requires auth).
  useEffect(
    () =>
      subscribeHttpErrors((error) => {
        if (error.status !== 401) {
          return;
        }
        const current =
          queryClient.getQueryData<AdminSession>(SESSION_QUERY_KEY);
        if (!current?.authenticated) {
          return;
        }
        invalidateSession(normalizeIssueReason(error.details.reason));
      }),
    [invalidateSession, queryClient],
  );

  // Keep the crossing of the warning/expiry boundary fresh without ever
  // generating fake user activity.
  useEffect(() => {
    const id = window.setInterval(() => {
      setClockTick((tick) => tick + 1);
    }, SESSION_CLOCK_TICK_MS);
    return () => window.clearInterval(id);
  }, []);

  const sessionExpiry = useMemo(() => {
    // clockTick is the intentional invalidation signal for time-based state.
    void clockTick;
    return evaluateSessionExpiry(session, Date.now());
  }, [session, clockTick]);

  useEffect(() => {
    if (sessionExpiry.state === "expired") {
      invalidateSession(sessionIssueFromExpiry(sessionExpiry) ?? "invalid");
    }
  }, [sessionExpiry, invalidateSession]);

  // A fresh authenticated payload re-enables the write path; an anonymous
  // payload without a 401 (logout) leaves it stopped on purpose. The session
  // issue is cleared only by an explicit refresh/login, never here: an expiry
  // invalidation and this effect can observe the same stale session render.
  useEffect(() => {
    if (session?.authenticated) {
      allowWrites();
    }
    setWritesStopped(writesAreStopped());
  }, [session]);

  const expired = sessionExpiry.state === "expired";
  const authenticated = session?.authenticated === true && !expired;
  const role =
    authenticated && session?.access_role ? session.access_role : null;

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

  const refreshSession = useCallback(async () => {
    setAuthPending(true);
    try {
      const next = await refreshAdminSession();
      queryClient.setQueryData<AdminSession>(SESSION_QUERY_KEY, next);
      setSessionIssue(null);
      setWritesStopped(writesAreStopped());
    } finally {
      setAuthPending(false);
    }
  }, [queryClient]);

  const logout = useCallback(async () => {
    setAuthPending(true);
    try {
      await logoutSession();
      clearCsrfCredential();
      stopWrites();
      setWritesStopped(true);
      setSessionIssue(null);
      queryClient.setQueryData<AdminSession>(
        SESSION_QUERY_KEY,
        ANONYMOUS_SESSION,
      );
    } finally {
      setAuthPending(false);
    }
  }, [queryClient]);

  const value = useMemo(
    () => ({
      capability,
      isLoading: capabilitiesQuery.isPending,
      session,
      sessionLoading: isAdminMode && sessionQuery.isPending,
      role,
      subject,
      refreshCapabilities,
      sessionIssue,
      sessionExpiry,
      writesStopped,
      authPending,
      logout,
      refreshSession,
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
      sessionIssue,
      sessionExpiry,
      writesStopped,
      authPending,
      logout,
      refreshSession,
    ],
  );

  return (
    <RuntimeModeContext.Provider value={value}>
      {children}
    </RuntimeModeContext.Provider>
  );
}
