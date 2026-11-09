import type { ReactNode } from "react";
import { ErrorState } from "@/components/states";
import { can, disabledReasonFor, type Action } from "@/runtime/permissions";
import { usePermissionSubject } from "@/runtime/use-permission";

export interface ForbiddenStateProps {
  message: string;
  requestId?: string | null;
}

export function ForbiddenState({
  message,
  requestId,
}: ForbiddenStateProps) {
  return (
    <ErrorState
      forbidden
      title="没有权限"
      description={message}
      requestId={requestId}
    />
  );
}

export interface PermissionGateProps {
  action: Action;
  children: ReactNode;
  fallback?: ReactNode;
}

/**
 * Route/section guard. Direct navigation renders the same forbidden state;
 * it is not a security boundary (the server re-checks every request).
 */
export function PermissionGate({
  action,
  children,
  fallback,
}: PermissionGateProps) {
  const subject = usePermissionSubject();
  if (can(subject, action)) {
    return <>{children}</>;
  }
  if (fallback !== undefined) {
    return <>{fallback}</>;
  }
  const reason = disabledReasonFor(subject, action);
  return <ForbiddenState message={reason?.message ?? "需要更高权限"} />;
}
