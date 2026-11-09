import { ForbiddenState, PermissionGate } from "@/components/permission-gate";
import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { OperatorAuditQuerySection } from "@/pages/security/OperatorAuditQuerySection";
import { IdentityPanel } from "@/pages/security/IdentityPanel";
import {
  adminCapabilityAvailability,
  CAPABILITY_KEYS,
} from "@/runtime/capability-gates";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

export function SecurityPage() {
  const { capability, role } = useRuntimeMode();

  const auditAvailability =
    capability?.mode === "admin-api"
      ? adminCapabilityAvailability(capability, CAPABILITY_KEYS.operatorAudit)
      : null;
  const auditCapabilityReason =
    auditAvailability?.kind === "disabled" ? auditAvailability.reason : null;

  return (
    <>
      <PageHeader
        title="Security & Audit"
        description="当前身份与 Operator Audit Event 查询；首版不做 IAM 管理，前端禁用只是界面约束，服务端授权才是安全边界。"
        status={
          role ? (
            <StatusBadge tone="good">{role}</StatusBadge>
          ) : (
            <StatusBadge tone="unknown">无 Access Role</StatusBadge>
          )
        }
      />

      <IdentityPanel />

      <PermissionGate action="audit.view">
        {auditCapabilityReason ? (
          <ForbiddenState message={auditCapabilityReason.message} />
        ) : (
          <OperatorAuditQuerySection />
        )}
      </PermissionGate>
    </>
  );
}
