import { ForbiddenState, PermissionGate } from "@/components/permission-gate";
import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { AuditQuerySection } from "@/pages/security/AuditQuerySection";
import { IdentityPanel } from "@/pages/security/IdentityPanel";
import { capabilityDisabledReason } from "@/runtime/permissions";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

export function SecurityPage() {
  const { capability, role } = useRuntimeMode();

  const auditCapabilityReason = (() => {
    if (!capability || capability.mode !== "admin-api") {
      return null;
    }
    if (capability.compatible === false) {
      return {
        code: "backend-unsupported" as const,
        message: "Admin API 版本不兼容",
      };
    }
    if (!capability.document) {
      return null;
    }
    return capabilityDisabledReason(
      capability.document.capabilities.operator_audit,
    );
  })();

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
          <AuditQuerySection />
        )}
      </PermissionGate>
    </>
  );
}
