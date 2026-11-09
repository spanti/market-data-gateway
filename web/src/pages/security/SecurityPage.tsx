import { ModulePlaceholder } from "@/components/module-placeholder";
import { PageHeader } from "@/components/page-header";
import { PermissionGate } from "@/components/permission-gate";
import { StatusBadge } from "@/components/status-badge";
import { Panel, PanelDescription, PanelTitle } from "@/components/ui/panel";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

export function SecurityPage() {
  const { capability, session, role } = useRuntimeMode();

  const identity =
    capability?.mode === "current-contract"
      ? { name: "匿名只读", detail: "Current Contract 不分配 Access Role" }
      : capability?.mode === "admin-api" && session?.authenticated
        ? {
            name: session.user?.displayName ?? "已认证用户",
            detail: session.user?.email ?? "会话已建立",
          }
        : capability?.mode === "admin-api"
          ? { name: "未登录", detail: "等待 OIDC 登录" }
          : { name: "能力未知", detail: "无法确认当前身份与权限" };

  return (
    <>
      <PageHeader
        title="Security & Audit"
        description="当前身份与 Operator Audit Event 查询；首版不做 IAM 管理，前端不构成安全边界。"
        status={
          role ? (
            <StatusBadge tone="good">{role}</StatusBadge>
          ) : (
            <StatusBadge tone="unknown">无 Access Role</StatusBadge>
          )
        }
      />

      <Panel className="mt-5 p-5" aria-label="当前身份">
        <PanelTitle>当前身份</PanelTitle>
        <PanelDescription>
          Access Role 由服务端在会话中返回，前端没有角色切换器。
        </PanelDescription>
        <dl className="mt-3 grid grid-cols-1 gap-3 text-xs sm:grid-cols-3">
          <div>
            <dt className="text-faint-foreground">身份</dt>
            <dd className="mt-1 font-medium">{identity.name}</dd>
          </div>
          <div>
            <dt className="text-faint-foreground">Access Role</dt>
            <dd className="mt-1">{role ?? "无"}</dd>
          </div>
          <div>
            <dt className="text-faint-foreground">说明</dt>
            <dd className="mt-1 text-muted-foreground">{identity.detail}</dd>
          </div>
        </dl>
      </Panel>

      <div className="mt-6">
        <PermissionGate action="audit.view">
          <ModulePlaceholder
            title="Operator Audit Event 查询尚未接入"
            description="时间、操作者、Access Role、动作、对象、结果、Request ID 筛选与脱敏 Diff 时间线将在后续工单接入；这是契约目标，未进行真实后端联调。"
          />
        </PermissionGate>
      </div>
    </>
  );
}
