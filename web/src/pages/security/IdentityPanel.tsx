import { UserRound } from "lucide-react";
import { useLocation } from "react-router-dom";
import { DisabledAction } from "@/components/disabled-reason";
import { HelpTrigger } from "@/components/help-trigger";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatUtcTimestamp } from "@/lib/format";
import { buildLoginUrl } from "@/runtime/auth";
import { loginDisabledReason } from "@/runtime/login-availability";
import {
  capabilityModeKind,
  capabilityModePresentation,
} from "@/runtime/mode-presentation";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import type { DisabledReason } from "@/runtime/permissions";
import { IconTile } from "@/components/icon-tile";

interface IdentityView {
  name: string;
  subject: string;
  roleLabel: string;
  authMode: string;
  issuedAt: string | null;
  idleExpiresAt: string | null;
  absoluteExpiresAt: string | null;
  badge: { tone: StatusTone; label: string };
}

function identityView(
  capability: ReturnType<typeof useRuntimeMode>["capability"],
  session: ReturnType<typeof useRuntimeMode>["session"],
  role: ReturnType<typeof useRuntimeMode>["role"],
): IdentityView {
  const modeKind = capabilityModeKind(capability);
  const modeBadge = capabilityModePresentation(capability, {
    discovering: { tone: "warning", label: "能力未知" },
    "capabilities-unknown": { tone: "warning", label: "能力未知" },
    "current-contract": { tone: "unknown", label: "匿名只读" },
  });
  if (modeKind === "discovering" || modeKind === "capabilities-unknown") {
    return {
      name: "能力未知",
      subject: "—",
      roleLabel: "无",
      authMode: "未知（能力发现失败）",
      issuedAt: null,
      idleExpiresAt: null,
      absoluteExpiresAt: null,
      badge: { tone: modeBadge.tone, label: modeBadge.label },
    };
  }
  if (modeKind === "current-contract") {
    return {
      name: "匿名只读",
      subject: "—",
      roleLabel: "不分配（Current Contract）",
      authMode: "Current Contract（无 OIDC 会话）",
      issuedAt: null,
      idleExpiresAt: null,
      absoluteExpiresAt: null,
      badge: { tone: modeBadge.tone, label: modeBadge.label },
    };
  }

  if (session?.authenticated && role) {
    return {
      name: session.user?.displayName ?? "已认证用户",
      subject: session.user?.subject ?? "—",
      roleLabel: role,
      authMode: "OIDC + 服务端会话（30 分钟空闲 / 8 小时绝对）",
      issuedAt: session.session?.issued_at ?? null,
      idleExpiresAt: session.session?.idle_expires_at ?? null,
      absoluteExpiresAt: session.session?.absolute_expires_at ?? null,
      badge: { tone: "good", label: role },
    };
  }

  return {
    name: "未登录",
    subject: "—",
    roleLabel: "无",
    authMode: "OIDC（Admin API）",
    issuedAt: null,
    idleExpiresAt: null,
    absoluteExpiresAt: null,
    badge: { tone: "unknown", label: "未登录" },
  };
}

const LOGIN_DISABLED_REASON: Record<string, DisabledReason> = {
  anonymous: {
    code: "requires-admin-api",
    message: "需要 Admin API（OIDC 登录）",
  },
  unknown: {
    code: "capabilities-unknown",
    message: "能力未知，暂不可用",
  },
};

/**
 * Read-only identity: operator, server-assigned Access Role, auth mode and
 * session times. There is deliberately no role selector; identity actions
 * either navigate to the backend OIDC login or explain why they are disabled.
 */
export function IdentityPanel() {
  const { capability, session, role, subject } = useRuntimeMode();
  const location = useLocation();
  const view = identityView(capability, session, role);

  const loginEntry = (() => {
    if (subject.kind === "role") {
      return (
        <span className="text-xs text-muted-foreground">
          Access Role 与会话时间来自服务端；前端不显示或保存任何 Token。
        </span>
      );
    }
    if (subject.kind === "unauthenticated") {
      const loginReason = loginDisabledReason(capability);
      if (loginReason) {
        return <DisabledAction label="登录 OIDC" reason={loginReason} />;
      }
      return (
        <a
          className="text-xs font-medium underline underline-offset-2"
          href={buildLoginUrl(`${location.pathname}${location.search}`)}
        >
          登录 OIDC
        </a>
      );
    }
    return (
      <DisabledAction
        label="登录 OIDC"
        reason={
          LOGIN_DISABLED_REASON[subject.kind] ?? {
            code: "requires-admin-api",
            message: "需要 Admin API",
          }
        }
      />
    );
  })();

  return (
    <Card role="region" aria-label="当前身份" className="mt-4">
      <CardHeader className="p-5 pb-0">
        <div className="flex min-w-0 items-start gap-2.5">
          <IconTile icon={UserRound} />
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-1.5">
              <CardTitle>当前身份</CardTitle>
              <HelpTrigger
                label="身份与 Access Role 说明"
                content="身份、Access Role 和会话时间均由服务端返回。前端没有角色切换器，服务端授权才是安全边界。Access Role 是控制台使用者权限，与 Service Role 进程职责无关。"
                side="bottom"
              />
            </div>
          </div>
        </div>
        <StatusBadge tone={view.badge.tone}>{view.badge.label}</StatusBadge>
      </CardHeader>

      <CardContent className="p-5 pt-4">
        <dl className="grid grid-cols-1 gap-x-6 gap-y-3 rounded-lg bg-surface-sunken p-3 text-xs sm:grid-cols-2 lg:grid-cols-3">
          <div>
            <dt className="text-faint-foreground">操作者</dt>
            <dd className="mt-1 font-medium">{view.name}</dd>
          </div>
          <div>
            <dt className="text-faint-foreground">Subject</dt>
            <dd className="mt-1 font-mono break-all">{view.subject}</dd>
          </div>
          <div>
            <dt className="text-faint-foreground">Access Role</dt>
            <dd className="mt-1">{view.roleLabel}</dd>
          </div>
          <div>
            <dt className="text-faint-foreground">认证模式</dt>
            <dd className="mt-1">{view.authMode}</dd>
          </div>
          <div>
            <dt className="text-faint-foreground">会话签发</dt>
            <dd className="mt-1 tabular-nums">
              {formatUtcTimestamp(view.issuedAt)}
            </dd>
          </div>
          <div>
            <dt className="text-faint-foreground">空闲到期</dt>
            <dd className="mt-1 tabular-nums">
              {formatUtcTimestamp(view.idleExpiresAt)}
            </dd>
          </div>
          <div>
            <dt className="text-faint-foreground">绝对到期</dt>
            <dd className="mt-1 tabular-nums">
              {formatUtcTimestamp(view.absoluteExpiresAt)}
            </dd>
          </div>
        </dl>

        <div className="mt-4 flex flex-wrap items-center gap-3">{loginEntry}</div>
      </CardContent>
    </Card>
  );
}
