import { useLocation } from "react-router-dom";
import { DisabledAction } from "@/components/disabled-reason";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { StatusBadge } from "@/components/status-badge";
import { formatUtcTimestamp } from "@/lib/format";
import { buildLoginUrl } from "@/runtime/auth";
import { loginDisabledReason } from "@/runtime/login-availability";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import type { SessionIssueReason } from "@/runtime/session-expiry";

function sessionIssueDetail(reason: SessionIssueReason): string {
  switch (reason) {
    case "idle_timeout":
      return "原因：30 分钟无操作，空闲会话已过期";
    case "absolute_timeout":
      return "原因：已达 8 小时最长会话时限";
    case "idp_error":
      return "原因：OIDC 登录失败";
    case "missing":
      return "原因：没有有效的服务端会话";
    default:
      return "原因：会话无效或已被服务端拒绝";
  }
}

/**
 * The runtime-mode banner is the only place the console states which contract
 * it is talking to. It never offers a role switcher or a fake login: login is
 * a top-level browser navigation to the backend OIDC entry, logout is a
 * CSRF-protected POST.
 */
export function ModeBanner() {
  const {
    capability,
    isLoading,
    session,
    sessionLoading,
    role,
    sessionIssue,
    authPending,
    logout,
    refreshCapabilities,
  } = useRuntimeMode();
  const location = useLocation();

  if (isLoading || !capability) {
    return (
      <div
        role="status"
        aria-live="polite"
        className="border-b border-border bg-card"
      >
        <div className="mx-auto flex max-w-[1600px] flex-wrap items-center gap-3 px-5 py-2 text-xs text-muted-foreground lg:px-8">
          正在发现能力…
        </div>
      </div>
    );
  }

  if (capability.mode === "current-contract") {
    return (
      <div
        data-testid="runtime-mode-banner"
        data-mode="current-contract"
        role="status"
        className="border-b border-border bg-card"
      >
        <div className="mx-auto flex max-w-[1600px] flex-wrap items-center gap-3 px-5 py-2 text-xs lg:px-8">
          <Badge variant="outline">Current Contract</Badge>
          <span className="font-medium">匿名只读</span>
          <span className="text-muted-foreground">
            当前部署未提供 Admin API；所有管理写操作已禁用并注明原因。
          </span>
        </div>
      </div>
    );
  }

  if (capability.mode === "admin-api") {
    const returnTo = `${location.pathname}${location.search}`;
    const loginHref = buildLoginUrl(returnTo);
    const loginReason = loginDisabledReason(capability);
    return (
      <div
        data-testid="runtime-mode-banner"
        data-mode="admin-api"
        role="status"
        className="border-b border-border bg-card"
      >
        <div className="mx-auto flex max-w-[1600px] flex-wrap items-center gap-3 px-5 py-2 text-xs lg:px-8">
          <Badge variant="soft">Admin API</Badge>
          {sessionLoading ? (
            <span className="font-medium">正在确认会话…</span>
          ) : role ? (
            <span className="font-medium">{role}</span>
          ) : sessionIssue ? (
            <StatusBadge tone="warning">会话已失效</StatusBadge>
          ) : (
            <span className="font-medium">未登录</span>
          )}
          {!sessionLoading && session?.user ? (
            <span className="text-muted-foreground">
              {session.user.displayName}
            </span>
          ) : null}
          {!sessionLoading && !role && sessionIssue ? (
            <span className="text-muted-foreground">
              {sessionIssueDetail(sessionIssue)}
            </span>
          ) : null}
          {capability.compatible === false ? (
            <StatusBadge tone="critical">Admin API 版本不兼容</StatusBadge>
          ) : null}
          {!sessionLoading && !role ? (
            loginReason ? (
              <DisabledAction label="登录" reason={loginReason} />
            ) : (
              <a className="underline underline-offset-2" href={loginHref}>
                登录
              </a>
            )
          ) : null}
          {!sessionLoading && role ? (
            <Button
              size="sm"
              variant="outline"
              disabled={authPending}
              onClick={() => {
                void logout().catch(() => {});
              }}
            >
              {authPending ? "注销中…" : "注销"}
            </Button>
          ) : null}
          <span className="text-muted-foreground">
            Access Role 由服务端分配；前端不构成安全边界。
          </span>
        </div>
      </div>
    );
  }

  return (
    <div
      data-testid="runtime-mode-banner"
      data-mode="capabilities-unknown"
      role="status"
      className="border-b border-border bg-card"
    >
      <div className="mx-auto flex max-w-[1600px] flex-wrap items-center gap-3 px-5 py-2 text-xs lg:px-8">
        <StatusBadge tone="warning">能力未知</StatusBadge>
        <span>能力发现失败，所有写操作保持禁用，不降级为只读页面。</span>
        {capability.lastSuccessAt ? (
          <span className="text-muted-foreground">
            最后成功更新时间：{formatUtcTimestamp(capability.lastSuccessAt)}
          </span>
        ) : (
          <span className="text-muted-foreground">没有可用的缓存数据。</span>
        )}
        <Button size="sm" variant="outline" onClick={refreshCapabilities}>
          重试
        </Button>
      </div>
    </div>
  );
}
