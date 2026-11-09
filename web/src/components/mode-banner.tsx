import { useLocation } from "react-router-dom";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { StatusBadge } from "@/components/status-badge";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

function formatUtcTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return `${date.toISOString().replace("T", " ").slice(0, 19)} UTC`;
}

/**
 * The runtime-mode banner is the only place the console states which contract
 * it is talking to. It never offers a role switcher or a fake login.
 */
export function ModeBanner() {
  const {
    capability,
    isLoading,
    session,
    sessionLoading,
    role,
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
    const loginHref = `/api/v1/admin/auth/login?return_to=${encodeURIComponent(returnTo)}`;
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
          ) : (
            <span className="font-medium">未登录</span>
          )}
          {!sessionLoading && session?.user ? (
            <span className="text-muted-foreground">
              {session.user.displayName}
            </span>
          ) : null}
          {capability.compatible === false ? (
            <StatusBadge tone="critical">Admin API 版本不兼容</StatusBadge>
          ) : null}
          {!sessionLoading && !role ? (
            <a className="underline underline-offset-2" href={loginHref}>
              登录
            </a>
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
