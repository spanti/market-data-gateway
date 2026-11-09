import { StatusBadge } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

/**
 * Non-blocking warning five minutes before the earlier of the 30-minute idle
 * and 8-hour absolute limits. "继续" is the only genuine user activity that
 * calls `POST /auth/refresh`; background polling never extends a session.
 */
export function SessionExpiryNotice() {
  const {
    session,
    sessionExpiry,
    authPending,
    refreshSession,
    logout,
  } = useRuntimeMode();

  if (!session?.authenticated || sessionExpiry.state !== "warning") {
    return null;
  }

  const minutes = Math.max(
    1,
    Math.ceil((sessionExpiry.remainingSeconds ?? 0) / 60),
  );
  const absoluteBound = sessionExpiry.bound === "absolute";
  const boundCopy = absoluteBound
    ? "8 小时最长会话限制，不能延长"
    : "30 分钟空闲限制";

  return (
    <div
      data-testid="session-expiry-notice"
      role="status"
      aria-live="polite"
      className="border-b border-border bg-surface-sunken"
    >
      <div className="mx-auto flex max-w-[1600px] flex-wrap items-center gap-3 px-5 py-2 text-xs lg:px-8">
        <StatusBadge tone="warning">会话即将过期</StatusBadge>
        <span className="font-medium">
          会话将在 {minutes} 分钟后过期（{boundCopy}）。
        </span>
        <span className="text-muted-foreground">
          {absoluteBound
            ? "绝对会话上限不能被“继续”延长，请保存工作后重新登录。"
            : "只有点击“继续”才会延长空闲会话；后台轮询不算用户活动。"}
        </span>
        <div className="ml-auto flex items-center gap-2">
          {absoluteBound ? null : (
            <Button
              size="sm"
              disabled={authPending}
              onClick={() => {
                void refreshSession().catch(() => {
                  // 401/403 are surfaced by the global HTTP notice; a 401 also
                  // clears the session and stops writes in the provider.
                });
              }}
            >
              {authPending ? "续期中…" : "继续"}
            </Button>
          )}
          <Button
            size="sm"
            variant="outline"
            disabled={authPending}
            onClick={() => {
              void logout().catch(() => {});
            }}
          >
            退出
          </Button>
        </div>
      </div>
    </div>
  );
}
