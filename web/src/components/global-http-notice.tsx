import { X } from "lucide-react";
import { useEffect, useState } from "react";
import { classifyHttpError } from "@/api/http/errors";
import type { NormalizedHttpError } from "@/api/http/types";
import { Button } from "@/components/ui/button";
import { subscribeHttpErrors } from "@/runtime/http-error-bus";
import { useRetryAfter } from "@/runtime/use-retry-after";

function directiveMessage(error: NormalizedHttpError): string {
  const directive = classifyHttpError(error);
  switch (directive.action) {
    case "login-required":
      return "会话已失效或未登录，写操作已停止。请回到登录入口。";
    case "permission-denied":
      return directive.requiredRole
        ? `缺少权限：需要 ${directive.requiredRole} 角色。`
        : "缺少执行该操作的权限。";
    case "conflict":
      return "数据已被他人修改（409 冲突），请刷新后手动合并，不会自动覆盖。";
    case "retry-after":
      return directive.retryAfterS !== undefined
        ? `请求过于频繁，请在 ${directive.retryAfterS} 秒后重试。`
        : "请求过于频繁，请稍后重试。";
    case "show-error":
      return "请求失败，请查看具体错误与 Request ID。";
  }
}

/**
 * Global 401/403/409/429 handling. It never retries a write automatically and
 * always surfaces the Request ID for support correlation.
 */
export function GlobalHttpNotice() {
  const [error, setError] = useState<NormalizedHttpError | null>(null);
  const retryAfter = useRetryAfter();

  useEffect(() => subscribeHttpErrors(setError), []);

  if (!error) {
    return null;
  }

  // Keep the 429 notice counting down with the same shell-level gate that
  // disables write controls.
  const message =
    error.status === 429 && retryAfter.blocked
      ? `请求过于频繁，请在 ${retryAfter.secondsRemaining} 秒后重试。`
      : directiveMessage(error);

  return (
    <div
      data-testid="global-http-notice"
      role="alert"
      className="border-b border-border bg-surface-sunken"
    >
      <div className="mx-auto flex max-w-[1600px] flex-wrap items-center gap-3 px-5 py-2 text-xs lg:px-8">
        <span className="font-medium">{message}</span>
        {error.requestId ? (
          <span className="font-mono text-[11px] text-faint-foreground">
            Request ID: {error.requestId}
          </span>
        ) : null}
        <Button
          size="sm"
          variant="ghost"
          className="ml-auto"
          onClick={() => setError(null)}
          aria-label="关闭提示"
        >
          <X aria-hidden="true" />
        </Button>
      </div>
    </div>
  );
}
