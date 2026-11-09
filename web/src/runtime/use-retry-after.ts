import { useEffect, useState, useSyncExternalStore } from "react";
import type { DisabledReason } from "@/runtime/permissions";
import {
  getRetryAfterAt,
  retryAfterSecondsRemaining,
  subscribeRetryAfter,
} from "@/runtime/retry-after";

export interface RetryAfterState {
  blocked: boolean;
  secondsRemaining: number;
  reason: DisabledReason | null;
}

const TICK_MS = 1000;

/**
 * Ticking view of the shell-level Retry-After gate. Every write surface can
 * disable its controls with the same visible reason and re-enable them exactly
 * when the server-supplied delay expires.
 */
export function useRetryAfter(): RetryAfterState {
  const retryAt = useSyncExternalStore(
    subscribeRetryAfter,
    getRetryAfterAt,
    () => null,
  );
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    if (retryAt === null) {
      return;
    }
    setNow(Date.now());
    const id = window.setInterval(() => {
      setNow(Date.now());
    }, TICK_MS);
    return () => window.clearInterval(id);
  }, [retryAt]);

  const secondsRemaining = retryAfterSecondsRemaining(now);
  const blocked = secondsRemaining > 0;
  return {
    blocked,
    secondsRemaining,
    reason: blocked
      ? {
          code: "rate-limited",
          message: `限流中，请在 ${secondsRemaining} 秒后重试`,
        }
      : null,
  };
}
