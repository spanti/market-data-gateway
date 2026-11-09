import { classifyHttpError } from "@/api/http/errors";
import type { NormalizedHttpError } from "@/api/http/types";
import type { DisabledReason } from "@/runtime/permissions";
import { subscribeHttpErrors } from "@/runtime/http-error-bus";

/**
 * A shell-level 429 Retry-After gate. The HTTP error bus feeds it; the main
 * write surfaces consult it before issuing a mutation. The server remains the
 * only enforcement boundary — this avoids pointless requests and explains why
 * a control is temporarily disabled.
 */

type Listener = () => void;

let retryAtMs: number | null = null;
const listeners = new Set<Listener>();
let busSubscribed = false;

function notify(): void {
  for (const listener of listeners) {
    listener();
  }
}

function ensureBusSubscription(): void {
  if (busSubscribed) {
    return;
  }
  busSubscribed = true;
  subscribeHttpErrors((error) => {
    applyHttpErrorRetryAfter(error);
  });
}

/** Applies a normalized HTTP error to the Retry-After gate. */
export function applyHttpErrorRetryAfter(
  error: NormalizedHttpError,
  now: number = Date.now(),
): void {
  ensureBusSubscription();
  if (error.status !== 429) {
    return;
  }
  const directive = classifyHttpError(error);
  if (
    directive.action !== "retry-after" ||
    directive.retryAfterS === undefined ||
    directive.retryAfterS <= 0
  ) {
    return;
  }
  const candidate = now + directive.retryAfterS * 1000;
  // Never shorten a longer active block with a later, smaller Retry-After.
  retryAtMs = retryAtMs === null ? candidate : Math.max(retryAtMs, candidate);
  notify();
}

export function getRetryAfterAt(): number | null {
  return retryAtMs;
}

export function retryAfterSecondsRemaining(now: number = Date.now()): number {
  if (retryAtMs === null) {
    return 0;
  }
  return Math.max(0, Math.ceil((retryAtMs - now) / 1000));
}

export function retryAfterBlocked(now: number = Date.now()): boolean {
  return retryAfterSecondsRemaining(now) > 0;
}

export function retryAfterDisabledReason(
  now: number = Date.now(),
): DisabledReason | null {
  const seconds = retryAfterSecondsRemaining(now);
  if (seconds <= 0) {
    return null;
  }
  return {
    code: "rate-limited",
    message: `限流中，请在 ${seconds} 秒后重试`,
  };
}

export function subscribeRetryAfter(listener: Listener): () => void {
  ensureBusSubscription();
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** Test helper: clears the active block. */
export function resetRetryAfter(): void {
  retryAtMs = null;
  notify();
}

ensureBusSubscription();
