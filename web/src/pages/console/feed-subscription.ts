import type {
  AdminLastError,
  FeedSubscription,
  FeedSubscriptionUpdateBody,
  FlowControlResponse,
} from "@/api/endpoints/admin";

/**
 * Feed Subscription presentation and conflict model.
 *
 * CONTRACT TARGET ONLY: the Admin Feed Subscription / flow-control endpoints
 * do not exist in the current backend. Everything here is exercised against
 * MSW fixtures and must be reported as contract-tested, not integrated.
 *
 * Desired, Observed, Drift and Last Error are deliberately separate values;
 * a 409 stops the write and requires a manual merge from a refreshed version.
 * Never auto-retry or last-write-wins.
 */

const OBSERVED_STATE_LABELS: Record<string, string> = {
  pending: "待应用",
  applying: "应用中",
  applied: "已应用",
  rejected: "已拒绝",
  unknown: "状态未知",
};

const DRIFT_REASON_LABELS: Record<string, string> = {
  not_applied: "尚未应用",
  collector_rejected: "Collector 拒绝",
  collector_unavailable: "Collector 不可用",
  deletion_pending: "删除待确认",
};

export interface FeedSubscriptionView {
  subscriptionId: string;
  identity: {
    feed: string;
    venue: string;
    instrumentId: string;
    interval: string;
  };
  desired: {
    label: string;
    enabled: boolean;
    deleted: boolean;
    version: number;
    updatedAt: string;
    updatedBy: string;
  };
  observed: {
    label: string;
    version: number;
    state: string;
    observedAt: string;
    collectorInstanceId: string;
  };
  drift: {
    label: string;
    inSync: boolean;
    desiredVersion: number;
    observedVersion: number;
    since: string;
    reason: string | null;
    reasonLabel: string;
  };
  lastError: {
    label: string;
    code: string;
    message: string;
    at: string;
    requestId: string;
  } | null;
}

export function describeFeedSubscription(
  subscription: FeedSubscription,
): FeedSubscriptionView {
  const desiredLabel = subscription.desired.deleted
    ? "待删除"
    : subscription.desired.enabled
      ? "启用"
      : "停用";
  const observedLabel =
    OBSERVED_STATE_LABELS[subscription.observed.state] ??
    subscription.observed.state;
  const reasonLabel = subscription.drift.reason
    ? (DRIFT_REASON_LABELS[subscription.drift.reason] ??
      subscription.drift.reason)
    : "未知原因";

  return {
    subscriptionId: subscription.subscription_id,
    identity: {
      feed: subscription.feed,
      venue: subscription.venue,
      instrumentId: subscription.instrument_id,
      interval: subscription.interval,
    },
    desired: {
      label: desiredLabel,
      enabled: subscription.desired.enabled,
      deleted: subscription.desired.deleted,
      version: subscription.desired.version,
      updatedAt: subscription.desired.updated_at,
      updatedBy: subscription.desired.updated_by,
    },
    observed: {
      label: observedLabel,
      version: subscription.observed.version,
      state: subscription.observed.state,
      observedAt: subscription.observed.observed_at,
      collectorInstanceId: subscription.observed.collector_instance_id,
    },
    drift: {
      label: subscription.drift.in_sync
        ? "同步"
        : `漂移：${reasonLabel}`,
      inSync: subscription.drift.in_sync,
      desiredVersion: subscription.drift.desired_version,
      observedVersion: subscription.drift.observed_version,
      since: subscription.drift.since,
      reason: subscription.drift.reason,
      reasonLabel,
    },
    lastError: subscription.last_error
      ? {
          label: "Last Error",
          code: subscription.last_error.code,
          message: subscription.last_error.message,
          at: subscription.last_error.at,
          requestId: subscription.last_error.request_id,
        }
      : null,
  };
}

export const FEED_DELETION_HISTORY_NOTE =
  "删除仅停止后续采集；已写入的历史 Bars、Instrument 与审计数据不受影响。";

export function feedSubscriptionUpdateBody(
  subscription: FeedSubscription,
  enabled: boolean,
): FeedSubscriptionUpdateBody {
  return {
    feed: subscription.feed,
    instrument_id: subscription.instrument_id,
    interval: subscription.interval,
    enabled,
  };
}

export interface FeedConflictState {
  subscriptionId: string;
  attemptedVersion: number;
  currentVersion: number | null;
  current: FeedSubscription | null;
  reason: string | null;
  requiresManualMerge: true;
}

export interface FeedConflictErrorInput {
  status: number | null;
  code: string;
  details: Record<string, unknown>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isFeedSubscription(value: unknown): value is FeedSubscription {
  return (
    isRecord(value) &&
    typeof value.subscription_id === "string" &&
    typeof value.feed === "string" &&
    typeof value.instrument_id === "string" &&
    typeof value.interval === "string" &&
    isRecord(value.desired) &&
    isRecord(value.observed) &&
    isRecord(value.drift)
  );
}

/**
 * Converts a 409 into a manual-merge state. It deliberately returns no
 * retry instruction: the caller must refresh, show the current version and
 * wait for the operator to confirm a merged desired state.
 */
export function conflictFromError(
  subscriptionId: string,
  attemptedVersion: number,
  error: FeedConflictErrorInput,
): FeedConflictState | null {
  if (error.status !== 409) {
    return null;
  }
  const currentVersion =
    typeof error.details.current_version === "number"
      ? error.details.current_version
      : null;
  const current = isFeedSubscription(error.details.current)
    ? error.details.current
    : null;
  return {
    subscriptionId,
    attemptedVersion,
    currentVersion,
    current,
    reason:
      typeof error.details.reason === "string" ? error.details.reason : null,
    requiresManualMerge: true,
  };
}

export interface FeedMergeDraft {
  subscriptionId: string;
  baseVersion: number;
  enabled: boolean;
}

/**
 * Only a refreshed current resource can seed a manual merge. Returning null
 * means the operator must refresh first; the UI must not invent a version.
 */
export function buildManualMergeDraft(
  conflict: FeedConflictState,
): FeedMergeDraft | null {
  if (!conflict.current) {
    return null;
  }
  return {
    subscriptionId: conflict.subscriptionId,
    baseVersion: conflict.current.desired.version,
    enabled: conflict.current.desired.enabled,
  };
}

export const API_RATE_LIMIT_FIELDS = [
  "enabled",
  "requests_per_second",
  "burst",
  "max_ws_sessions_per_ip",
] as const;

export const WS_SLOW_CONSUMER_FIELDS = [
  "enabled",
  "queue_size",
  "lag_notice_interval_ms",
] as const;

export type FlowControlResourceKey = "api_rate_limit" | "ws_slow_consumer";

export function isFlowControlResource(
  value: string,
): value is FlowControlResourceKey {
  return value === "api_rate_limit" || value === "ws_slow_consumer";
}

export interface FlowControlResourceView {
  key: FlowControlResourceKey;
  title: string;
  version: number;
  applyMode: "hot" | "restart_required";
  applyNote: string;
  observedVersion: number;
  inSync: boolean;
  lastError: AdminLastError | null;
}

export interface FlowControlView {
  apiRateLimit: FlowControlResourceView;
  wsSlowConsumer: FlowControlResourceView;
}

function applyNote(mode: "hot" | "restart_required"): string {
  return mode === "restart_required"
    ? "需要重启 Service Instance 后生效"
    : "热应用，立即生效";
}

export function describeFlowControl(
  flow: FlowControlResponse,
): FlowControlView {
  return {
    apiRateLimit: {
      key: "api_rate_limit",
      title: "API Rate Limit",
      version: flow.api_rate_limit.version,
      applyMode: flow.api_rate_limit.apply_mode,
      applyNote: applyNote(flow.api_rate_limit.apply_mode),
      observedVersion: flow.api_rate_limit.observed.version,
      inSync: flow.api_rate_limit.drift.in_sync,
      lastError: flow.api_rate_limit.last_error,
    },
    wsSlowConsumer: {
      key: "ws_slow_consumer",
      title: "WebSocket Slow Consumer",
      version: flow.ws_slow_consumer.version,
      applyMode: flow.ws_slow_consumer.apply_mode,
      applyNote: applyNote(flow.ws_slow_consumer.apply_mode),
      observedVersion: flow.ws_slow_consumer.observed.version,
      inSync: flow.ws_slow_consumer.drift.in_sync,
      lastError: flow.ws_slow_consumer.last_error,
    },
  };
}
