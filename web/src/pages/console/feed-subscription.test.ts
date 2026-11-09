import { describe, expect, it } from "vitest";
import type {
  FeedSubscription,
  FlowControlResponse,
} from "@/api/endpoints/admin";
import {
  API_RATE_LIMIT_FIELDS,
  buildManualMergeDraft,
  conflictFromError,
  describeFeedSubscription,
  describeFlowControl,
  FEED_DELETION_HISTORY_NOTE,
  feedSubscriptionUpdateBody,
  isFlowControlResource,
  WS_SLOW_CONSUMER_FIELDS,
} from "@/pages/console/feed-subscription";

const subscription: FeedSubscription = {
  subscription_id: "fsub_1",
  feed: "binance_spot",
  venue: "binance",
  instrument_id: "binance:BTC/USDT",
  data_type: "bar",
  interval: "1m",
  desired: {
    enabled: true,
    deleted: false,
    version: 3,
    updated_at: "2026-11-09T12:00:00.000000000Z",
    updated_by: "oidc-subject-123",
  },
  observed: {
    enabled: true,
    deleted: false,
    version: 2,
    state: "applied",
    observed_at: "2026-11-09T12:00:02.000000000Z",
    collector_instance_id: "collector",
  },
  drift: {
    in_sync: false,
    desired_version: 3,
    observed_version: 2,
    since: "2026-11-09T12:00:00.000000000Z",
    reason: "not_applied",
  },
  last_error: {
    code: "collector_unavailable",
    message: "collector did not confirm",
    at: "2026-11-09T12:00:03.000000000Z",
    request_id: "req_9",
  },
  created_at: "2026-11-09T11:00:00.000000000Z",
  created_by: "oidc-subject-123",
  updated_at: "2026-11-09T12:00:00.000000000Z",
};

const flowControl: FlowControlResponse = {
  api_rate_limit: {
    enabled: true,
    requests_per_second: 100,
    burst: 200,
    max_ws_sessions_per_ip: 16,
    version: 2,
    apply_mode: "hot",
    observed: {
      version: 2,
      state: "applied",
      observed_at: "2026-11-09T12:00:00.000000000Z",
    },
    drift: {
      in_sync: true,
      desired_version: 2,
      observed_version: 2,
      reason: null,
    },
    last_error: null,
  },
  ws_slow_consumer: {
    enabled: true,
    queue_size: 1000,
    lag_notice_interval_ms: 1000,
    version: 5,
    apply_mode: "restart_required",
    observed: {
      version: 4,
      state: "applied",
      observed_at: "2026-11-09T11:00:00.000000000Z",
    },
    drift: {
      in_sync: false,
      desired_version: 5,
      observed_version: 4,
      reason: "not_applied",
    },
    last_error: null,
  },
};

describe("Feed Subscription presentation", () => {
  it("keeps Desired, Observed, Drift and Last Error as separate values", () => {
    const view = describeFeedSubscription(subscription);

    expect(view.desired).toMatchObject({ label: "启用", version: 3 });
    expect(view.observed).toMatchObject({
      label: "已应用",
      version: 2,
      collectorInstanceId: "collector",
    });
    expect(view.drift).toMatchObject({
      label: expect.stringContaining("漂移"),
      inSync: false,
      desiredVersion: 3,
      observedVersion: 2,
      reasonLabel: "尚未应用",
    });
    expect(view.lastError).toMatchObject({
      code: "collector_unavailable",
      requestId: "req_9",
    });

    // The four concepts must never collapse into one combined status value.
    const values = [view.desired.label, view.observed.label, view.drift.label];
    expect(new Set(values).size).toBeGreaterThan(1);
  });

  it("marks a tombstoned desired state as pending deletion", () => {
    const view = describeFeedSubscription({
      ...subscription,
      desired: { ...subscription.desired, deleted: true, enabled: false },
    });
    expect(view.desired.label).toBe("待删除");
  });

  it("states that deletion only stops future collection", () => {
    expect(FEED_DELETION_HISTORY_NOTE).toContain("仅停止后续采集");
    expect(FEED_DELETION_HISTORY_NOTE).toContain("历史 Bars");
  });

  it("builds an update body from the current identity and desired enabled flag", () => {
    expect(feedSubscriptionUpdateBody(subscription, false)).toEqual({
      feed: "binance_spot",
      instrument_id: "binance:BTC/USDT",
      interval: "1m",
      enabled: false,
    });
  });
});

describe("Feed Subscription 409 conflicts", () => {
  it("returns a manual-merge conflict for 409 and never an automatic retry plan", () => {
    const conflict = conflictFromError("fsub_1", 3, {
      status: 409,
      code: "conflict",
      details: {
        current_version: 4,
        current: { ...subscription, desired: { ...subscription.desired, version: 4, enabled: false } },
      },
    });

    expect(conflict).toMatchObject({
      subscriptionId: "fsub_1",
      attemptedVersion: 3,
      currentVersion: 4,
      requiresManualMerge: true,
    });
    expect(conflict?.current?.desired.version).toBe(4);
  });

  it("ignores non-409 errors", () => {
    expect(
      conflictFromError("fsub_1", 3, {
        status: 400,
        code: "invalid_parameter",
        details: {},
      }),
    ).toBeNull();
  });

  it("builds a merge draft only from the refreshed current version", () => {
    const conflict = conflictFromError("fsub_1", 3, {
      status: 409,
      code: "conflict",
      details: {
        current_version: 4,
        current: { ...subscription, desired: { ...subscription.desired, version: 4, enabled: false } },
      },
    });

    expect(conflict).not.toBeNull();
    if (!conflict) {
      throw new Error("expected a 409 conflict state");
    }

    expect(buildManualMergeDraft(conflict)).toEqual({
      subscriptionId: "fsub_1",
      baseVersion: 4,
      enabled: false,
    });

    expect(
      buildManualMergeDraft({
        subscriptionId: "fsub_1",
        attemptedVersion: 3,
        currentVersion: null,
        current: null,
        reason: null,
        requiresManualMerge: true,
      }),
    ).toBeNull();
  });
});

describe("split flow control", () => {
  it("presents API Rate Limit and WebSocket Slow Consumer as separate versioned resources", () => {
    const view = describeFlowControl(flowControl);

    expect(view.apiRateLimit).toMatchObject({
      key: "api_rate_limit",
      version: 2,
      applyMode: "hot",
    });
    expect(view.wsSlowConsumer).toMatchObject({
      key: "ws_slow_consumer",
      version: 5,
      applyMode: "restart_required",
    });
    expect(view.apiRateLimit.version).not.toBe(view.wsSlowConsumer.version);
    expect(view.wsSlowConsumer.applyNote).toContain("重启");
    expect(view.apiRateLimit.applyNote).not.toContain("重启");
  });

  it("exposes only the two documented field sets with no generic threshold or backpressure controls", () => {
    expect([...API_RATE_LIMIT_FIELDS]).toEqual([
      "enabled",
      "requests_per_second",
      "burst",
      "max_ws_sessions_per_ip",
    ]);
    expect([...WS_SLOW_CONSUMER_FIELDS]).toEqual([
      "enabled",
      "queue_size",
      "lag_notice_interval_ms",
    ]);

    const combined = [
      ...API_RATE_LIMIT_FIELDS,
      ...WS_SLOW_CONSUMER_FIELDS,
    ].join(",");
    expect(combined).not.toContain("threshold");
    expect(combined).not.toContain("venue");
    expect(combined).not.toContain("collector_queue");
    expect(combined).not.toContain("storage");
    expect(combined).not.toContain("backpressure");
  });

  it("maps each sub-resource to its own endpoint", () => {
    expect(isFlowControlResource("api_rate_limit")).toBe(true);
    expect(isFlowControlResource("ws_slow_consumer")).toBe(true);
    expect(isFlowControlResource("threshold")).toBe(false);
  });
});
