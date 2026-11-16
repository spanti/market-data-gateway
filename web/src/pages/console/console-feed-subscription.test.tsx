import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import { renderApp } from "@/test/render-app";
import { server } from "@/test/server";

const subscriptionFixture = {
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
    message: "collector · unavailable：timeout",
    at: "2026-11-09T12:00:03.000000000Z",
    request_id: "req_9",
  },
  created_at: "2026-11-09T11:00:00.000000000Z",
  created_by: "oidc-subject-123",
  updated_at: "2026-11-09T12:00:00.000000000Z",
};

const flowControlFixture = {
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

const capabilityDocument = {
  schema_version: 1,
  server_time: "2026-11-09T12:00:00.000000000Z",
  version: { admin_api: "1.0.0", contract: 1, supported_contracts: [1] },
  auth: {
    required: true,
    mode: "oidc",
    state: "available",
    roles: ["SuperAdmin", "Operator", "Auditor"],
  },
  adapters: [
    { name: "collector_control", kind: "feed_subscription", state: "available" },
  ],
  capabilities: {
    feed_subscription: { state: "enabled" },
    feed_flow_control: {
      state: "enabled",
      api_rate_limit: { hot_apply: true },
      ws_slow_consumer: { hot_apply: false },
    },
  },
  limits: {},
};

function adminHandlers(role: "SuperAdmin" | "Operator" | "Auditor") {
  return [
    http.get("*/api/v1/admin/capabilities", () =>
      HttpResponse.json(capabilityDocument),
    ),
    http.get("*/api/v1/admin/auth/session", () =>
      HttpResponse.json({
        authenticated: true,
        server_time: "2026-11-09T12:00:00.000000000Z",
        user: {
          subject: "oidc-subject-123",
          display_name: "Ada Operator",
          email: "ada@example.com",
        },
        access_role: role,
        session: null,
        csrf: { header_name: "X-CSRF-Token", token: "csrf-token" },
      }),
    ),
  ];
}

function feedHandlers(overrides: { desiredVersion?: () => number } = {}) {
  return [
    http.get("*/api/v1/admin/feed-subscriptions", () =>
      HttpResponse.json({
        items: [
          {
            ...subscriptionFixture,
            desired: {
              ...subscriptionFixture.desired,
              version: overrides.desiredVersion?.() ?? 3,
            },
          },
        ],
        page: { next_cursor: null, has_more: false },
      }),
    ),
    http.get("*/api/v1/admin/flow-control", () =>
      HttpResponse.json(flowControlFixture),
    ),
  ];
}

async function openFeedTab() {
  const user = userEvent.setup();
  renderApp({ initialEntries: ["/console"] });
  await user.click(await screen.findByRole("tab", { name: "Feed Subscription" }));
  return user;
}

describe("Feed Subscription — Current Contract", () => {
  it("disables the whole area with a visible Admin API reason and fabricates no table", async () => {
    await openFeedTab();

    expect(
      await screen.findByRole("region", { name: "Feed Subscription" }),
    ).toBeInTheDocument();
    expect(
      (await screen.findAllByText("需要 Admin API")).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getByRole("button", { name: "Feed Subscription 说明" }),
    ).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(screen.queryByText("fsub_1")).not.toBeInTheDocument();
  });
});

describe("Feed Subscription — Admin API contract target", () => {
  it("shows Desired, Observed, Drift and Last Error as separate groups", async () => {
    server.use(...adminHandlers("Operator"), ...feedHandlers());
    await openFeedTab();

    const row = await screen.findByTestId("feed-subscription-fsub_1");
    const desired = within(row).getByRole("group", { name: "Desired" });
    expect(within(desired).getByText("启用")).toBeInTheDocument();
    expect(within(desired).getByText("v3")).toBeInTheDocument();

    const observed = within(row).getByRole("group", { name: "Observed" });
    expect(within(observed).getByText("已应用")).toBeInTheDocument();
    expect(within(observed).getByText("v2")).toBeInTheDocument();

    const drift = within(row).getByRole("group", { name: "Drift" });
    expect(within(drift).getByText("存在漂移")).toBeInTheDocument();
    expect(within(drift).getByText("尚未应用")).toBeInTheDocument();
    expect(within(drift).getByText(/Desired v3/)).toBeInTheDocument();
    expect(within(drift).getByText(/Observed v2/)).toBeInTheDocument();

    const lastError = within(row).getByRole("group", { name: "Last Error" });
    expect(
      within(lastError).getByText("collector_unavailable"),
    ).toBeInTheDocument();
    expect(within(lastError).getByText("Request ID")).toBeInTheDocument();
    expect(within(lastError).getByText("req_9")).toBeInTheDocument();

    // Backend raw error text stays verbatim even when it contains separators
    // that UI-generated copy must never use.
    const raw = lastError.querySelector('[data-raw-evidence="true"]');
    expect(raw).toHaveTextContent("collector · unavailable：timeout");

    expect(
      screen.getAllByText(/删除仅停止后续采集/).length,
    ).toBeGreaterThan(0);
  });

  it("shows the real empty state when no subscription exists", async () => {
    server.use(
      ...adminHandlers("Operator"),
      http.get("*/api/v1/admin/feed-subscriptions", () =>
        HttpResponse.json({
          items: [],
          page: { next_cursor: null, has_more: false },
        }),
      ),
      http.get("*/api/v1/admin/flow-control", () =>
        HttpResponse.json(flowControlFixture),
      ),
    );

    await openFeedTab();
    expect(
      await screen.findByText("暂无 Feed Subscription"),
    ).toBeInTheDocument();
  });

  it("shows read errors with raw backend text and the Request ID inline", async () => {
    server.use(
      ...adminHandlers("Operator"),
      http.get("*/api/v1/admin/feed-subscriptions", () =>
        HttpResponse.json(
          {
            error: {
              code: "internal_error",
              message: "feed · read：failed",
              details: {},
              request_id: "req-read-500",
            },
          },
          { status: 500, headers: { "X-Request-ID": "req-read-500" } },
        ),
      ),
      http.get("*/api/v1/admin/flow-control", () =>
        HttpResponse.json(flowControlFixture),
      ),
    );

    await openFeedTab();
    const feedRegion = await screen.findByRole("region", {
      name: "Feed Subscription",
    });
    expect(
      await within(feedRegion).findByText(/req-read-500/),
    ).toBeInTheDocument();
    expect(
      feedRegion.querySelector('[data-raw-evidence="true"]'),
    ).toHaveTextContent("internal_error: feed · read：failed");
  });

  it("shows capability unknown instead of claiming Current Contract", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(
          {
            error: {
              code: "internal_error",
              message: "discovery failed",
              details: {},
              request_id: "cap-500",
            },
          },
          { status: 500 },
        ),
      ),
    );

    await openFeedTab();
    expect(
      await screen.findByText(
        "运行时能力发现失败，无法确认 Feed Subscription 的读写能力。",
      ),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/Current Contract 不伪造订阅表/),
    ).not.toBeInTheDocument();
  });

  it("stops on a 409 and only writes again after refresh and explicit manual merge", async () => {
    let currentVersion = 3;
    let putCount = 0;
    const ifMatchValues: string[] = [];

    server.use(
      ...adminHandlers("Operator"),
      ...feedHandlers({ desiredVersion: () => currentVersion }),
      http.put("*/api/v1/admin/feed-subscriptions/fsub_1", async ({ request }) => {
        putCount += 1;
        const ifMatch = request.headers.get("If-Match") ?? "";
        ifMatchValues.push(ifMatch);
        const version = Number(ifMatch.replaceAll('"', ""));
        if (version === 3) {
          currentVersion = 4;
          return HttpResponse.json(
            {
              error: {
                code: "conflict",
                message: "version conflict",
                details: {
                  current_version: 4,
                  current: {
                    ...subscriptionFixture,
                    desired: {
                      ...subscriptionFixture.desired,
                      version: 4,
                      enabled: false,
                    },
                  },
                },
                request_id: "req_409",
              },
            },
            { status: 409 },
          );
        }
        return HttpResponse.json(
          {
            ...subscriptionFixture,
            desired: {
              ...subscriptionFixture.desired,
              version: 4,
              enabled: false,
            },
          },
          { status: 202 },
        );
      }),
    );

    const user = await openFeedTab();
    const row = await screen.findByTestId("feed-subscription-fsub_1");
    await user.click(within(row).getByRole("button", { name: "停用" }));

    const conflict = await screen.findByTestId("feed-conflict");
    expect(
      within(conflict).getAllByText(/版本冲突/).length,
    ).toBeGreaterThan(0);
    expect(putCount).toBe(1);

    await user.click(
      within(conflict).getByRole("button", { name: "刷新并人工合并" }),
    );
    expect(
      await screen.findByText(/已刷新到版本 4/),
    ).toBeInTheDocument();
    expect(putCount).toBe(1);

    await user.click(
      screen.getByRole("button", { name: "应用合并" }),
    );
    await waitFor(() => expect(putCount).toBe(2));
    expect(ifMatchValues).toEqual(['"3"', '"4"']);
    expect(
      await screen.findByText(/已提交/),
    ).toBeInTheDocument();
  });

  it("keeps API Rate Limit and WebSocket Slow Consumer as separate versioned writes", async () => {
    const writes: Array<{ path: string; ifMatch: string | null }> = [];
    server.use(
      ...adminHandlers("Operator"),
      ...feedHandlers(),
      http.put("*/api/v1/admin/flow-control/api-rate-limit", ({ request }) => {
        writes.push({
          path: "/api/v1/admin/flow-control/api-rate-limit",
          ifMatch: request.headers.get("If-Match"),
        });
        return HttpResponse.json({}, { status: 202 });
      }),
      http.put("*/api/v1/admin/flow-control/ws-slow-consumer", ({ request }) => {
        writes.push({
          path: "/api/v1/admin/flow-control/ws-slow-consumer",
          ifMatch: request.headers.get("If-Match"),
        });
        return HttpResponse.json({}, { status: 202 });
      }),
    );

    const user = await openFeedTab();
    const apiPanel = await screen.findByTestId("flow-api-rate-limit");
    const wsPanel = screen.getByTestId("flow-ws-slow-consumer");

    expect(
      within(apiPanel).getByText("Desired 版本 2"),
    ).toBeInTheDocument();
    expect(
      within(wsPanel).getByText("Desired 版本 5"),
    ).toBeInTheDocument();
    expect(
      within(wsPanel).getByText(/需要重启 Service Instance 后生效/),
    ).toBeInTheDocument();
    expect(
      within(apiPanel).getByRole("button", { name: "API Rate Limit 说明" }),
    ).toBeInTheDocument();
    expect(
      within(wsPanel).getByRole("button", {
        name: "WebSocket Slow Consumer 说明",
      }),
    ).toBeInTheDocument();
    expect(within(apiPanel).getByText("req/s")).toBeInTheDocument();
    expect(within(wsPanel).getByText("messages")).toBeInTheDocument();
    expect(screen.queryByLabelText(/通用阈值/)).not.toBeInTheDocument();

    await user.click(
      within(apiPanel).getByRole("button", { name: "保存 API Rate Limit" }),
    );
    await waitFor(() => expect(writes).toHaveLength(1));

    await user.click(
      within(wsPanel).getByRole("button", {
        name: "保存 WebSocket Slow Consumer",
      }),
    );
    await waitFor(() => expect(writes).toHaveLength(2));

    expect(writes).toEqual([
      {
        path: "/api/v1/admin/flow-control/api-rate-limit",
        ifMatch: '"2"',
      },
      {
        path: "/api/v1/admin/flow-control/ws-slow-consumer",
        ifMatch: '"5"',
      },
    ]);
  });

  it("keeps form units and validation inline without sending an invalid write", async () => {
    let writes = 0;
    server.use(
      ...adminHandlers("Operator"),
      ...feedHandlers(),
      http.put("*/api/v1/admin/flow-control/api-rate-limit", () => {
        writes += 1;
        return HttpResponse.json({}, { status: 202 });
      }),
    );

    const user = await openFeedTab();
    const apiPanel = await screen.findByTestId("flow-api-rate-limit");
    const requests = within(apiPanel).getByRole("spinbutton", {
      name: /requests_per_second/,
    });

    await user.clear(requests);
    await user.type(requests, "0");
    await user.click(
      within(apiPanel).getByRole("button", { name: "保存 API Rate Limit" }),
    );

    expect(
      await within(apiPanel).findByText(
        "requests_per_second 必须大于 0",
      ),
    ).toBeInTheDocument();
    expect(writes).toBe(0);
  });

  it("shows submitting state and operation results inline", async () => {
    const release: { write?: () => void } = {};
    const writeGate = new Promise<void>((resolve) => {
      release.write = resolve;
    });
    server.use(
      ...adminHandlers("Operator"),
      ...feedHandlers(),
      http.put(
        "*/api/v1/admin/flow-control/api-rate-limit",
        async () => {
          await writeGate;
          return HttpResponse.json({}, { status: 202 });
        },
      ),
    );

    const user = await openFeedTab();
    const apiPanel = await screen.findByTestId("flow-api-rate-limit");
    await user.click(
      within(apiPanel).getByRole("button", { name: "保存 API Rate Limit" }),
    );

    expect(
      await within(apiPanel).findByText("正在保存…"),
    ).toBeInTheDocument();
    release.write?.();
    expect(
      await within(apiPanel).findByText(
        "保存请求已提交，等待 Observed 确认。",
      ),
    ).toBeInTheDocument();
  });

  it("shows write failures, raw backend text and the Request ID inline", async () => {
    server.use(
      ...adminHandlers("Operator"),
      ...feedHandlers(),
      http.put(
        "*/api/v1/admin/flow-control/api-rate-limit",
        () =>
          HttpResponse.json(
            {
              error: {
                code: "forbidden",
                message: "role · denied：policy",
                details: {},
                request_id: "req-flow-403",
              },
            },
            { status: 403, headers: { "X-Request-ID": "req-flow-403" } },
          ),
      ),
    );

    const user = await openFeedTab();
    const apiPanel = await screen.findByTestId("flow-api-rate-limit");
    await user.click(
      within(apiPanel).getByRole("button", { name: "保存 API Rate Limit" }),
    );

    expect(
      await within(apiPanel).findByText("Request ID req-flow-403"),
    ).toBeInTheDocument();
    expect(
      apiPanel.querySelector('[data-raw-evidence="true"]'),
    ).toHaveTextContent("role · denied：policy");
  });

  it("keeps Auditor read-only with a visible role reason", async () => {
    server.use(...adminHandlers("Auditor"), ...feedHandlers());
    await openFeedTab();

    const row = await screen.findByTestId("feed-subscription-fsub_1");
    expect(within(row).getByRole("button", { name: "停用" })).toBeDisabled();
    expect(
      within(row).getAllByText("需要 SuperAdmin 或 Operator 角色").length,
    ).toBeGreaterThan(0);

    const apiPanel = screen.getByTestId("flow-api-rate-limit");
    expect(
      within(apiPanel).getByRole("button", { name: "保存 API Rate Limit" }),
    ).toBeDisabled();
    expect(
      within(apiPanel).getAllByText("需要 SuperAdmin 或 Operator 角色").length,
    ).toBeGreaterThan(0);
  });
});
