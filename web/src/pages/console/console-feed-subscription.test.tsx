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
    message: "collector did not confirm",
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
      (await screen.findAllByText("需要 Admin API")).length,
    ).toBeGreaterThan(0);
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(screen.queryByText("fsub_1")).not.toBeInTheDocument();
  });
});

describe("Feed Subscription — Admin API contract target", () => {
  it("shows Desired, Observed, Drift and Last Error as separate columns", async () => {
    server.use(...adminHandlers("Operator"), ...feedHandlers());
    await openFeedTab();

    const row = await screen.findByTestId("feed-subscription-fsub_1");
    for (const header of ["Desired", "Observed", "Drift", "Last Error"]) {
      expect(
        screen.getByRole("columnheader", { name: header }),
      ).toBeInTheDocument();
    }

    expect(within(row).getByText(/启用 · v3/)).toBeInTheDocument();
    expect(within(row).getByText(/已应用 · v2/)).toBeInTheDocument();
    expect(within(row).getByText(/漂移：尚未应用/)).toBeInTheDocument();
    expect(within(row).getByText(/collector_unavailable/)).toBeInTheDocument();
    expect(
      screen.getAllByText(/删除仅停止后续采集/).length,
    ).toBeGreaterThan(0);
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

    expect(within(apiPanel).getByText(/版本 2/)).toBeInTheDocument();
    expect(within(wsPanel).getByText(/版本 5/)).toBeInTheDocument();
    expect(
      within(wsPanel).getByText(/需要重启 Service Instance 后生效/),
    ).toBeInTheDocument();
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
