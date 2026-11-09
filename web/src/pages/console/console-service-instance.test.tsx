import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import { renderApp } from "@/test/render-app";
import { server } from "@/test/server";

const apiStatus = {
  dataset: "bars",
  schema_version: 1,
  last_flushed_ts: "2026-11-09T04:24:42.946489000Z",
  active_file_count: 1,
  median_file_size_bytes: 5572,
  intervals: {},
  service: "api",
  role: "api",
  uptime_s: 3600,
  http: { host: "127.0.0.1", port: 8080 },
  zmq: {
    publish_endpoint: "tcp://127.0.0.1:5556",
    collector_endpoint: "tcp://127.0.0.1:5555",
  },
  messages_received: 10,
  messages_forwarded: 10,
  collector: {
    ready: true,
    last_bar_ts: "2024-01-01T00:01:00.000000000Z",
    status: null,
  },
};

const collectorStatus = {
  service: "collector",
  role: "collector",
  ready: true,
  uptime_s: 1800,
  feed: "fake",
  last_ingest_ts: "2026-11-09T04:24:54.380612000Z",
  last_bar_ts: "2024-01-01T00:01:00.000000000Z",
  bars_ingested: 67,
  bars_written: 1,
  stale_count: 4,
  gap_unrecoverable_count: 1,
  write_queue_depth: 0,
  write_dropped_bars: 0,
  write_lag_bars: 0,
  write_last_flush_ts: "2026-11-09T04:24:53.921467000Z",
  write_last_flush_duration_s: 0.0148,
  write_sink: {
    mode: "buffered",
    queue_depth: 0,
    queue_capacity: 10000,
    pending_bytes: 0,
    dropped_bars: 0,
    lag_bars: 0,
    dropped_identities: 0,
    flushes: 1,
    flush_failures: 0,
    persisted_bars: 1,
    last_flush_ts: "2026-11-09T04:24:53.921467000Z",
    last_flush_duration_s: 0.0148,
  },
  quarantine: {
    schema_version: 1,
    root: "/data/quarantine",
    retention_days: 30,
    records_written: 0,
    write_failures: 0,
    last_write_ts: null,
    active_file_count: 0,
    active_bytes: 0,
    gc_runs: 0,
    last_gc: null,
    records_by_source_stage: {},
    records_by_reason_code: {},
    records_by_stage_and_reason: [],
  },
  health: {
    stale: [],
    stale_count: 4,
    tracked: [],
    gap_unrecoverable: [],
    gap_unrecoverable_count: 1,
  },
};

const replayStatus = {
  service: "replay",
  role: "replay",
  uptime_s: 60,
  control_endpoint: "tcp://127.0.0.1:60180",
  publish_endpoint: "tcp://127.0.0.1:60179",
  active_sessions: [],
  sessions: [],
  instrument_snapshot: {
    status: "loaded",
    loaded: true,
    snapshot: "snapshot.parquet",
    count: 2,
  },
};

const capabilityDocument = ({
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
    {
      name: "supervisor",
      kind: "service_lifecycle",
      state: "available",
      reason: null,
    },
  ],
  capabilities: {
    service_lifecycle: {
      state: "enabled",
      actions: ["start", "stop", "reload"],
    },
    ws_admin_session: { state: "enabled" },
  },
  limits: {},
});

const adminServiceInstances = {
  items: [
    {
      instance_id: "collector",
      service_role: "collector",
      display_name: "Collector primary",
      desired_state: "running",
      observed_state: "running",
      health: "healthy",
      ready: true,
      ready_reason: "collector ready",
      pid: 1001,
      started_at: "2026-11-09T11:00:00.000000000Z",
      last_observed_at: "2026-11-09T12:00:00.000000000Z",
      last_error: null,
      available_actions: ["start", "stop", "reload"],
    },
    {
      instance_id: "api",
      service_role: "api",
      display_name: "API primary",
      desired_state: "running",
      observed_state: "running",
      health: "healthy",
      ready: true,
      ready_reason: "collector subscription ready",
      pid: 1002,
      started_at: "2026-11-09T11:00:00.000000000Z",
      last_observed_at: "2026-11-09T12:00:00.000000000Z",
      last_error: null,
      available_actions: ["start", "stop", "reload"],
    },
    {
      instance_id: "replay",
      service_role: "replay",
      display_name: "Replay primary",
      desired_state: "running",
      observed_state: "running",
      health: "healthy",
      ready: true,
      ready_reason: "replay service ready",
      pid: 1003,
      started_at: "2026-11-09T11:00:00.000000000Z",
      last_observed_at: "2026-11-09T12:00:00.000000000Z",
      last_error: null,
      available_actions: ["start", "stop", "reload"],
    },
  ],
  page: { next_cursor: null, has_more: false },
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
          display_name: "Ada Admin",
          email: "ada@example.com",
        },
        access_role: role,
        session: null,
        csrf: { header_name: "X-CSRF-Token", token: "csrf-token" },
      }),
    ),
    http.get("*/api/v1/admin/service-instances", () =>
      HttpResponse.json(adminServiceInstances),
    ),
  ];
}

function statusHandlers() {
  return [
    http.get("*/ops/collector/status", () => HttpResponse.json(collectorStatus)),
    http.get("*/ops/collector/readyz", () =>
      HttpResponse.json({ ready: true, reason: "collector ready" }),
    ),
    http.get("*/ops/replay/status", () => HttpResponse.json(replayStatus)),
    http.get("*/ops/replay/readyz", () =>
      HttpResponse.json({ ready: true, reason: "replay service ready" }),
    ),
    http.get("*/readyz", () =>
      HttpResponse.json({
        ready: true,
        reason: "collector subscription ready",
        collector_ready: true,
      }),
    ),
    http.get("*/status", () => HttpResponse.json(apiStatus)),
  ];
}

async function openServiceInstanceTab() {
  const user = userEvent.setup();
  renderApp({ initialEntries: ["/console"] });
  await user.click(
    await screen.findByRole("tab", { name: "Service Instance" }),
  );
  return user;
}

describe("Service Instance — Current Contract", () => {
  it("shows the three real Service Roles with identity, readiness, uptime and last observed time", async () => {
    server.use(...statusHandlers());
    await openServiceInstanceTab();

    expect(
      screen.getByRole("columnheader", { name: "Last Observed" }),
    ).toBeInTheDocument();

    for (const role of ["collector", "api", "replay"] as const) {
      const row = await screen.findByTestId(`service-instance-${role}`);
      expect(within(row).getByText(role)).toBeInTheDocument();
      expect(within(row).getByText("Ready")).toBeInTheDocument();
      expect(
        within(row).getByTestId(`last-observed-${role}`).textContent,
      ).toContain("UTC");
    }

    const collectorRow = screen.getByTestId("service-instance-collector");
    expect(within(collectorRow).getByText("fake")).toBeInTheDocument();
    expect(within(collectorRow).getByText("30m 0s")).toBeInTheDocument();

    const apiRow = screen.getByTestId("service-instance-api");
    expect(within(apiRow).getByText("tcp://127.0.0.1:5556")).toBeInTheDocument();

    const replayRow = screen.getByTestId("service-instance-replay");
    expect(within(replayRow).getByText("loaded")).toBeInTheDocument();
  });

  it("disables every lifecycle control with a visible Admin API reason", async () => {
    server.use(...statusHandlers());
    await openServiceInstanceTab();

    const apiRow = await screen.findByTestId("service-instance-api");
    for (const label of ["Start", "Graceful Stop", "Reload"]) {
      expect(
        within(apiRow).getByRole("button", { name: label }),
      ).toBeDisabled();
    }
    expect(
      within(apiRow).getAllByText("需要 Admin API").length,
    ).toBeGreaterThan(0);
  });
});

describe("Service Instance — Admin API lifecycle (contract target)", () => {
  it("uses the Admin service-instance seam and never reads unauthenticated ops endpoints", async () => {
    let opsRequested = false;
    server.use(
      ...adminHandlers("SuperAdmin"),
      http.get("*/status", () => {
        opsRequested = true;
        return HttpResponse.json(apiStatus);
      }),
      http.get("*/ops/*", () => {
        opsRequested = true;
        return HttpResponse.json({});
      }),
    );
    await openServiceInstanceTab();

    const apiRow = await screen.findByTestId("service-instance-api");
    expect(within(apiRow).getByText("API primary")).toBeInTheDocument();
    expect(within(apiRow).getByText("1002")).toBeInTheDocument();
    expect(within(apiRow).getByText("PID")).toBeInTheDocument();
    expect(opsRequested).toBe(false);
  });

  it("shows an explicit capability gap when the Admin service-instance API is unsupported", async () => {
    let serviceInstanceRequested = false;
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json({
          ...capabilityDocument,
          capabilities: {
            ...capabilityDocument.capabilities,
            service_lifecycle: { state: "unsupported" },
          },
        }),
      ),
      ...adminHandlers("SuperAdmin"),
      http.get("*/api/v1/admin/service-instances", () => {
        serviceInstanceRequested = true;
        return HttpResponse.json(adminServiceInstances);
      }),
    );
    await openServiceInstanceTab();

    expect(
      await screen.findByText("需要 Admin API 服务实例接口（未实现）"),
    ).toBeInTheDocument();
    expect(serviceInstanceRequested).toBe(false);
  });

  it("401 from a lifecycle write stops writes and returns to the login entry", async () => {
    server.use(
      ...adminHandlers("SuperAdmin"),
      http.post("*/api/v1/admin/service-instances/api/stop", () =>
        HttpResponse.json(
          {
            error: {
              code: "unauthenticated",
              message: "session expired",
              details: { reason: "idle_timeout" },
              request_id: "req-401",
            },
          },
          { status: 401, headers: { "X-Request-ID": "req-401" } },
        ),
      ),
    );
    const user = await openServiceInstanceTab();

    const apiRow = await screen.findByTestId("service-instance-api");
    await user.click(
      within(apiRow).getByRole("button", { name: "Graceful Stop" }),
    );
    const dialog = await screen.findByRole("dialog");
    await user.type(within(dialog).getByLabelText("操作原因"), "rotate");
    await user.type(within(dialog).getByLabelText(/输入目标实例名/), "api");
    await user.click(within(dialog).getByRole("button", { name: "确认执行" }));

    expect(
      await screen.findByTestId("global-http-notice"),
    ).toHaveTextContent(/会话已失效或未登录，写操作已停止/);
    expect(
      await screen.findByTestId("runtime-mode-banner"),
    ).toHaveTextContent("会话已失效");
    expect(await screen.findByTestId("service-instance-gap")).toHaveTextContent(
      "需要登录",
    );
  });

  it("requires a reason and the exact instance name before Stop can be submitted", async () => {
    server.use(...adminHandlers("SuperAdmin"), ...statusHandlers());
    const user = await openServiceInstanceTab();

    const apiRow = await screen.findByTestId("service-instance-api");
    await user.click(
      within(apiRow).getByRole("button", { name: "Graceful Stop" }),
    );

    const dialog = await screen.findByRole("dialog");
    const confirm = within(dialog).getByRole("button", { name: "确认执行" });
    expect(confirm).toBeDisabled();

    await user.type(
      within(dialog).getByLabelText("操作原因"),
      "drain before certificate rotation",
    );
    expect(confirm).toBeDisabled();

    await user.type(
      within(dialog).getByLabelText(/输入目标实例名/),
      "api-1",
    );
    expect(confirm).toBeDisabled();
    expect(
      within(dialog).getByText("实例名不匹配，无法提交。"),
    ).toBeInTheDocument();

    await user.clear(within(dialog).getByLabelText(/输入目标实例名/));
    await user.type(within(dialog).getByLabelText(/输入目标实例名/), "api");
    expect(confirm).toBeEnabled();
  });

  it("treats acceptance as unfinished, turns a disconnect into unknown, and resolves it by querying the Operation without retrying the POST", async () => {
    let postCount = 0;
    let operationStatus: "accepted" | "succeeded" = "accepted";

    server.use(
      ...adminHandlers("SuperAdmin"),
      ...statusHandlers(),
      http.post("*/api/v1/admin/service-instances/api/stop", async ({ request }) => {
        postCount += 1;
        expect(request.headers.get("Idempotency-Key")).toBeTruthy();
        expect(request.headers.get("X-CSRF-Token")).toBe("csrf-token");
        const body = (await request.json()) as Record<string, unknown>;
        expect(body).toMatchObject({
          reason: "drain before certificate rotation",
          confirm: true,
          confirm_instance_name: "api",
        });
        return HttpResponse.json(
          {
            operation_id: "op_1",
            kind: "service_instance.stop",
            status: "accepted",
            request_id: "req_1",
          },
          { status: 202 },
        );
      }),
      http.get("*/api/v1/admin/operations/op_1", () =>
        HttpResponse.json({
          operation_id: "op_1",
          kind: "service_instance.stop",
          target: { instance_id: "api", service_role: "api" },
          requested_by: {
            subject: "oidc-subject-123",
            display_name: "Ada Admin",
            access_role: "SuperAdmin",
          },
          reason: "drain before certificate rotation",
          status: operationStatus,
          requested_at: "2026-11-09T12:00:00.000000000Z",
          started_at: "2026-11-09T12:00:01.000000000Z",
          finished_at: null,
          result: null,
          error: null,
          request_id: "req_1",
        }),
      ),
    );

    const user = await openServiceInstanceTab();
    const apiRow = await screen.findByTestId("service-instance-api");
    await user.click(
      within(apiRow).getByRole("button", { name: "Graceful Stop" }),
    );

    const dialog = await screen.findByRole("dialog");
    await user.type(within(dialog).getByLabelText("操作原因"), "drain before certificate rotation");
    await user.type(within(dialog).getByLabelText(/输入目标实例名/), "api");
    await user.click(within(dialog).getByRole("button", { name: "确认执行" }));

    expect(await within(apiRow).findByText("已接受（未完成）")).toBeInTheDocument();
    expect(within(apiRow).queryByText("成功")).not.toBeInTheDocument();
    expect(postCount).toBe(1);

    // The operation is accepted but the console loses its connection.
    act(() => {
      window.dispatchEvent(new Event("offline"));
    });

    expect(await within(apiRow).findByText("状态未知")).toBeInTheDocument();
    expect(within(apiRow).queryByText("失败")).not.toBeInTheDocument();
    expect(within(apiRow).queryByText("成功")).not.toBeInTheDocument();
    expect(postCount).toBe(1);

    // Reconnection queries the Operation; it never re-submits the write.
    operationStatus = "succeeded";
    act(() => {
      window.dispatchEvent(new Event("online"));
    });

    expect(await within(apiRow).findByText("成功")).toBeInTheDocument();
    expect(postCount).toBe(1);
  });

  it("shows the explicit Admin service-instance gap and no fallback rows when service_lifecycle is unsupported", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json({
          ...capabilityDocument,
          capabilities: {
            ...capabilityDocument.capabilities,
            service_lifecycle: { state: "unsupported" },
          },
        }),
      ),
      ...adminHandlers("SuperAdmin"),
    );
    await openServiceInstanceTab();

    expect(await screen.findByTestId("service-instance-gap")).toHaveTextContent(
      "需要 Admin API 服务实例接口（未实现）",
    );
    expect(screen.queryByTestId("service-instance-api")).not.toBeInTheDocument();
  });

  it.each([
    ["SuperAdmin", true],
    ["Operator", false],
    ["Auditor", false],
  ] as const)(
    "only SuperAdmin sees enabled lifecycle actions (%s)",
    async (role, expectedEnabled) => {
      server.use(...adminHandlers(role), ...statusHandlers());
      await openServiceInstanceTab();

      const apiRow = await screen.findByTestId("service-instance-api");
      const stop = within(apiRow).getByRole("button", {
        name: "Graceful Stop",
      });
      if (expectedEnabled) {
        expect(stop).toBeEnabled();
      } else {
        expect(stop).toBeDisabled();
        expect(
          within(apiRow).getAllByText("需要 SuperAdmin 角色").length,
        ).toBeGreaterThan(0);
      }
    },
  );
});
