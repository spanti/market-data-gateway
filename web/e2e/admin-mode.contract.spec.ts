import {
  expect,
  test,
  type Page,
  type Request,
  type Route,
} from "@playwright/test";

/**
 * CONTRACT-ONLY: the Admin API does not exist in the current backend.
 *
 * These flows are exercised in a real browser with Playwright request
 * interception. They verify the frontend contract for 401/403/409/429 and the
 * high-risk confirmation path; they are NOT real backend integration and must
 * not be reported as such.
 */

const ADMIN_GAP = "需要 Admin API 服务实例接口（未实现）";

function futureIso(offsetMs: number): string {
  return new Date(Date.now() + offsetMs).toISOString();
}

const capabilityDocument = {
  schema_version: 1,
  server_time: futureIso(0),
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
    {
      name: "prometheus",
      kind: "monitoring",
      state: "available",
      reason: null,
    },
  ],
  capabilities: {
    oidc_session: { state: "enabled" },
    service_lifecycle: {
      state: "enabled",
      actions: ["start", "stop", "reload"],
    },
    monitoring_proxy: { state: "enabled", arbitrary_promql: false },
    ws_admin_session: { state: "enabled", path: "/api/v1/admin/ws" },
  },
};

const serviceInstances = {
  items: [
    {
      instance_id: "collector-1",
      service_role: "collector",
      display_name: "Collector primary",
      desired_state: "running",
      observed_state: "running",
      health: "healthy",
      ready: true,
      ready_reason: "collector ready",
      pid: 1001,
      started_at: futureIso(-3_600_000),
      last_observed_at: futureIso(-1_000),
      last_error: null,
      available_actions: ["start", "stop", "reload"],
    },
    {
      instance_id: "api-1",
      service_role: "api",
      display_name: "API primary",
      desired_state: "running",
      observed_state: "running",
      health: "healthy",
      ready: true,
      ready_reason: "collector subscription ready",
      pid: 1002,
      started_at: futureIso(-3_600_000),
      last_observed_at: futureIso(-1_000),
      last_error: null,
      available_actions: ["start", "stop", "reload"],
    },
  ],
  page: { next_cursor: null, has_more: false },
};

function sessionBody(role: "SuperAdmin" | "Operator" | "Auditor") {
  return {
    authenticated: true,
    server_time: futureIso(0),
    user: {
      subject: "oidc-subject-123",
      display_name: "Ada Admin",
      email: "ada@example.com",
    },
    access_role: role,
    session: {
      issued_at: futureIso(-60_000),
      last_activity_at: futureIso(-1_000),
      idle_expires_at: futureIso(30 * 60_000),
      absolute_expires_at: futureIso(8 * 3_600_000),
    },
    csrf: { header_name: "X-CSRF-Token", token: "csrf-token" },
  };
}

async function json(route: Route, status: number, body: unknown, headers = {}) {
  await route.fulfill({
    status,
    contentType: "application/json",
    headers,
    body: JSON.stringify(body),
  });
}

async function installAdmin(
  page: Page,
  options: { role?: "SuperAdmin" | "Operator" | "Auditor" } = {},
): Promise<void> {
  await page.route("**/api/v1/admin/capabilities", (route) =>
    json(route, 200, capabilityDocument),
  );
  await page.route("**/api/v1/admin/auth/session", (route) =>
    json(route, 200, sessionBody(options.role ?? "SuperAdmin")),
  );
  await page.route("**/api/v1/admin/service-instances", (route) =>
    json(route, 200, serviceInstances),
  );
}

async function openServiceInstanceTab(page: Page): Promise<void> {
  await page.goto("/console");
  await page.getByRole("tab", { name: "Service Instance" }).click();
  await expect(page.getByTestId("service-instance-api-1")).toBeVisible();
}

async function submitStop(
  page: Page,
  instanceId = "api-1",
  reason = "drain before certificate rotation",
): Promise<void> {
  const row = page.getByTestId(`service-instance-${instanceId}`);
  await row.getByRole("button", { name: "Graceful Stop" }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("操作原因").fill(reason);
  await dialog.getByLabel(/输入目标实例名/).fill(instanceId);
  await dialog.getByRole("button", { name: "确认执行" }).click();
}

function trackUntrustedOps(page: Page): string[] {
  const requested: string[] = [];
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (
      path === "/status" ||
      path === "/metrics" ||
      path.startsWith("/ops/") ||
      path.startsWith("/prometheus/")
    ) {
      requested.push(path);
    }
  });
  return requested;
}

test.describe("CONTRACT-ONLY Admin API mode (backend absent)", () => {
  test("uses the service-instance seam, confirms high-risk actions and sends CSRF + idempotency headers", async ({
    page,
  }) => {
    const opsPaths = trackUntrustedOps(page);
    await installAdmin(page);

    let postCount = 0;
    const captured: {
      request: Request | null;
      body: Record<string, unknown> | null;
    } = { request: null, body: null };
    await page.route(
      "**/api/v1/admin/service-instances/*/stop",
      async (route) => {
        postCount += 1;
        captured.request = route.request();
        captured.body = (await route.request().postDataJSON()) as Record<
          string,
          unknown
        >;
        await json(
          route,
          202,
          { operation_id: "op_1", request_id: "req_1", status: "accepted" },
          { "X-Request-ID": "req_1" },
        );
      },
    );
    await page.route("**/api/v1/admin/operations/op_1", (route) =>
      json(route, 200, {
        operation_id: "op_1",
        kind: "service_instance.stop",
        target: { instance_id: "api-1", service_role: "api" },
        requested_by: {
          subject: "oidc-subject-123",
          display_name: "Ada Admin",
          access_role: "SuperAdmin",
        },
        reason: "drain before certificate rotation",
        status: "succeeded",
        requested_at: futureIso(-1_000),
        started_at: futureIso(-900),
        finished_at: futureIso(-500),
        result: {},
        error: null,
        request_id: "req_1",
      }),
    );

    await openServiceInstanceTab(page);

    // Real Admin instance identity, not a Current Contract /status fallback.
    const row = page.getByTestId("service-instance-api-1");
    await expect(row).toContainText("API primary");
    await expect(row.getByText("api-1", { exact: true }).first()).toBeVisible();
    expect(opsPaths).toEqual([]);

    await row.getByRole("button", { name: "Graceful Stop" }).click();
    const dialog = page.getByRole("dialog");
    const confirm = dialog.getByRole("button", { name: "确认执行" });
    await expect(confirm).toBeDisabled();

    await dialog.getByLabel("操作原因").fill("drain before certificate rotation");
    await expect(confirm).toBeDisabled();

    await dialog.getByLabel(/输入目标实例名/).fill("api");
    await expect(confirm).toBeDisabled();
    await expect(dialog.getByText("实例名不匹配，无法提交。")).toBeVisible();

    await dialog.getByLabel(/输入目标实例名/).fill("api-1");
    await expect(confirm).toBeEnabled();
    await confirm.click();

    await expect(row.getByText("成功")).toBeVisible();
    expect(postCount).toBe(1);
    expect(captured.body).toMatchObject({
      reason: "drain before certificate rotation",
      confirm: true,
      confirm_instance_name: "api-1",
    });
    expect(captured.request?.headers()["idempotency-key"]).toBeTruthy();
    expect(captured.request?.headers()["x-csrf-token"]).toBe("csrf-token");

    // No automatic replay of a non-idempotent write.
    await page.waitForTimeout(1_000);
    expect(postCount).toBe(1);
  });

  test("401 stops writes, clears the session and returns to the login entry", async ({
    page,
  }) => {
    await installAdmin(page);
    await page.route(
      "**/api/v1/admin/service-instances/*/stop",
      (route) =>
        json(
          route,
          401,
          {
            error: {
              code: "unauthenticated",
              message: "session expired",
              details: { reason: "idle_timeout" },
              request_id: "req-401",
            },
          },
          { "X-Request-ID": "req-401" },
        ),
    );

    await openServiceInstanceTab(page);
    await submitStop(page);

    await expect(page.getByTestId("global-http-notice")).toContainText(
      "会话已失效或未登录，写操作已停止。请回到登录入口。",
    );
    const banner = page.getByTestId("runtime-mode-banner");
    await expect(banner.getByText("会话已失效")).toBeVisible();
    await expect(banner.getByRole("link", { name: "登录" })).toBeVisible();
    await expect(page.getByTestId("service-instance-gap")).toContainText(
      "需要登录",
    );
  });

  test("403 names the missing role without logging out", async ({ page }) => {
    await installAdmin(page);
    await page.route(
      "**/api/v1/admin/service-instances/*/stop",
      (route) =>
        json(
          route,
          403,
          {
            error: {
              code: "forbidden",
              message: "role denied",
              details: { reason: "role", required_role: "SuperAdmin" },
              request_id: "req-403",
            },
          },
          { "X-Request-ID": "req-403" },
        ),
    );

    await openServiceInstanceTab(page);
    await submitStop(page);

    await expect(page.getByTestId("global-http-notice")).toContainText(
      "缺少权限：需要 SuperAdmin 角色。",
    );
    await expect(page.getByTestId("service-instance-api-1")).toContainText(
      "forbidden: role denied",
    );
    await expect(
      page.getByTestId("runtime-mode-banner").getByText("SuperAdmin"),
    ).toBeVisible();
  });

  test("409 surfaces a manual-merge conflict", async ({ page }) => {
    await installAdmin(page);
    await page.route(
      "**/api/v1/admin/service-instances/*/stop",
      (route) =>
        json(
          route,
          409,
          {
            error: {
              code: "conflict",
              message: "version conflict",
              details: {
                reason: "operation_in_progress",
                current_version: 3,
              },
              request_id: "req-409",
            },
          },
          { "X-Request-ID": "req-409" },
        ),
    );

    await openServiceInstanceTab(page);
    await submitStop(page);

    await expect(page.getByTestId("global-http-notice")).toContainText(
      "数据已被他人修改（409 冲突）",
    );
    await expect(page.getByTestId("service-instance-api-1")).toContainText(
      "conflict: version conflict",
    );
  });

  test("429 honours Retry-After", async ({ page }) => {
    await installAdmin(page);
    await page.route(
      "**/api/v1/admin/service-instances/*/stop",
      (route) =>
        json(
          route,
          429,
          {
            error: {
              code: "rate_limited",
              message: "too many requests",
              details: { retry_after_s: 7 },
              request_id: "req-429",
            },
          },
          { "Retry-After": "7", "X-Request-ID": "req-429" },
        ),
    );

    await openServiceInstanceTab(page);
    await submitStop(page);

    await expect(page.getByTestId("global-http-notice")).toContainText(
      "请求过于频繁，请在 7 秒后重试。",
    );
    await expect(page.getByTestId("service-instance-api-1")).toContainText(
      "rate_limited: too many requests",
    );
  });

  test("shows the explicit Admin service-instance gap when the capability is unsupported", async ({
    page,
  }) => {
    await installAdmin(page);
    await page.route("**/api/v1/admin/capabilities", (route) =>
      json(route, 200, {
        ...capabilityDocument,
        capabilities: {
          ...capabilityDocument.capabilities,
          service_lifecycle: { state: "unsupported" },
        },
      }),
    );

    await page.goto("/console");
    await page.getByRole("tab", { name: "Service Instance" }).click();
    await expect(page.getByTestId("service-instance-gap")).toContainText(
      ADMIN_GAP,
    );
    await expect(page.getByTestId("service-instance-api-1")).toHaveCount(0);
  });
});
