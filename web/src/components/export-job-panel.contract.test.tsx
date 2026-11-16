/**
 * CONTRACT TARGET TESTS — NOT REAL BACKEND INTEGRATION.
 *
 * The current backend has no `/api/v1/admin/export-jobs`. Every Admin flow in
 * this file is exercised against MSW fixtures derived from
 * `admin-api-contract.md` §6/§11/§12. These tests do not prove that an Admin
 * API exists or behaves this way.
 */

import { QueryClientProvider } from "@tanstack/react-query";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import type { ComponentProps } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { resetRetryAfter } from "@/runtime/retry-after";
import type { CapabilityState } from "@/api/capabilities";
import type { ExportJob, ExportJobStatus } from "@/api/endpoints/admin";
import { ExportJobPanel } from "@/components/export-job-panel";
import { TooltipProvider } from "@/components/ui/tooltip";
import { createQueryClient } from "@/api/query-client";
import { classifyExportRange } from "@/lib/export-policy";
import { server } from "@/test/server";
import { installResizeObserverStub } from "@/test/tooltip-env";
import { expectNoBannedUiSymbols } from "@/test/ui-copy";
import type { PermissionSubject } from "@/runtime/permissions";

installResizeObserverStub();

const START = new Date("2026-01-01T00:00:00.000Z");
const END_45_DAYS = new Date("2026-02-15T00:00:00.000Z");
const NOW = new Date("2026-11-09T12:00:00.000Z");

const OPERATOR: PermissionSubject = { kind: "role", role: "Operator" };
const SUPERADMIN: PermissionSubject = { kind: "role", role: "SuperAdmin" };
const AUDITOR: PermissionSubject = { kind: "role", role: "Auditor" };
const ANONYMOUS: PermissionSubject = { kind: "anonymous" };

const ENABLED_EXPORT_JOB = {
  state: "enabled" as const,
  formats: ["csv"],
  min_days: 31,
  max_days: 366,
  retention_s: 86400,
  max_running_per_user: 1,
};

function adminCapability(
  compatible = true,
  exportJob: Record<string, unknown> = ENABLED_EXPORT_JOB,
): CapabilityState {
  return {
    mode: "admin-api",
    compatible,
    lastSuccessAt: "2026-11-09T12:00:00.000000000Z",
    document: {
      server_time: "2026-11-09T12:00:00.000000000Z",
      version: {
        admin_api: "1.0.0",
        contract: 1,
        supported_contracts: compatible ? [1] : [2],
      },
      auth: {
        required: true,
        roles: ["SuperAdmin", "Operator", "Auditor"],
      },
      adapters: [],
      capabilities: { export_job: exportJob },
    } as never,
  };
}

function exportJob(overrides: Partial<ExportJob> = {}): ExportJob {
  return {
    job_id: "exp_01JABCXYZ",
    status: "running",
    instrument_id: "binance:BTC/USDT",
    interval: "1m",
    start: "2026-01-01T00:00:00.000000000Z",
    end: "2026-02-15T00:00:00.000000000Z",
    range_days: 45,
    format: "csv",
    created_at: "2026-11-09T12:00:00.000000000Z",
    created_by: {
      subject: "oidc-subject-123",
      display_name: "Ada Operator",
    },
    progress: {
      processed_rows: 1200,
      processed_bytes: 65536,
      total_rows: null,
      percent: null,
      indeterminate: true,
    },
    result: null,
    error: null,
    cancelled_at: null,
    expires_at: null,
    download_url: null,
    ...overrides,
  };
}

function succeededJob(overrides: Partial<ExportJob> = {}): ExportJob {
  return exportJob({
    status: "succeeded",
    progress: {
      processed_rows: 450000,
      processed_bytes: 12_345_678,
      total_rows: 450000,
      percent: 100,
      indeterminate: false,
    },
    result: {
      file_name: "binance_BTCUSDT_1m.csv",
      content_type: "text/csv; charset=utf-8",
      size_bytes: 12_345_678,
      sha256: "e3b0c44298fc1c149afbf4c8996fb924abc",
      row_count: 450000,
    },
    expires_at: "2026-11-10T12:00:00.000000000Z",
    download_url: "/api/v1/admin/export-jobs/exp_01JABCXYZ/download",
    ...overrides,
  });
}

function renderPanel(
  props: Partial<ComponentProps<typeof ExportJobPanel>> = {},
) {
  const queryClient = createQueryClient();
  const decision =
    props.decision ??
    classifyExportRange(START, END_45_DAYS);
  return {
    queryClient,
    ...render(
      <QueryClientProvider client={queryClient}>
        <TooltipProvider>
          <ExportJobPanel
            query={{
              instrumentId: "binance:BTC/USDT",
              interval: "1m",
              start: START,
              end: END_45_DAYS,
            }}
            decision={decision}
            subject={OPERATOR}
            capability={adminCapability()}
            currentSubject="oidc-subject-123"
            now={NOW}
            {...props}
          />
        </TooltipProvider>
      </QueryClientProvider>,
    ),
  };
}

function exportPanel(): HTMLElement {
  return screen.getByRole("region", { name: "Export Job" });
}

function statusPill(label: string): HTMLElement | null {
  const match = screen
    .getAllByText(label)
    .find((element) => element.closest('[data-slot="status-badge"]'));
  return (match?.closest('[data-slot="status-badge"]') as HTMLElement) ?? null;
}

function listHandler(
  items: ExportJob[] | (() => ExportJob[]),
  onRequest?: (url: URL) => void,
) {
  return http.get("*/api/v1/admin/export-jobs", ({ request }) => {
    const url = new URL(request.url);
    onRequest?.(url);
    return HttpResponse.json({
      items: typeof items === "function" ? items() : items,
      page: { next_cursor: null, has_more: false },
    });
  });
}

describe("CONTRACT TARGET (Admin API, MSW only — not integrated)", () => {
  it("Current Contract disables the job control with 需要 Admin API and sends no admin request", async () => {
    let requests = 0;
    server.use(
      listHandler([], () => {
        requests += 1;
      }),
    );
    renderPanel({ subject: ANONYMOUS, capability: null });

    expect(
      screen.getByRole("button", { name: "创建 Export Job" }),
    ).toBeDisabled();
    expect(screen.getAllByText("需要 Admin API").length).toBeGreaterThan(0);
    expect(
      screen.getByText(/不会自动退回超大 Blob/),
    ).toBeInTheDocument();
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(requests).toBe(0);
  });

  it("lists server-scoped Operator jobs with indeterminate progress and no ownership selector", async () => {
    const requested: URL[] = [];
    server.use(
      listHandler([exportJob()], (url) => requested.push(url)),
    );
    renderPanel();

    expect(
      await screen.findByText("进度未知"),
    ).toBeInTheDocument();
    expect(screen.getByText(/已处理 1,200 行/)).toBeInTheDocument();
    expect(screen.getByText("生成中")).toBeInTheDocument();
    expect(screen.getByText("Ada Operator")).toBeInTheDocument();
    expect(screen.getByText(/仅本人 Export Job/)).toBeInTheDocument();
    expect(
      screen.queryByRole("combobox", { name: /所有权|owner/i }),
    ).not.toBeInTheDocument();
    expect(requested[0]?.searchParams.get("limit")).toBe("50");
    expect(
      screen.getByText(/已有运行中的 Export Job/),
    ).toBeInTheDocument();
  });

  it("lets SuperAdmin see every creator while the server remains the ownership filter", async () => {
    server.use(
      listHandler([
        succeededJob({
          job_id: "exp_own",
          created_by: { subject: "s1", display_name: "Ada Operator" },
        }),
        succeededJob({
          job_id: "exp_other",
          created_by: { subject: "s2", display_name: "Grace Auditor" },
        }),
      ]),
    );
    renderPanel({ subject: SUPERADMIN });

    expect(await screen.findByText("Ada Operator")).toBeInTheDocument();
    expect(screen.getByText("Grace Auditor")).toBeInTheDocument();
    expect(screen.getByText(/可见范围 全部 Export Job/)).toBeInTheDocument();
  });

  it("keeps Auditor visibility to own jobs and lets Auditor create without an ownership selector", async () => {
    server.use(
      listHandler([
        succeededJob({
          created_by: {
            subject: "auditor-subject",
            display_name: "Avery Auditor",
          },
        }),
      ]),
    );
    renderPanel({ subject: AUDITOR, currentSubject: "auditor-subject" });

    expect(await screen.findByText("Avery Auditor")).toBeInTheDocument();
    expect(screen.getByText(/可见范围 仅本人 Export Job/)).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "创建 Export Job" }),
    ).toBeEnabled();
    expect(
      screen.queryByRole("combobox", { name: /所有权|owner/i }),
    ).not.toBeInTheDocument();
  });

  it("does not let another user's running job block a SuperAdmin's own create hint", async () => {
    server.use(
      listHandler([
        exportJob({
          created_by: { subject: "someone-else", display_name: "Other User" },
        }),
      ]),
    );
    renderPanel({ subject: SUPERADMIN, currentSubject: "super-admin-subject" });

    expect(await screen.findByText("生成中")).toBeInTheDocument();
    expect(
      screen.queryByText(/已有运行中的 Export Job/),
    ).not.toBeInTheDocument();
  });

  it("renders all seven job states with icon, text and distinct tone semantics", async () => {
    const expected: Array<{
      status: ExportJobStatus;
      label: string;
      tone: string;
    }> = [
      { status: "queued", label: "排队中", tone: "info" },
      { status: "running", label: "生成中", tone: "info" },
      { status: "succeeded", label: "已完成", tone: "good" },
      { status: "failed", label: "失败", tone: "critical" },
      { status: "cancelling", label: "取消中", tone: "warning" },
      { status: "cancelled", label: "已取消", tone: "unknown" },
      { status: "expired", label: "已过期", tone: "warning" },
    ];
    server.use(
      listHandler(
        expected.map(({ status }, index) => {
          const result =
            status === "succeeded" || status === "expired"
              ? succeededJob().result
              : null;
          return exportJob({
            job_id: `exp_${status}`,
            status,
            created_by: {
              subject: `s${index}`,
              display_name: `User ${index}`,
            },
            ...(result === null
              ? {}
              : {
                  progress: {
                    processed_rows: 450000,
                    processed_bytes: 12_345_678,
                    total_rows: 450000,
                    percent: 100,
                    indeterminate: false,
                  },
                  result,
                }),
            expires_at:
              status === "expired"
                ? "2026-11-08T12:00:00.000000000Z"
                : status === "succeeded"
                  ? "2026-11-10T12:00:00.000000000Z"
                  : null,
            error:
              status === "failed"
                ? {
                    code: "internal_error",
                    message: "upstream · failed：timeout",
                    details: {},
                  }
                : null,
          });
        }),
      ),
    );
    renderPanel();

    for (const { label, tone } of expected) {
      const pill = await waitFor(() => {
        const result = statusPill(label);
        expect(result, `status pill ${label}`).not.toBeNull();
        return result;
      });
      expect(pill).toHaveAttribute("data-tone", tone);
      expect(pill?.querySelector("svg")).not.toBeNull();
    }

    // Raw backend errors keep their exact `·` and `：`; the surrounding UI
    // copy must not introduce either separator.
    const rawError = await screen.findByText("upstream · failed：timeout");
    expect(rawError).toHaveAttribute("data-raw-evidence", "true");
    expect(screen.getByText("internal_error")).toBeInTheDocument();
    expect(screen.getByText("任务失败")).toBeInTheDocument();
    expectNoBannedUiSymbols(exportPanel());
  });

  it("renders the D card surface and Export Job help triggers", async () => {
    server.use(listHandler([]));
    renderPanel();

    const panel = await screen.findByRole("region", { name: "Export Job" });
    expect(panel).toHaveAttribute("data-slot", "card");
    expect(
      within(panel).getByRole("button", { name: "Export Job 说明" }),
    ).toBeInTheDocument();
    expect(
      within(panel).getByRole("button", { name: "Export Job 状态说明" }),
    ).toBeInTheDocument();
    expect(
      within(panel).getByText(/不会自动退回超大 Blob/),
    ).toBeInTheDocument();
    expectNoBannedUiSymbols(panel);
  });

  it("creates a job with exact UTC strings and never sends an owner", async () => {
    let created = false;
    let postBody: Record<string, unknown> | null = null;
    server.use(
      listHandler(() => (created ? [exportJob({ status: "queued" })] : [])),
      http.post("*/api/v1/admin/export-jobs", async ({ request }) => {
        postBody = (await request.json()) as Record<string, unknown>;
        created = true;
        return HttpResponse.json(exportJob({ status: "queued" }), {
          status: 202,
        });
      }),
    );

    const user = userEvent.setup();
    renderPanel();

    expect(
      await screen.findByText("当前没有可见的 Export Job"),
    ).toBeInTheDocument();
    await user.click(
      screen.getByRole("button", { name: "创建 Export Job" }),
    );

    await waitFor(() => expect(postBody).not.toBeNull());
    expect(postBody).toEqual({
      instrument_id: "binance:BTC/USDT",
      interval: "1m",
      start: "2026-01-01T00:00:00.000Z",
      end: "2026-02-15T00:00:00.000Z",
      format: "csv",
    });
    expect(postBody).not.toHaveProperty("created_by");
    expect(postBody).not.toHaveProperty("owner");
    expect(await screen.findByText("排队中")).toBeInTheDocument();
  });

  it("disables creation while a write is pending and sends exactly one request", async () => {
    let posts = 0;
    let created = false;
    let release!: () => void;
    const pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    server.use(
      listHandler(() => (created ? [exportJob({ status: "queued" })] : [])),
      http.post("*/api/v1/admin/export-jobs", async () => {
        posts += 1;
        await pending;
        created = true;
        return HttpResponse.json(exportJob({ status: "queued" }), {
          status: 202,
        });
      }),
    );

    const user = userEvent.setup();
    renderPanel();
    await screen.findByText("当前没有可见的 Export Job");
    const create = screen.getByRole("button", { name: "创建 Export Job" });
    await user.click(create);

    const pendingButton = await screen.findByRole("button", {
      name: "创建中…",
    });
    expect(pendingButton).toBeDisabled();
    fireEvent.click(pendingButton);
    expect(posts).toBe(1);

    release();
    expect(await screen.findByText("排队中")).toBeInTheDocument();
    expect(posts).toBe(1);
  });

  it("shows the quota error from a 429 quota_exceeded response", async () => {
    server.use(
      listHandler([]),
      http.post("*/api/v1/admin/export-jobs", () =>
        HttpResponse.json(
          {
            error: {
              code: "quota_exceeded",
              message: "too many running jobs",
              details: {
                quota: "running_jobs",
                limit: 1,
                current: 1,
                retry_after_s: 30,
              },
              request_id: "quota-request-id",
            },
          },
          {
            status: 429,
            headers: { "Retry-After": "30" },
          },
        ),
      ),
    );

    const user = userEvent.setup();
    renderPanel();
    await screen.findByText("当前没有可见的 Export Job");
    await user.click(
      screen.getByRole("button", { name: "创建 Export Job" }),
    );

    expect(
      (await screen.findAllByText(/配额已用尽（running_jobs，上限 1）/)).length,
    ).toBeGreaterThan(0);
    expect(screen.getAllByText(/30 秒后重试/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/quota-request-id/).length).toBeGreaterThan(0);
  });

  it("keeps a raw backend create error verbatim with its inline Request ID", async () => {
    server.use(
      listHandler([]),
      http.post("*/api/v1/admin/export-jobs", () =>
        HttpResponse.json(
          {
            error: {
              code: "internal_error",
              message: "upstream · failed：timeout",
              details: {},
              request_id: "raw-create-request-id",
            },
          },
          { status: 500 },
        ),
      ),
    );

    const user = userEvent.setup();
    renderPanel();
    await screen.findByText("当前没有可见的 Export Job");
    await user.click(
      screen.getByRole("button", { name: "创建 Export Job" }),
    );

    const raw = await screen.findByText("upstream · failed：timeout");
    expect(raw).toHaveAttribute("data-raw-evidence", "true");
    expect(screen.getByText("internal_error")).toBeInTheDocument();
    expect(screen.getByText(/raw-create-request-id/)).toBeInTheDocument();
    expect(screen.getByText("Export Job 创建失败")).toBeInTheDocument();
    expectNoBannedUiSymbols(exportPanel());
  });

  it("reports a network failure as an unknown outcome and never re-sends the write", async () => {
    let posts = 0;
    server.use(
      listHandler([]),
      http.post("*/api/v1/admin/export-jobs", () => {
        posts += 1;
        return HttpResponse.error();
      }),
    );

    const user = userEvent.setup();
    renderPanel();
    await screen.findByText("当前没有可见的 Export Job");
    await user.click(
      screen.getByRole("button", { name: "创建 Export Job" }),
    );

    expect(
      await screen.findByText("Export Job 创建结果未知"),
    ).toBeInTheDocument();
    expect(screen.getByText("network_error")).toBeInTheDocument();
    expect(screen.getByText(/未自动重试/)).toBeInTheDocument();
    expect(
      screen.queryByText("Export Job 创建失败"),
    ).not.toBeInTheDocument();
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(posts).toBe(1);
  });

  it("derives expiry from expires_at, treats it as expired and removes the download link", async () => {
    server.use(
      listHandler([
        succeededJob({
          expires_at: "2026-11-09T11:59:59.000000000Z",
        }),
      ]),
    );
    renderPanel();

    expect(await screen.findByText("已过期")).toBeInTheDocument();
    expect(
      screen.queryByRole("link", { name: /原生下载/ }),
    ).not.toBeInTheDocument();
    expect(screen.getByText(/410 export_expired/)).toBeInTheDocument();
  });

  it("never renders indeterminate progress as a fake percentage", async () => {
    server.use(listHandler([exportJob()]));
    renderPanel();

    const raw = await screen.findByText("进度未知");
    expect(raw.closest('[data-slot="status-badge"]')).not.toBeNull();
    const article = raw.closest("[data-job-id]") as HTMLElement;
    expect(article.querySelector("progress")).toBeNull();
    expect(article.textContent).not.toMatch(/\d+%/);
    expect(within(article).getByText(/已处理 1,200 行/)).toBeInTheDocument();
    expect(within(article).getByText(/64 KB/)).toBeInTheDocument();
    expect(
      within(article).getByText(/后端未提供可靠总量/),
    ).toBeInTheDocument();
  });

  it("shows a trusted percentage only when the server provides one", async () => {
    server.use(listHandler([succeededJob()]));
    renderPanel();

    const article = (await screen.findByText("已完成")).closest(
      "[data-job-id]",
    ) as HTMLElement;
    const progress = article.querySelector("progress");
    expect(progress).not.toBeNull();
    expect(progress).toHaveAttribute("max", "100");
    expect(progress).toHaveAttribute("value", "100");
    expect(within(article).getAllByText("100%").length).toBeGreaterThan(0);
    expect(
      within(article).getByText(/已处理 450,000 \/ 450,000 行/),
    ).toBeInTheDocument();
  });

  it("keeps query context, creator, expiry, file size and SHA-256 readable next to the native download", async () => {
    server.use(listHandler([succeededJob()]));
    renderPanel();

    const link = await screen.findByRole("link", {
      name: /原生下载（支持 Range）/,
    });
    expect(link).toHaveAttribute(
      "href",
      "/api/v1/admin/export-jobs/exp_01JABCXYZ/download",
    );
    expect(link.tagName).toBe("A");
    expect(
      screen.getByText(/e3b0c44298fc1c149afbf4c8996fb924abc/),
    ).toBeInTheDocument();

    const article = link.closest("[data-job-id]") as HTMLElement;
    expect(within(article).getByText("binance:BTC/USDT")).toBeInTheDocument();
    expect(within(article).getByText("1m")).toBeInTheDocument();
    expect(within(article).getByText("45 天")).toBeInTheDocument();
    expect(
      within(article).getByText(/2026-01-01T00:00:00.000000000Z/),
    ).toBeInTheDocument();
    expect(
      within(article).getByText(/2026-02-15T00:00:00.000000000Z/),
    ).toBeInTheDocument();
    expect(within(article).getByText("Ada Operator")).toBeInTheDocument();
    expect(within(article).getByText("24 小时过期")).toBeInTheDocument();
    expect(
      within(article).getByText(/2026-11-10T12:00:00.000000000Z/),
    ).toBeInTheDocument();
    expect(within(article).getByText("binance_BTCUSDT_1m.csv")).toBeInTheDocument();
    expect(within(article).getAllByText(/11.77 MB/).length).toBeGreaterThan(0);
    expect(within(article).getAllByText(/450,000 行/).length).toBeGreaterThan(0);
  });

  it("treats cancelling as cooperative and only shows cancelled after a terminal state", async () => {
    let status: ExportJobStatus = "running";
    server.use(
      listHandler(() => [exportJob({ status })]),
      http.post("*/api/v1/admin/export-jobs/:jobId/cancel", () => {
        status = "cancelling";
        return HttpResponse.json(exportJob({ status }), { status: 202 });
      }),
    );

    const user = userEvent.setup();
    renderPanel();
    await screen.findByText("生成中");

    await user.click(screen.getByRole("button", { name: "取消任务" }));

    expect(await screen.findByText("取消中")).toBeInTheDocument();
    expect(
      screen.getByText(/合作式取消进行中；未到终态前不宣称已取消/),
    ).toBeInTheDocument();

    status = "cancelled";
    await user.click(
      screen.getByRole("button", { name: "刷新任务列表" }),
    );
    expect(await screen.findByText("已取消")).toBeInTheDocument();
  });

  it("surfaces a 409 conflict when the job is no longer cancellable", async () => {
    server.use(
      listHandler([exportJob({ status: "running" })]),
      http.post("*/api/v1/admin/export-jobs/:jobId/cancel", () =>
        HttpResponse.json(
          {
            error: {
              code: "conflict",
              message: "job is not cancellable",
              details: { reason: "job_not_cancellable", status: "succeeded" },
              request_id: "conflict-request-id",
            },
          },
          { status: 409 },
        ),
      ),
    );

    const user = userEvent.setup();
    renderPanel();
    await screen.findByText("生成中");
    await user.click(screen.getByRole("button", { name: "取消任务" }));

    expect(
      await screen.findByText(/任务已不在可取消状态/),
    ).toBeInTheDocument();
    expect(screen.getByText(/conflict-request-id/)).toBeInTheDocument();
  });

  it("shows split guidance for >366 days and blocks a single combined job", async () => {
    const splitDecision = classifyExportRange(
      START,
      new Date(START.getTime() + 800 * 24 * 60 * 60 * 1000),
    );
    server.use(listHandler([]));
    renderPanel({ decision: splitDecision });

    expect(
      await screen.findByText(/需要拆分为 3 个 Export Job/),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "创建 Export Job" }),
    ).toBeDisabled();
    expect(screen.getByText(/不能创建单个任务/)).toBeInTheDocument();
    expect(
      screen.getByText(/不会合并为多 Instrument ZIP/),
    ).toBeInTheDocument();
  });

  it("creates a split chunk as an independent Export Job only after explicit confirmation", async () => {
    const splitDecision = classifyExportRange(
      START,
      new Date(START.getTime() + 800 * 24 * 60 * 60 * 1000),
    );
    let created = false;
    let postCount = 0;
    let postBody: Record<string, unknown> | null = null;
    server.use(
      listHandler(() =>
        created ? [exportJob({ status: "queued" })] : [],
      ),
      http.post("*/api/v1/admin/export-jobs", async ({ request }) => {
        postCount += 1;
        postBody = (await request.json()) as Record<string, unknown>;
        created = true;
        return HttpResponse.json(exportJob({ status: "queued" }), {
          status: 202,
        });
      }),
    );

    const user = userEvent.setup();
    renderPanel({ decision: splitDecision });
    await screen.findByText("当前没有可见的 Export Job");

    const chunkButtons = screen.getAllByRole("button", {
      name: /创建第 \d+ 段 Export Job/,
    });
    expect(chunkButtons).toHaveLength(3);

    // Cancel sends no admin write.
    await user.click(chunkButtons[0]!);
    let dialog = screen.getByRole("dialog");
    expect(
      within(dialog).getAllByText(/2026-01-01T00:00:00.000Z/).length,
    ).toBeGreaterThan(0);
    await user.click(within(dialog).getByRole("button", { name: "取消" }));
    expect(postCount).toBe(0);

    // Explicit confirmation creates exactly one chunk, not the combined range.
    await user.click(chunkButtons[0]!);
    dialog = screen.getByRole("dialog");
    await user.click(
      within(dialog).getByLabelText(/我确认只创建所选分段/),
    );
    await user.click(
      within(dialog).getByRole("button", { name: "确认创建该段" }),
    );

    await waitFor(() => expect(postBody).not.toBeNull());
    expect(postBody).toEqual({
      instrument_id: "binance:BTC/USDT",
      interval: "1m",
      start: "2026-01-01T00:00:00.000Z",
      end: "2027-01-02T00:00:00.000Z",
      format: "csv",
    });
    expect(postCount).toBe(1);
    expect(postBody).not.toHaveProperty("instrument_ids");
    expect(postBody).not.toHaveProperty("zip");

    // The refetched running job blocks the remaining chunks.
    expect(
      (await screen.findAllByText(/已有运行中的 Export Job/)).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getByRole("button", { name: "创建第 2 段 Export Job" }),
    ).toBeDisabled();
    expect(
      screen.getByRole("button", { name: "创建第 3 段 Export Job" }),
    ).toBeDisabled();
  });

  it("surfaces a quota error when creating a split chunk", async () => {
    const splitDecision = classifyExportRange(
      START,
      new Date(START.getTime() + 800 * 24 * 60 * 60 * 1000),
    );
    server.use(
      listHandler([]),
      http.post("*/api/v1/admin/export-jobs", () =>
        HttpResponse.json(
          {
            error: {
              code: "quota_exceeded",
              message: "too many running jobs",
              details: {
                quota: "running_jobs",
                limit: 1,
                current: 1,
                retry_after_s: 30,
              },
              request_id: "quota-split-request-id",
            },
          },
          { status: 429, headers: { "Retry-After": "30" } },
        ),
      ),
    );

    const user = userEvent.setup();
    renderPanel({ decision: splitDecision });
    await screen.findByText("当前没有可见的 Export Job");
    await user.click(
      screen.getByRole("button", { name: "创建第 1 段 Export Job" }),
    );
    const dialog = screen.getByRole("dialog");
    await user.click(
      within(dialog).getByLabelText(/我确认只创建所选分段/),
    );
    await user.click(
      within(dialog).getByRole("button", { name: "确认创建该段" }),
    );

    expect(
      (await screen.findAllByText(/配额已用尽（running_jobs，上限 1）/)).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByText(/quota-split-request-id/).length,
    ).toBeGreaterThan(0);
  });

  it("fails closed when the export_job capability is unsupported", async () => {
    let requests = 0;
    server.use(
      listHandler([], () => {
        requests += 1;
      }),
    );
    renderPanel({
      subject: OPERATOR,
      capability: adminCapability(true, { state: "unsupported" }),
    });

    expect(
      screen.getByRole("button", { name: "创建 Export Job" }),
    ).toBeDisabled();
    expect(screen.getAllByText("后端未实现").length).toBeGreaterThan(0);
    expect(requests).toBe(0);
  });

  it("disables job creation and listing when the Admin contract is incompatible", async () => {
    let requests = 0;
    server.use(
      listHandler([], () => {
        requests += 1;
      }),
    );
    renderPanel({ subject: OPERATOR, capability: adminCapability(false) });

    expect(
      screen.getByRole("button", { name: "创建 Export Job" }),
    ).toBeDisabled();
    expect(screen.getAllByText("Admin API 版本不兼容").length).toBeGreaterThan(
      0,
    );
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(requests).toBe(0);
  });

  it("labels the panel as a contract target", () => {
    server.use(listHandler([]));
    renderPanel();
    expect(
      screen.getAllByText(/Admin API 契约目标/).length,
    ).toBeGreaterThan(0);
  });
});

describe("Export Job Retry-After gate", () => {
  afterEach(() => {
    resetRetryAfter();
    vi.useRealTimers();
  });

  it("disables creation after a 429 Retry-After, sends no write, and re-enables after expiry", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let posts = 0;
    server.use(
      http.get("*/api/v1/admin/export-jobs", () =>
        HttpResponse.json(
          {
            error: {
              code: "rate_limited",
              message: "too many requests",
              details: { retry_after_s: 30 },
              request_id: "list-rate-limited",
            },
          },
          { status: 429, headers: { "Retry-After": "30" } },
        ),
      ),
      http.post("*/api/v1/admin/export-jobs", () => {
        posts += 1;
        return HttpResponse.json(exportJob({ status: "queued" }), {
          status: 202,
        });
      }),
    );

    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    renderPanel();

    const create = screen.getByRole("button", { name: "创建 Export Job" });
    await waitFor(() =>
      expect(
        screen.getAllByText(/限流中，请在 30 秒后重试/).length,
      ).toBeGreaterThan(0),
    );
    expect(create).toBeDisabled();

    await user.click(create);
    expect(posts).toBe(0);

    act(() => {
      vi.advanceTimersByTime(30_000);
    });
    await waitFor(() => expect(create).toBeEnabled());
    expect(screen.queryByText(/限流中，请在/)).not.toBeInTheDocument();
  });
});
