/**
 * CONTRACT TARGET TESTS — NOT REAL BACKEND INTEGRATION.
 *
 * The current backend has no `/api/v1/admin/export-jobs`. Every Admin flow in
 * this file is exercised against MSW fixtures derived from
 * `admin-api-contract.md` §6/§11/§12. These tests do not prove that an Admin
 * API exists or behaves this way.
 */

import { QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import type { ComponentProps } from "react";
import { describe, expect, it } from "vitest";
import type { ExportJob, ExportJobStatus } from "@/api/endpoints/admin";
import { ExportJobPanel } from "@/components/export-job-panel";
import { createQueryClient } from "@/api/query-client";
import { classifyExportRange } from "@/lib/export-policy";
import { server } from "@/test/server";
import type { PermissionSubject } from "@/runtime/permissions";

const START = new Date("2026-01-01T00:00:00.000Z");
const END_45_DAYS = new Date("2026-02-15T00:00:00.000Z");
const NOW = new Date("2026-11-09T12:00:00.000Z");

const OPERATOR: PermissionSubject = { kind: "role", role: "Operator" };
const SUPERADMIN: PermissionSubject = { kind: "role", role: "SuperAdmin" };
const ANONYMOUS: PermissionSubject = { kind: "anonymous" };

const ENABLED_CAPABILITY = {
  state: "enabled" as const,
  formats: ["csv"],
  min_days: 31,
  max_days: 366,
  retention_s: 86400,
  max_running_per_user: 1,
};

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
        <ExportJobPanel
          query={{
            instrumentId: "binance:BTC/USDT",
            interval: "1m",
            start: START,
            end: END_45_DAYS,
          }}
          decision={decision}
          subject={OPERATOR}
          capability={ENABLED_CAPABILITY}
          currentSubject="oidc-subject-123"
          now={NOW}
          {...props}
        />
      </QueryClientProvider>,
    ),
  };
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
    expect(screen.getByText(/可见范围：全部 Export Job/)).toBeInTheDocument();
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

  it("renders all seven required job states with icon and text", async () => {
    const statuses: ExportJobStatus[] = [
      "queued",
      "running",
      "succeeded",
      "failed",
      "cancelling",
      "cancelled",
      "expired",
    ];
    server.use(
      listHandler(
        statuses.map((status, index) =>
          exportJob({
            job_id: `exp_${status}`,
            status,
            created_by: {
              subject: `s${index}`,
              display_name: `User ${index}`,
            },
            expires_at:
              status === "expired"
                ? "2026-11-08T12:00:00.000000000Z"
                : null,
            error:
              status === "failed"
                ? {
                    code: "internal_error",
                    message: "boom",
                    details: {},
                  }
                : null,
          }),
        ),
      ),
    );
    renderPanel();

    for (const label of [
      "排队中",
      "生成中",
      "已完成",
      "失败",
      "取消中",
      "已取消",
      "已过期",
    ]) {
      expect(await screen.findByText(label)).toBeInTheDocument();
    }
    expect(screen.getByText("失败：internal_error · boom")).toBeInTheDocument();
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
      await screen.findByText("当前没有可见的 Export Job。"),
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
    await screen.findByText("当前没有可见的 Export Job。");
    await user.click(
      screen.getByRole("button", { name: "创建 Export Job" }),
    );

    expect(
      (await screen.findAllByText(/配额已用尽（running_jobs，上限 1）/)).length,
    ).toBeGreaterThan(0);
    expect(screen.getAllByText(/30 秒后重试/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/quota-request-id/).length).toBeGreaterThan(0);
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

  it("uses a native same-origin anchor for downloads and never fetches the file as a Blob", async () => {
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
    expect(screen.getAllByText(/11.77 MB/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/450,000 行/).length).toBeGreaterThan(0);
    expect(screen.getByText(/2026-11-10T12:00:00.000000000Z/)).toBeInTheDocument();
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

  it("shows split guidance for >366 days and creates nothing", async () => {
    const splitDecision = classifyExportRange(
      START,
      new Date(START.getTime() + 800 * 24 * 60 * 60 * 1000),
    );
    server.use(listHandler([]));
    renderPanel({ decision: splitDecision });

    expect(
      screen.getByText(/需要拆分为 3 个 Export Job/),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "创建 Export Job" }),
    ).toBeDisabled();
    expect(screen.getByText(/不能创建单个任务/)).toBeInTheDocument();
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
      capability: { state: "unsupported" },
    });

    expect(
      screen.getByRole("button", { name: "创建 Export Job" }),
    ).toBeDisabled();
    expect(screen.getAllByText("后端未实现").length).toBeGreaterThan(0);
    expect(requests).toBe(0);
  });

  it("labels the panel as a contract target", () => {
    server.use(listHandler([]));
    renderPanel();
    expect(
      screen.getByText(/Admin API 契约目标/),
    ).toBeInTheDocument();
  });
});
