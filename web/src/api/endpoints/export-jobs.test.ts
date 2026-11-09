import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import {
  createExportJob,
  effectiveExportJobStatus,
  exportJobDownloadPath,
  formatBytes,
  getExportJob,
  isActiveExportJob,
  isExportJobExpired,
  listExportJobs,
} from "@/api/endpoints/export-jobs";
import type { ExportJob } from "@/api/endpoints/admin";
import { server } from "@/test/server";

function job(overrides: Partial<ExportJob> = {}): ExportJob {
  return {
    job_id: "exp_01JABCXYZ",
    status: "succeeded",
    instrument_id: "binance:BTC/USDT",
    interval: "1m",
    start: "2026-01-01T00:00:00.000000000Z",
    end: "2026-02-15T00:00:00.000000000Z",
    range_days: 45,
    format: "csv",
    created_at: "2026-11-09T12:00:00.000000000Z",
    created_by: { subject: "oidc-subject-123", display_name: "Ada Operator" },
    progress: {
      processed_rows: 1,
      processed_bytes: 10,
      total_rows: 1,
      percent: 100,
      indeterminate: false,
    },
    result: {
      file_name: "out.csv",
      content_type: "text/csv; charset=utf-8",
      size_bytes: 10,
      sha256: "abc",
      row_count: 1,
    },
    error: null,
    cancelled_at: null,
    expires_at: "2026-11-10T12:00:00.000000000Z",
    download_url: "/api/v1/admin/export-jobs/exp_01JABCXYZ/download",
    ...overrides,
  };
}

describe("Export Job state helpers (contract target)", () => {
  it("treats queued, running and cancelling as one running slot", () => {
    expect(isActiveExportJob("queued")).toBe(true);
    expect(isActiveExportJob("running")).toBe(true);
    expect(isActiveExportJob("cancelling")).toBe(true);
    expect(isActiveExportJob("succeeded")).toBe(false);
    expect(isActiveExportJob("cancelled")).toBe(false);
  });

  it("derives expired from expires_at even when the server still says succeeded", () => {
    const now = new Date("2026-11-10T12:00:00.001Z");
    expect(isExportJobExpired(job(), now)).toBe(true);
    expect(effectiveExportJobStatus(job(), now)).toBe("expired");
    expect(
      effectiveExportJobStatus(job(), new Date("2026-11-10T11:59:59Z")),
    ).toBe("succeeded");
    expect(isExportJobExpired(job({ status: "running" }), now)).toBe(false);
  });

  it("accepts only same-origin relative download paths", () => {
    expect(exportJobDownloadPath(job())).toBe(
      "/api/v1/admin/export-jobs/exp_01JABCXYZ/download",
    );
    expect(
      exportJobDownloadPath(
        job({ download_url: "https://evil.example/file.csv" }),
      ),
    ).toBe("/api/v1/admin/export-jobs/exp_01JABCXYZ/download");
    expect(
      exportJobDownloadPath(job({ download_url: "//evil.example/file.csv" })),
    ).toBe("/api/v1/admin/export-jobs/exp_01JABCXYZ/download");
    expect(exportJobDownloadPath(job({ download_url: null }))).toBe(
      "/api/v1/admin/export-jobs/exp_01JABCXYZ/download",
    );
  });

  it("formats byte sizes without inventing a value", () => {
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(1536)).toBe("1.5 KB");
    expect(formatBytes(12_345_678)).toBe("11.77 MB");
    expect(formatBytes(null)).toBe("—");
  });
});

describe("CONTRACT TARGET (Admin API, MSW only — not integrated)", () => {
  it("creates, lists and reads a single Export Job through the documented paths", async () => {
    server.use(
      http.post("*/api/v1/admin/export-jobs", async ({ request }) => {
        const body = (await request.json()) as Record<string, unknown>;
        expect(body).toMatchObject({
          instrument_id: "binance:BTC/USDT",
          interval: "1m",
          format: "csv",
        });
        return HttpResponse.json(job({ status: "queued" }), { status: 202 });
      }),
      http.get("*/api/v1/admin/export-jobs", ({ request }) => {
        const url = new URL(request.url);
        expect(url.searchParams.get("limit")).toBe("50");
        expect(url.searchParams.get("cursor")).toBe("cursor-1");
        return HttpResponse.json({
          items: [job()],
          page: { next_cursor: null, has_more: false },
        });
      }),
      http.get("*/api/v1/admin/export-jobs/:jobId", ({ params }) => {
        expect(params.jobId).toBe("exp_01JABCXYZ");
        return HttpResponse.json(job());
      }),
    );

    const created = await createExportJob({
      instrument_id: "binance:BTC/USDT",
      interval: "1m",
      start: "2026-01-01T00:00:00.000000000Z",
      end: "2026-02-15T00:00:00.000000000Z",
      format: "csv",
    });
    expect(created.status).toBe("queued");

    const listed = await listExportJobs({ cursor: "cursor-1", limit: 50 });
    expect(listed.items).toHaveLength(1);

    const fetched = await getExportJob("exp_01JABCXYZ");
    expect(fetched.job_id).toBe("exp_01JABCXYZ");
    expect(fetched.created_by.display_name).toBe("Ada Operator");
  });
});
