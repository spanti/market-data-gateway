import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import {
  cancelBackfillJob,
  createBackfillJob,
  fetchBackfillGaps,
  fetchBackfillJobs,
  retryBackfillJob,
} from "@/api/endpoints/backfill";
import { server } from "@/test/server";

/**
 * CONTRACT TARGET — NOT IMPLEMENTED BY THE CURRENT BACKEND.
 *
 * These tests exercise only the Admin API contract
 * (`E:/total-project/mdgw-notes/operator-console/admin-api-contract.md` §7).
 * They must not be reported as real backend integration.
 */

const job = {
  job_id: "bf_01JABCXYZ",
  status: "queued",
  instrument_id: "binance:BTC/USDT",
  interval: "1h",
  start: "2026-09-01T00:00:00.000000000Z",
  end: "2026-09-02T00:00:00.000000000Z",
  plan: {
    mode: "one_minute_plus_aggregation",
    source_intervals: ["1m"],
    target_interval: "1h",
    description: "补采 1m + 内部 Bar Aggregation",
  },
  current_range: null,
  counters: {
    scanned_bars: 0,
    gap_bars: 0,
    backfilled_bars: 0,
    written_bars: 0,
    rejected_bars: 0,
  },
  requested_by: {
    subject: "oidc-subject-123",
    display_name: "Ada Operator",
    access_role: "Operator",
  },
  reason: "recover exchange outage",
  retry_of_job_id: null,
  created_at: "2026-11-09T12:00:00.000000000Z",
  started_at: null,
  finished_at: null,
  error: null,
  resolved_gap_ids: [],
};

const gap = {
  gap_id: "gap_01JABCXYZ",
  status: "open",
  feed: "binance_spot",
  instrument_id: "binance:BTC/USDT",
  interval: "1m",
  start: "2026-09-01T00:00:00.000000000Z",
  end: "2026-09-01T01:00:00.000000000Z",
  reason: "history_unsupported",
  first_discovered_at: "2026-09-01T01:05:00.000000000Z",
  last_discovered_at: "2026-11-09T12:00:00.000000000Z",
  occurrence_count: 3,
  resolved_at: null,
  resolved_by_job_id: null,
  related_job_ids: [],
  created_at: "2026-09-01T01:05:00.000000000Z",
  updated_at: "2026-11-09T12:00:00.000000000Z",
};

describe("CONTRACT TARGET — Backfill admin endpoints (backend absent)", () => {
  it("sends gap filters as server-side query parameters and reads the cursor page", async () => {
    let seen: URL | null = null;
    server.use(
      http.get("*/api/v1/admin/backfill-gaps", ({ request }) => {
        seen = new URL(request.url);
        return HttpResponse.json({
          items: [gap],
          page: { next_cursor: "cursor_2", has_more: true },
        });
      }),
    );

    const page = await fetchBackfillGaps({
      status: "open",
      feed: "binance_spot",
      instrument_id: "binance:BTC/USDT",
      interval: "1m",
      reason: "history_unsupported",
      cursor: "cursor_1",
      limit: 50,
    });

    expect(seen).not.toBeNull();
    expect((seen as unknown as URL).pathname).toBe(
      "/api/v1/admin/backfill-gaps",
    );
    const params = (seen as unknown as URL).searchParams;
    expect(params.get("status")).toBe("open");
    expect(params.get("feed")).toBe("binance_spot");
    expect(params.get("instrument_id")).toBe("binance:BTC/USDT");
    expect(params.get("interval")).toBe("1m");
    expect(params.get("reason")).toBe("history_unsupported");
    expect(params.get("cursor")).toBe("cursor_1");
    expect(params.get("limit")).toBe("50");
    expect(page.items[0]?.gap_id).toBe("gap_01JABCXYZ");
    expect(page.page.next_cursor).toBe("cursor_2");
    expect(page.page.has_more).toBe(true);
  });

  it("lists jobs with server-side status and instrument filters", async () => {
    let seen: URL | null = null;
    server.use(
      http.get("*/api/v1/admin/backfill-jobs", ({ request }) => {
        seen = new URL(request.url);
        return HttpResponse.json({
          items: [job],
          page: { next_cursor: null, has_more: false },
        });
      }),
    );

    const page = await fetchBackfillJobs({
      status: "queued",
      instrument_id: "binance:BTC/USDT",
    });

    expect((seen as unknown as URL).searchParams.get("status")).toBe("queued");
    expect(
      (seen as unknown as URL).searchParams.get("instrument_id"),
    ).toBe("binance:BTC/USDT");
    expect(page.items[0]?.job_id).toBe("bf_01JABCXYZ");
  });

  it("creates a job with confirm=true, CSRF and Idempotency-Key headers", async () => {
    let body: unknown = null;
    let headers: Headers | null = null;
    server.use(
      http.post("*/api/v1/admin/backfill-jobs", async ({ request }) => {
        body = await request.json();
        headers = request.headers;
        return HttpResponse.json(job, { status: 202 });
      }),
    );

    const created = await createBackfillJob(
      {
        instrument_id: "binance:BTC/USDT",
        interval: "1h",
        start: "2026-09-01T00:00:00.000Z",
        end: "2026-09-02T00:00:00.000Z",
        reason: "recover exchange outage",
        confirm: true,
      },
      { csrfToken: "csrf-token-1", idempotencyKey: "idem-1" },
    );

    expect(body).toEqual({
      instrument_id: "binance:BTC/USDT",
      interval: "1h",
      start: "2026-09-01T00:00:00.000Z",
      end: "2026-09-02T00:00:00.000Z",
      reason: "recover exchange outage",
      confirm: true,
    });
    expect((headers as unknown as Headers).get("x-csrf-token")).toBe(
      "csrf-token-1",
    );
    expect((headers as unknown as Headers).get("idempotency-key")).toBe(
      "idem-1",
    );
    expect(created.job_id).toBe("bf_01JABCXYZ");
  });

  it("surfaces a 409 active-job conflict with details, never as success", async () => {
    server.use(
      http.post("*/api/v1/admin/backfill-jobs", () =>
        HttpResponse.json(
          {
            error: {
              code: "conflict",
              message: "an active backfill job already exists",
              details: { reason: "active_job", active_job_id: "bf_active_1" },
              request_id: "req_conflict_1",
            },
          },
          { status: 409 },
        ),
      ),
    );

    await expect(
      createBackfillJob({
        instrument_id: "binance:BTC/USDT",
        interval: "1m",
        start: "2026-09-01T00:00:00.000Z",
        end: "2026-09-02T00:00:00.000Z",
        reason: "retry",
        confirm: true,
      }),
    ).rejects.toMatchObject({
      code: "conflict",
      details: { reason: "active_job", active_job_id: "bf_active_1" },
      requestId: "req_conflict_1",
    });
  });

  it("cancels and retries through distinct POST endpoints", async () => {
    const paths: string[] = [];
    server.use(
      http.post("*/api/v1/admin/backfill-jobs/:jobId/cancel", ({ params }) => {
        paths.push(`cancel:${String(params.jobId)}`);
        return HttpResponse.json(
          { ...job, status: "cancelling" },
          { status: 202 },
        );
      }),
      http.post("*/api/v1/admin/backfill-jobs/:jobId/retry", ({ params }) => {
        paths.push(`retry:${String(params.jobId)}`);
        return HttpResponse.json(
          { ...job, job_id: "bf_retry_1", retry_of_job_id: "bf_old_1" },
          { status: 202 },
        );
      }),
    );

    const cancelled = await cancelBackfillJob(
      "bf_run_1",
      { reason: "no longer needed" },
      { csrfToken: "csrf-token-1", idempotencyKey: "idem-cancel" },
    );
    const retried = await retryBackfillJob(
      "bf_old_1",
      { reason: "retry after failure" },
      { csrfToken: "csrf-token-1", idempotencyKey: "idem-retry" },
    );

    expect(paths).toEqual(["cancel:bf_run_1", "retry:bf_old_1"]);
    expect(cancelled?.status).toBe("cancelling");
    expect(retried.job_id).toBe("bf_retry_1");
    expect(retried.retry_of_job_id).toBe("bf_old_1");
  });
});
