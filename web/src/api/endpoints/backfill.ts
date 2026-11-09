import { apiClient } from "@/api/http/client";
import {
  ADMIN_PATHS,
  type AdminListResponse,
  type BackfillGap,
  type BackfillJob,
  type BackfillJobCreateBody,
  type BackfillJobStatus,
} from "@/api/endpoints/admin";

/**
 * CONTRACT TARGET — NOT IMPLEMENTED BY THE CURRENT BACKEND.
 *
 * Adapters for `E:/total-project/mdgw-notes/operator-console/admin-api-contract.md`
 * §7. Until the Admin API exists these functions can only be exercised with
 * MSW fixtures; do not report them as integrated features.
 */

export interface BackfillJobQuery {
  status?: BackfillJobStatus;
  instrument_id?: string;
  interval?: string;
  cursor?: string;
  limit?: number;
}

export interface BackfillGapQuery {
  status?: "open" | "resolved";
  feed?: string;
  instrument_id?: string;
  interval?: string;
  reason?: string;
  discovered_from?: string;
  discovered_to?: string;
  resolved_from?: string;
  resolved_to?: string;
  cursor?: string;
  limit?: number;
}

export interface BackfillJobActionBody {
  reason: string;
}

export interface AdminWriteAuth {
  /** Session CSRF token held in memory; never persisted. */
  csrfToken?: string | null;
  /** One key per deliberate user attempt; the UI never auto-replays. */
  idempotencyKey?: string | null;
}

function buildQueryString(
  params: Record<string, string | number | null | undefined>,
): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined) {
      continue;
    }
    const text = String(value).trim();
    if (text.length > 0) {
      search.set(key, text);
    }
  }
  const query = search.toString();
  return query ? `?${query}` : "";
}

function writeHeaders(auth: AdminWriteAuth = {}): Record<string, string> {
  const headers: Record<string, string> = {};
  if (auth.csrfToken) {
    headers["X-CSRF-Token"] = auth.csrfToken;
  }
  if (auth.idempotencyKey) {
    headers["Idempotency-Key"] = auth.idempotencyKey;
  }
  return headers;
}

export function newIdempotencyKey(): string {
  const webCrypto = globalThis.crypto;
  if (webCrypto && typeof webCrypto.randomUUID === "function") {
    return webCrypto.randomUUID();
  }
  return `idem_${Date.now().toString(36)}_${Math.random()
    .toString(36)
    .slice(2)}`;
}

export async function fetchBackfillJobs(
  query: BackfillJobQuery = {},
  signal?: AbortSignal,
): Promise<AdminListResponse<BackfillJob>> {
  const suffix = buildQueryString({
    status: query.status,
    instrument_id: query.instrument_id,
    interval: query.interval,
    cursor: query.cursor,
    limit: query.limit,
  });
  const response = await apiClient.get<AdminListResponse<BackfillJob>>(
    `${ADMIN_PATHS.backfillJobs}${suffix}`,
    { signal },
  );
  return response.data;
}

export async function fetchBackfillJob(
  jobId: string,
  signal?: AbortSignal,
): Promise<BackfillJob> {
  const response = await apiClient.get<BackfillJob>(
    `${ADMIN_PATHS.backfillJobs}/${encodeURIComponent(jobId)}`,
    { signal },
  );
  return response.data;
}

export async function createBackfillJob(
  body: BackfillJobCreateBody,
  auth: AdminWriteAuth = {},
): Promise<BackfillJob> {
  const response = await apiClient.post<BackfillJob>(
    ADMIN_PATHS.backfillJobs,
    body,
    { headers: writeHeaders(auth) },
  );
  return response.data;
}

/**
 * Cooperative cancellation. A 202 means the request was accepted, not that
 * the Job has finished cancelling; already-written Revisions are never
 * rolled back. A response body is optional.
 */
export async function cancelBackfillJob(
  jobId: string,
  body: BackfillJobActionBody,
  auth: AdminWriteAuth = {},
): Promise<BackfillJob | null> {
  const response = await apiClient.post<BackfillJob | null>(
    `${ADMIN_PATHS.backfillJobs}/${encodeURIComponent(jobId)}/cancel`,
    body,
    { headers: writeHeaders(auth) },
  );
  return response.data && typeof response.data === "object"
    ? response.data
    : null;
}

/**
 * Retry always creates a NEW Job linked with `retry_of_job_id`; the old Job
 * is never mutated.
 */
export async function retryBackfillJob(
  jobId: string,
  body: BackfillJobActionBody,
  auth: AdminWriteAuth = {},
): Promise<BackfillJob> {
  const response = await apiClient.post<BackfillJob>(
    `${ADMIN_PATHS.backfillJobs}/${encodeURIComponent(jobId)}/retry`,
    body,
    { headers: writeHeaders(auth) },
  );
  return response.data;
}

export async function fetchBackfillGaps(
  query: BackfillGapQuery = {},
  signal?: AbortSignal,
): Promise<AdminListResponse<BackfillGap>> {
  const suffix = buildQueryString({
    status: query.status,
    feed: query.feed,
    instrument_id: query.instrument_id,
    interval: query.interval,
    reason: query.reason,
    discovered_from: query.discovered_from,
    discovered_to: query.discovered_to,
    resolved_from: query.resolved_from,
    resolved_to: query.resolved_to,
    cursor: query.cursor,
    limit: query.limit,
  });
  const response = await apiClient.get<AdminListResponse<BackfillGap>>(
    `${ADMIN_PATHS.backfillGaps}${suffix}`,
    { signal },
  );
  return response.data;
}

export async function fetchBackfillGap(
  gapId: string,
  signal?: AbortSignal,
): Promise<BackfillGap> {
  const response = await apiClient.get<BackfillGap>(
    `${ADMIN_PATHS.backfillGaps}/${encodeURIComponent(gapId)}`,
    { signal },
  );
  return response.data;
}
