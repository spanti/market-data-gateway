import { ADMIN_PATHS } from "@/api/endpoints/admin";
import type {
  AdminListResponse,
  ExportJob,
  ExportJobCreateBody,
  ExportJobStatus,
} from "@/api/endpoints/admin";
import { apiClient } from "@/api/http/client";

/**
 * CONTRACT TARGET — NOT IMPLEMENTED BY THE CURRENT BACKEND.
 *
 * These calls mirror `admin-api-contract.md` §6 and can only be exercised
 * against MSW fixtures. The current public API has a synchronous
 * `/api/v1/bars/export` but no job, progress, ownership, quota, expiry or
 * Range support. Never report this module as integrated.
 */

export interface ExportJobListOptions {
  cursor?: string | null;
  limit?: number;
}

export async function listExportJobs(
  options: ExportJobListOptions = {},
  signal?: AbortSignal,
): Promise<AdminListResponse<ExportJob>> {
  const response = await apiClient.get<AdminListResponse<ExportJob>>(
    ADMIN_PATHS.exportJobs,
    {
      params: {
        ...(options.cursor ? { cursor: options.cursor } : {}),
        ...(options.limit !== undefined ? { limit: options.limit } : {}),
      },
      signal,
    },
  );
  return response.data;
}

export async function getExportJob(
  jobId: string,
  signal?: AbortSignal,
): Promise<ExportJob> {
  const response = await apiClient.get<ExportJob>(
    `${ADMIN_PATHS.exportJobs}/${encodeURIComponent(jobId)}`,
    { signal },
  );
  return response.data;
}

export async function createExportJob(
  body: ExportJobCreateBody,
  signal?: AbortSignal,
): Promise<ExportJob> {
  const response = await apiClient.post<ExportJob>(
    ADMIN_PATHS.exportJobs,
    body,
    { signal },
  );
  return response.data;
}

export async function cancelExportJob(
  jobId: string,
  signal?: AbortSignal,
): Promise<ExportJob> {
  const response = await apiClient.post<ExportJob>(
    `${ADMIN_PATHS.exportJobs}/${encodeURIComponent(jobId)}/cancel`,
    undefined,
    { signal },
  );
  return response.data;
}

export const ACTIVE_EXPORT_JOB_STATUSES: ReadonlySet<ExportJobStatus> = new Set([
  "queued",
  "running",
  "cancelling",
]);

export function isActiveExportJob(status: ExportJobStatus): boolean {
  return ACTIVE_EXPORT_JOB_STATUSES.has(status);
}

export function isExportJobExpired(job: ExportJob, now: Date): boolean {
  if (job.status === "expired") {
    return true;
  }
  if (job.status !== "succeeded" || job.expires_at === null) {
    return false;
  }
  const expires = Date.parse(job.expires_at);
  return Number.isFinite(expires) && expires <= now.getTime();
}

/**
 * `expired` is derived: the server may still report `succeeded` after the
 * 24-hour window until it cleans up. The UI must not offer a download then.
 */
export function effectiveExportJobStatus(
  job: ExportJob,
  now: Date,
): ExportJobStatus {
  return isExportJobExpired(job, now) ? "expired" : job.status;
}

/**
 * Only a same-origin relative download path is accepted. The contract promises
 * an opaque job ID; an absolute or scheme-relative URL is a contract failure
 * and falls back to the canonical job path.
 */
export function exportJobDownloadPath(job: ExportJob): string {
  const candidate = job.download_url;
  if (
    typeof candidate === "string" &&
    candidate.startsWith("/") &&
    !candidate.startsWith("//") &&
    !candidate.includes("\\")
  ) {
    return candidate;
  }
  return `${ADMIN_PATHS.exportJobs}/${encodeURIComponent(job.job_id)}/download`;
}

export { formatBytes } from "@/lib/format";
