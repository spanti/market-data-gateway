import {
  COLLECTOR_OPS_BASE,
  REPLAY_OPS_BASE,
  opsClient,
} from "@/api/http/ops-client";
import { apiClient } from "@/api/http/client";
import type { BarInterval } from "@/lib/time-range";

/**
 * Current Contract ops/dataset types. `/status`, `/readyz` and `/healthz`
 * are per-Service-Role snapshots; `/api/v1/status` is Dataset-only.
 */

export interface DatasetIntervalStatus {
  last_flushed_ts: string | null;
  active_file_count: number;
  median_file_size_bytes: number | null;
}

export interface DatasetStatus {
  dataset: "bars";
  schema_version: number;
  last_flushed_ts: string | null;
  active_file_count: number;
  median_file_size_bytes: number | null;
  intervals: Record<string, DatasetIntervalStatus>;
}

export interface FeedFreshnessEntry {
  feed: string;
  instrument_id: string;
  interval: BarInterval;
  last_event_ts: string | null;
  age_s: number | null;
  threshold_s: number | null;
  stale: boolean;
}

export interface UnrecoverableBackfillGapEvent {
  instrument_id: string;
  interval: BarInterval;
  from_ts: string | null;
  to_ts: string | null;
  reason: string;
  recorded_at: string;
}

export interface CollectorHealth {
  stale: FeedFreshnessEntry[];
  stale_count: number;
  tracked: FeedFreshnessEntry[];
  gap_unrecoverable: UnrecoverableBackfillGapEvent[];
  gap_unrecoverable_count: number;
}

export interface CollectorWriteSink {
  mode: string;
  queue_depth: number;
  queue_capacity: number;
  pending_bytes: number;
  dropped_bars: number;
  lag_bars: number;
  dropped_identities: number;
  flushes: number;
  flush_failures: number;
  persisted_bars: number;
  last_flush_ts: string | null;
  last_flush_duration_s: number | null;
}

export interface CollectorQuarantine {
  schema_version: number;
  root: string;
  retention_days: number;
  records_written: number;
  write_failures: number;
  last_write_ts: string | null;
  active_file_count: number;
  active_bytes: number;
  gc_runs: number;
  last_gc: Record<string, unknown> | null;
  records_by_source_stage: Record<string, number>;
  records_by_reason_code: Record<string, number>;
  records_by_stage_and_reason: Array<{
    source_stage: string;
    reason_code: string;
    count: number;
  }>;
}

export interface CollectorStatus {
  service: "collector";
  role: "collector";
  ready: boolean;
  uptime_s: number;
  feed: string | null;
  last_ingest_ts: string | null;
  last_bar_ts: string | null;
  bars_ingested: number;
  bars_written: number;
  stale_count: number;
  gap_unrecoverable_count: number;
  write_queue_depth: number;
  write_dropped_bars: number;
  write_lag_bars: number;
  write_last_flush_ts: string | null;
  write_last_flush_duration_s: number | null;
  write_sink: CollectorWriteSink;
  quarantine: CollectorQuarantine;
  health: CollectorHealth;
}

export interface ApiStatus extends DatasetStatus {
  service: "api";
  role: "api";
  uptime_s: number;
  http: { host: string; port: number | null };
  zmq: {
    publish_endpoint: string | null;
    collector_endpoint: string;
  };
  messages_received: number;
  messages_forwarded: number;
  collector: {
    ready: boolean;
    last_bar_ts: string | null;
    status: CollectorStatus | null;
  };
}

export interface ReplaySessionSummary {
  session_id: string;
  status: "created" | "running" | "finished" | "stopped";
  bars_sent: number;
}

export interface ReplayStatus {
  service: "replay";
  role: "replay";
  uptime_s: number;
  control_endpoint: string | null;
  publish_endpoint: string | null;
  active_sessions: string[];
  sessions: ReplaySessionSummary[];
  instrument_snapshot: {
    status: "loaded" | "missing";
    loaded: boolean;
    snapshot: string | null;
    count: number;
  };
}

export interface ReadyPayload {
  ready: boolean;
  reason: string;
  collector_ready?: boolean;
  service?: string;
}

export interface HealthPayload {
  status: "ok";
  service: string;
}

/**
 * `/readyz` uses 200 for ready and 503 for not-ready with a small JSON body.
 * A 503 is real readiness data, not a transport failure, so it must reach the
 * UI instead of being rejected as an error.
 */
function isReadinessResponse(status: number): boolean {
  return status === 200 || status === 503;
}

export async function fetchDatasetStatus(
  signal?: AbortSignal,
): Promise<DatasetStatus> {
  const response = await apiClient.get<DatasetStatus>("/api/v1/status", {
    signal,
  });
  return response.data;
}

export async function fetchApiStatus(signal?: AbortSignal): Promise<ApiStatus> {
  const response = await apiClient.get<ApiStatus>("/status", { signal });
  return response.data;
}

export async function fetchApiReady(signal?: AbortSignal): Promise<ReadyPayload> {
  const response = await apiClient.get<ReadyPayload>("/readyz", {
    signal,
    validateStatus: isReadinessResponse,
  });
  return response.data;
}

export async function fetchApiHealth(
  signal?: AbortSignal,
): Promise<HealthPayload> {
  const response = await apiClient.get<HealthPayload>("/healthz", { signal });
  return response.data;
}

export async function fetchCollectorStatus(
  signal?: AbortSignal,
): Promise<CollectorStatus> {
  const response = await opsClient.get<CollectorStatus>(
    `${COLLECTOR_OPS_BASE}/status`,
    { signal },
  );
  return response.data;
}

export async function fetchCollectorReady(
  signal?: AbortSignal,
): Promise<ReadyPayload> {
  const response = await opsClient.get<ReadyPayload>(
    `${COLLECTOR_OPS_BASE}/readyz`,
    { signal, validateStatus: isReadinessResponse },
  );
  return response.data;
}

export async function fetchReplayReady(
  signal?: AbortSignal,
): Promise<ReadyPayload> {
  const response = await opsClient.get<ReadyPayload>(
    `${REPLAY_OPS_BASE}/readyz`,
    { signal, validateStatus: isReadinessResponse },
  );
  return response.data;
}

export async function fetchReplayStatus(
  signal?: AbortSignal,
): Promise<ReplayStatus> {
  const response = await opsClient.get<ReplayStatus>(
    `${REPLAY_OPS_BASE}/status`,
    { signal },
  );
  return response.data;
}
