import type { CapabilityDocument } from "@/api/capabilities";
import type { AccessRole } from "@/runtime/roles";

/**
 * CONTRACT TARGET — NOT IMPLEMENTED BY THE CURRENT BACKEND.
 *
 * Types mirror `E:/total-project/mdgw-notes/operator-console/admin-api-contract.md`.
 * Everything here can only be contract-tested against fixtures/mocks until
 * the Admin API exists. Do not report these as integrated features.
 */

export const ADMIN_PATHS = {
  capabilities: "/api/v1/admin/capabilities",
  login: "/api/v1/admin/auth/login",
  callback: "/api/v1/admin/auth/callback",
  logout: "/api/v1/admin/auth/logout",
  session: "/api/v1/admin/auth/session",
  refresh: "/api/v1/admin/auth/refresh",
  feedSubscriptions: "/api/v1/admin/feed-subscriptions",
  flowControl: "/api/v1/admin/flow-control",
  serviceInstances: "/api/v1/admin/service-instances",
  operations: "/api/v1/admin/operations",
  exportJobs: "/api/v1/admin/export-jobs",
  backfillJobs: "/api/v1/admin/backfill-jobs",
  backfillGaps: "/api/v1/admin/backfill-gaps",
  auditEvents: "/api/v1/admin/audit-events",
  monitoringTemplates: "/api/v1/admin/monitoring/templates",
  monitoringQuery: "/api/v1/admin/monitoring/query",
  wsSession: "/api/v1/admin/ws",
} as const;

export type AdminCapabilityDiscoveryResponse = CapabilityDocument;

export interface AdminSessionPayload {
  authenticated: boolean;
  server_time: string;
  user: {
    subject: string;
    display_name: string;
    email: string | null;
  } | null;
  access_role: AccessRole | null;
  session: {
    issued_at: string;
    last_activity_at: string;
    idle_expires_at: string;
    absolute_expires_at: string;
  } | null;
  csrf: {
    header_name: string;
    token: string;
  } | null;
}

export interface AdminCursorPage {
  next_cursor: string | null;
  has_more: boolean;
}

export interface AdminListResponse<T> {
  items: T[];
  page: AdminCursorPage;
}

export interface AdminLastError {
  code: string;
  message: string;
  at: string;
  request_id: string;
}

export interface FeedSubscriptionDesired {
  enabled: boolean;
  deleted: boolean;
  version: number;
  updated_at: string;
  updated_by: string;
}

export interface FeedSubscriptionObserved {
  enabled: boolean;
  deleted: boolean;
  version: number;
  state: "pending" | "applying" | "applied" | "rejected" | "unknown";
  observed_at: string;
  collector_instance_id: string;
}

export interface FeedSubscriptionDrift {
  in_sync: boolean;
  desired_version: number;
  observed_version: number;
  since: string;
  reason:
    | "not_applied"
    | "collector_rejected"
    | "collector_unavailable"
    | "deletion_pending"
    | null;
}

export interface FeedSubscription {
  subscription_id: string;
  feed: string;
  venue: string;
  instrument_id: string;
  data_type: "bar";
  interval: string;
  desired: FeedSubscriptionDesired;
  observed: FeedSubscriptionObserved;
  drift: FeedSubscriptionDrift;
  last_error: AdminLastError | null;
  created_at: string;
  created_by: string;
  updated_at: string;
}

export interface FeedSubscriptionCreateBody {
  feed: string;
  instrument_id: string;
  data_type: "bar";
  interval: string;
}

export type FeedSubscriptionUpdateBody = Pick<
  FeedSubscriptionCreateBody,
  "feed" | "instrument_id" | "interval"
> & { enabled?: boolean };

export interface ApiRateLimitState {
  enabled: boolean;
  requests_per_second: number;
  burst: number;
  max_ws_sessions_per_ip: number;
  version: number;
  apply_mode: "hot" | "restart_required";
  observed: {
    version: number;
    state: string;
    observed_at: string;
  };
  drift: {
    in_sync: boolean;
    desired_version: number;
    observed_version: number;
    reason: string | null;
  };
  last_error: AdminLastError | null;
}

export interface WsSlowConsumerState {
  enabled: boolean;
  queue_size: number;
  lag_notice_interval_ms: number;
  version: number;
  apply_mode: "hot" | "restart_required";
  observed: {
    version: number;
    state: string;
    observed_at: string;
  };
  drift: {
    in_sync: boolean;
    desired_version: number;
    observed_version: number;
    reason: string | null;
  };
  last_error: AdminLastError | null;
}

export interface FlowControlResponse {
  api_rate_limit: ApiRateLimitState;
  ws_slow_consumer: WsSlowConsumerState;
}

/** The only three real Service Roles in the contract. */
export const SERVICE_ROLES = ["collector", "api", "replay"] as const;
export type ServiceRole = (typeof SERVICE_ROLES)[number];

export interface ServiceInstance {
  instance_id: string;
  service_role: ServiceRole;
  display_name: string;
  desired_state: "running" | "stopped";
  observed_state:
    | "running"
    | "starting"
    | "stopping"
    | "reloading"
    | "stopped"
    | "unknown";
  health: string;
  ready: boolean;
  ready_reason: string;
  pid: number | null;
  started_at: string | null;
  last_observed_at: string;
  last_error: AdminLastError | null;
  available_actions: ServiceLifecycleAction[];
}

export type ServiceLifecycleAction = "start" | "stop" | "reload";

export interface ServiceLifecycleBody {
  reason: string;
  confirm: true;
  confirm_instance_name?: string;
}

export type OperationStatus =
  | "accepted"
  | "running"
  | "succeeded"
  | "failed"
  | "unknown";

export interface AdminOperation {
  operation_id: string;
  kind: string;
  target: {
    instance_id: string;
    service_role: string;
  };
  requested_by: {
    subject: string;
    display_name: string;
    access_role: AccessRole;
  };
  reason: string;
  status: OperationStatus;
  requested_at: string;
  started_at: string | null;
  finished_at: string | null;
  result: Record<string, unknown> | null;
  error: { code: string; message: string; details: Record<string, unknown> } | null;
  request_id: string;
}

export type ExportJobStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelling"
  | "cancelled"
  | "expired";

export interface ExportJobProgress {
  processed_rows: number;
  processed_bytes: number;
  total_rows: number | null;
  percent: number | null;
  indeterminate: boolean;
}

export interface ExportJob {
  job_id: string;
  status: ExportJobStatus;
  instrument_id: string;
  interval: string;
  start: string;
  end: string;
  range_days: number;
  format: "csv";
  created_at: string;
  created_by: {
    subject: string;
    display_name: string;
  };
  progress: ExportJobProgress;
  result: {
    file_name: string;
    content_type: string;
    size_bytes: number;
    sha256: string;
    row_count: number;
  } | null;
  error: { code: string; message: string; details: Record<string, unknown> } | null;
  cancelled_at: string | null;
  expires_at: string | null;
  download_url: string | null;
}

export interface ExportJobCreateBody {
  instrument_id: string;
  interval: string;
  start: string;
  end: string;
  format: "csv";
}

export type BackfillJobStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "partially_succeeded"
  | "failed"
  | "cancelling"
  | "cancelled";

export interface BackfillJobCounters {
  scanned_bars: number;
  gap_bars: number;
  backfilled_bars: number;
  written_bars: number;
  rejected_bars: number;
}

export interface BackfillJob {
  job_id: string;
  status: BackfillJobStatus;
  instrument_id: string;
  interval: string;
  start: string;
  end: string;
  plan: {
    mode: "native_interval" | "one_minute_plus_aggregation";
    source_intervals: string[];
    target_interval: string;
    description: string;
  };
  current_range: { start: string; end: string } | null;
  counters: BackfillJobCounters;
  requested_by: {
    subject: string;
    display_name: string;
    access_role: AccessRole;
  };
  reason: string;
  retry_of_job_id: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error: { code: string; message: string; details: Record<string, unknown> } | null;
  resolved_gap_ids: string[];
}

export interface BackfillJobCreateBody {
  instrument_id: string;
  interval: string;
  start: string;
  end: string;
  reason: string;
  confirm: true;
}

export interface BackfillGap {
  gap_id: string;
  status: "open" | "resolved";
  feed: string;
  instrument_id: string;
  interval: string;
  start: string | null;
  end: string | null;
  reason: string;
  first_discovered_at: string;
  last_discovered_at: string;
  occurrence_count: number;
  resolved_at: string | null;
  resolved_by_job_id: string | null;
  related_job_ids: string[];
  created_at: string;
  updated_at: string;
}

export type AuditResult = "requested" | "succeeded" | "failed" | "denied";

export interface OperatorAuditEvent {
  event_id: string;
  sequence: number;
  occurred_at: string;
  actor: {
    subject: string;
    display_name: string;
    access_role: AccessRole | null;
  };
  action: string;
  target: {
    type: string;
    id: string;
    display_name: string;
  };
  result: AuditResult;
  failure_reason: string | null;
  reason: string | null;
  request_id: string;
  operation_id: string | null;
  job_id: string | null;
  instance_id: string | null;
  diff: {
    before: unknown;
    after: unknown;
    redacted_paths: string[];
  };
  hash_chain: {
    algorithm: string;
    previous_event_hash: string | null;
    event_hash: string;
  };
}

export interface MonitoringTemplate {
  name: string;
  description: string;
  unit: string;
  params: Array<{
    name: string;
    type: "enum" | "string" | "integer";
    values?: string[];
    required: boolean;
  }>;
  max_range_s: number;
  max_points: number;
  min_step: string;
}

export interface MonitoringQueryResponse {
  template: string;
  unit: string;
  source: string;
  from: string;
  to: string;
  step: string;
  series: Array<{
    labels: Record<string, string>;
    points: Array<{ ts: string; value: number | null }>;
  }>;
  truncated: boolean;
  warnings: string[];
}

export type AdminWsClientMessage =
  | { op: "subscribe"; topics: string[] }
  | { op: "unsubscribe"; topics: string[] }
  | { op: "ping" };

export type AdminWsServerMessage =
  | { op: "pong" }
  | { op: "event"; topic: string; data: unknown }
  | { op: "lag_notice"; dropped: number; since: string }
  | { op: "error"; code: string; message: string; request_id?: string };
