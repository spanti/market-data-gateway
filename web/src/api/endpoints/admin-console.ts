import { apiClient } from "@/api/http/client";
import {
  ADMIN_PATHS,
  type AdminListResponse,
  type AdminOperation,
  type FeedSubscription,
  type FeedSubscriptionCreateBody,
  type FeedSubscriptionUpdateBody,
  type FlowControlResponse,
  type ServiceInstance,
  type ServiceLifecycleBody,
  type ServiceLifecycleAction,
} from "@/api/endpoints/admin";

/**
 * Admin API contract-target client for the Gateway Console.
 *
 * CONTRACT TARGET — NOT IMPLEMENTED BY THE CURRENT BACKEND. Every call below
 * is exercised only against MSW fixtures in tests. Do not report these as
 * integrated features.
 *
 * Mutations carry `If-Match: "<version>"`, `X-CSRF-Token` (in-memory session
 * token) and, for lifecycle POSTs, `Idempotency-Key`. The client never retries
 * a non-idempotent write automatically.
 */

export interface ApiRateLimitUpdateBody {
  enabled: boolean;
  requests_per_second: number;
  burst: number;
  max_ws_sessions_per_ip: number;
}

export interface WsSlowConsumerUpdateBody {
  enabled: boolean;
  queue_size: number;
  lag_notice_interval_ms: number;
}

function writeHeaders(options: {
  version?: number;
  csrfToken?: string | null;
  idempotencyKey?: string;
}): Record<string, string> {
  const headers: Record<string, string> = {};
  if (options.version !== undefined) {
    headers["If-Match"] = `"${options.version}"`;
  }
  if (options.csrfToken) {
    headers["X-CSRF-Token"] = options.csrfToken;
  }
  if (options.idempotencyKey) {
    headers["Idempotency-Key"] = options.idempotencyKey;
  }
  return headers;
}

export function newIdempotencyKey(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `idem-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export async function fetchFeedSubscriptions(
  signal?: AbortSignal,
): Promise<AdminListResponse<FeedSubscription>> {
  const response = await apiClient.get<AdminListResponse<FeedSubscription>>(
    ADMIN_PATHS.feedSubscriptions,
    { signal },
  );
  return response.data;
}

export async function createFeedSubscription(
  body: FeedSubscriptionCreateBody,
  csrfToken: string | null,
  idempotencyKey: string,
): Promise<FeedSubscription> {
  const response = await apiClient.post<FeedSubscription>(
    ADMIN_PATHS.feedSubscriptions,
    body,
    { headers: writeHeaders({ csrfToken, idempotencyKey }) },
  );
  return response.data;
}

export async function updateFeedSubscription(
  subscriptionId: string,
  body: FeedSubscriptionUpdateBody,
  version: number,
  csrfToken: string | null,
): Promise<FeedSubscription> {
  const response = await apiClient.put<FeedSubscription>(
    `${ADMIN_PATHS.feedSubscriptions}/${encodeURIComponent(subscriptionId)}`,
    body,
    { headers: writeHeaders({ version, csrfToken }) },
  );
  return response.data;
}

export async function deleteFeedSubscription(
  subscriptionId: string,
  version: number,
  csrfToken: string | null,
): Promise<void> {
  await apiClient.delete(
    `${ADMIN_PATHS.feedSubscriptions}/${encodeURIComponent(subscriptionId)}`,
    { headers: writeHeaders({ version, csrfToken }) },
  );
}

export async function fetchFlowControl(
  signal?: AbortSignal,
): Promise<FlowControlResponse> {
  const response = await apiClient.get<FlowControlResponse>(
    ADMIN_PATHS.flowControl,
    { signal },
  );
  return response.data;
}

export async function updateApiRateLimit(
  body: ApiRateLimitUpdateBody,
  version: number,
  csrfToken: string | null,
): Promise<void> {
  await apiClient.put(
    `${ADMIN_PATHS.flowControl}/api-rate-limit`,
    body,
    { headers: writeHeaders({ version, csrfToken }) },
  );
}

export async function updateWsSlowConsumer(
  body: WsSlowConsumerUpdateBody,
  version: number,
  csrfToken: string | null,
): Promise<void> {
  await apiClient.put(
    `${ADMIN_PATHS.flowControl}/ws-slow-consumer`,
    body,
    { headers: writeHeaders({ version, csrfToken }) },
  );
}

export async function fetchServiceInstances(
  signal?: AbortSignal,
): Promise<AdminListResponse<ServiceInstance>> {
  const response = await apiClient.get<AdminListResponse<ServiceInstance>>(
    ADMIN_PATHS.serviceInstances,
    { signal },
  );
  return response.data;
}

function lifecyclePath(
  instanceId: string,
  action: ServiceLifecycleAction,
): string {
  return `${ADMIN_PATHS.serviceInstances}/${encodeURIComponent(instanceId)}/${action}`;
}

export async function postServiceLifecycle(
  instanceId: string,
  action: ServiceLifecycleAction,
  body: ServiceLifecycleBody,
  csrfToken: string | null,
  idempotencyKey: string,
): Promise<unknown> {
  const response = await apiClient.post(
    lifecyclePath(instanceId, action),
    body,
    { headers: writeHeaders({ csrfToken, idempotencyKey }) },
  );
  return response.data;
}

export async function fetchAdminOperation(
  operationId: string,
  signal?: AbortSignal,
): Promise<AdminOperation> {
  const response = await apiClient.get<AdminOperation>(
    `${ADMIN_PATHS.operations}/${encodeURIComponent(operationId)}`,
    { signal },
  );
  return response.data;
}
