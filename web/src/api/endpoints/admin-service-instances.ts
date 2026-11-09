import { z } from "zod";
import { apiClient } from "@/api/http/client";
import type {
  AdminListResponse,
  ServiceInstance,
  ServiceLifecycleAction,
} from "@/api/endpoints/admin";

/**
 * CONTRACT TARGET — the Admin API does not exist in the current backend.
 *
 * Admin mode must not read the unauthenticated per-Role ops endpoints. This
 * module is the only allowed service-instance source for Admin mode and is
 * gated by the `service_lifecycle` capability; until the backend implements
 * `GET /api/v1/admin/service-instances`, the caller shows an explicit
 * capability gap instead of falling back to `/status` or `/ops/*`.
 *
 * Contract: `E:/total-project/mdgw-notes/operator-console/admin-api-contract.md`
 * §5.1 and §9.5.
 */

const lastErrorSchema = z
  .object({
    code: z.string(),
    message: z.string(),
    at: z.string(),
    request_id: z.string(),
  })
  .passthrough();

const serviceInstanceSchema = z
  .object({
    instance_id: z.string(),
    service_role: z.enum(["collector", "api", "replay"]),
    display_name: z.string(),
    desired_state: z.enum(["running", "stopped"]),
    observed_state: z.enum([
      "running",
      "starting",
      "stopping",
      "reloading",
      "stopped",
      "unknown",
    ]),
    health: z.string(),
    ready: z.boolean(),
    ready_reason: z.string(),
    pid: z.number().nullable().optional().default(null),
    started_at: z.string().nullable().optional().default(null),
    last_observed_at: z.string(),
    last_error: lastErrorSchema.nullable().optional().default(null),
    available_actions: z.array(z.enum(["start", "stop", "reload"])),
  })
  .passthrough();

const listSchema = z
  .object({
    items: z.array(serviceInstanceSchema),
    page: z
      .object({
        next_cursor: z.string().nullable(),
        has_more: z.boolean(),
      })
      .passthrough(),
  })
  .passthrough();

export class AdminServiceInstanceContractError extends Error {
  readonly code = "service_instances_malformed";

  constructor(message: string) {
    super(message);
    this.name = "AdminServiceInstanceContractError";
  }
}

/**
 * Fetches the authoritative Admin service-instance list. The session cookie is
 * sent by the browser; no token is ever placed in a URL.
 */
export async function fetchAdminServiceInstances(
  signal?: AbortSignal,
): Promise<ServiceInstance[]> {
  const response = await apiClient.get<unknown>(
    "/api/v1/admin/service-instances",
    { signal },
  );
  const parsed = listSchema.safeParse(response.data);
  if (!parsed.success) {
    throw new AdminServiceInstanceContractError(
      "Service instance list did not match the Admin API contract",
    );
  }
  return (parsed.data as AdminListResponse<ServiceInstance>).items;
}

export interface AdminServiceInstanceRow {
  instanceId: string;
  role: "collector" | "api" | "replay";
  displayName: string;
  desiredState: "running" | "stopped";
  observedState:
    | "running"
    | "starting"
    | "stopping"
    | "reloading"
    | "stopped"
    | "unknown";
  health: string;
  ready: boolean;
  readyReason: string;
  pid: number | null;
  startedAt: string | null;
  lastObservedAt: string;
  lastError: ServiceInstance["last_error"];
  availableActions: ServiceLifecycleAction[];
}

export function adminServiceInstanceToRow(
  instance: ServiceInstance,
): AdminServiceInstanceRow {
  return {
    instanceId: instance.instance_id,
    role: instance.service_role,
    displayName: instance.display_name,
    desiredState: instance.desired_state,
    observedState: instance.observed_state,
    health: instance.health,
    ready: instance.ready,
    readyReason: instance.ready_reason,
    pid: instance.pid,
    startedAt: instance.started_at,
    lastObservedAt: instance.last_observed_at,
    lastError: instance.last_error,
    availableActions: instance.available_actions,
  };
}
