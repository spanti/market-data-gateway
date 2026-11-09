import { z } from "zod";
import { apiClient } from "@/api/http/client";
import type { CapabilityState } from "@/api/capabilities";
import {
  adminCapabilityAvailability,
  CAPABILITY_KEYS,
  requiresAdminApiReason,
} from "@/runtime/capability-gates";
import type { DisabledReason } from "@/runtime/permissions";
import type { TrendSeries } from "@/api/monitoring/types";

/**
 * CONTRACT TARGET — the Admin API monitoring proxy is not implemented by the
 * current backend. Admin API mode may only use this authenticated,
 * restricted-template endpoint; it must never call direct PromQL.
 *
 * Contract: `E:/total-project/mdgw-notes/operator-console/admin-api-contract.md`
 * §9.
 */

const templateParamSchema = z.object({
  name: z.string(),
  type: z.enum(["enum", "string", "integer"]),
  values: z.array(z.string()).optional(),
  required: z.boolean(),
});

const templateSchema = z
  .object({
    name: z.string(),
    description: z.string().optional().default(""),
    unit: z.string(),
    params: z.array(templateParamSchema).optional().default([]),
    max_range_s: z.number(),
    max_points: z.number(),
    min_step: z.string(),
  })
  .passthrough();

const templatesResponseSchema = z
  .object({ items: z.array(templateSchema) })
  .passthrough();

const pointSchema = z.object({
  ts: z.string(),
  value: z.number().nullable(),
});

const querySeriesSchema = z.object({
  labels: z.record(z.string(), z.string()).optional().default({}),
  points: z.array(pointSchema),
});

const queryResponseSchema = z
  .object({
    template: z.string(),
    unit: z.string(),
    source: z.string(),
    from: z.string(),
    to: z.string(),
    step: z.string(),
    series: z.array(querySeriesSchema),
    truncated: z.boolean().optional().default(false),
    warnings: z.array(z.string()).optional().default([]),
  })
  .passthrough();

export type AdminMonitoringTemplateParam = z.infer<typeof templateParamSchema>;
export type AdminMonitoringTemplate = z.infer<typeof templateSchema>;
export type AdminMonitoringPoint = z.infer<typeof pointSchema>;
export type AdminMonitoringSeries = z.infer<typeof querySeriesSchema>;
export type AdminMonitoringQueryResponse = z.infer<
  typeof queryResponseSchema
>;

export type MonitoringProxyErrorCode = "monitoring_proxy_malformed";

export class MonitoringProxyContractError extends Error {
  readonly code: MonitoringProxyErrorCode;

  constructor(message: string) {
    super(message);
    this.name = "MonitoringProxyContractError";
    this.code = "monitoring_proxy_malformed";
  }
}

export interface AdminMonitoringQueryRequest {
  template: string;
  params?: Record<string, string>;
  from: string;
  to: string;
  step: string;
}

export const ADMIN_MONITORING_TEMPLATES_PATH =
  "/api/v1/admin/monitoring/templates";
export const ADMIN_MONITORING_QUERY_PATH = "/api/v1/admin/monitoring/query";

export async function fetchAdminMonitoringTemplates(
  signal?: AbortSignal,
): Promise<AdminMonitoringTemplate[]> {
  const response = await apiClient.get<unknown>(
    ADMIN_MONITORING_TEMPLATES_PATH,
    { signal },
  );
  const parsed = templatesResponseSchema.safeParse(response.data);
  if (!parsed.success) {
    throw new MonitoringProxyContractError(
      "Monitoring template list did not match the contract",
    );
  }
  return parsed.data.items;
}

export async function queryAdminMonitoring(
  request: AdminMonitoringQueryRequest,
  signal?: AbortSignal,
): Promise<AdminMonitoringQueryResponse> {
  const response = await apiClient.get<unknown>(
    ADMIN_MONITORING_QUERY_PATH,
    {
      signal,
      params: {
        template: request.template,
        ...request.params,
        from: request.from,
        to: request.to,
        step: request.step,
      },
    },
  );
  const parsed = queryResponseSchema.safeParse(response.data);
  if (!parsed.success) {
    throw new MonitoringProxyContractError(
      "Monitoring query response did not match the contract",
    );
  }
  return parsed.data;
}

/**
 * Only pass parameters the template actually declares, and only values its
 * enum admits. Arbitrary expressions must never be forwardable.
 */
export function buildAdminQueryParams(
  template: AdminMonitoringTemplate,
  desired: Record<string, string>,
): Record<string, string> {
  const params: Record<string, string> = {};
  for (const param of template.params) {
    const value = desired[param.name];
    if (value === undefined) {
      continue;
    }
    if (param.type === "enum" && param.values && !param.values.includes(value)) {
      continue;
    }
    params[param.name] = value;
  }
  return params;
}

export type AdminMonitoringAvailability =
  | { kind: "enabled" }
  | { kind: "disabled"; reason: DisabledReason };

/**
 * Admin mode must not fall back to unauthenticated Prometheus. If the proxy
 * capability is missing/unsupported/unavailable, show its disabled reason.
 */
export function adminMonitoringAvailability(
  capability: CapabilityState | null,
): AdminMonitoringAvailability {
  const availability = adminCapabilityAvailability(
    capability,
    CAPABILITY_KEYS.monitoringProxy,
  );
  switch (availability.kind) {
    case "enabled":
      return { kind: "enabled" };
    case "disabled":
      return availability;
    case "not-admin":
      return { kind: "disabled", reason: requiresAdminApiReason() };
  }
}

function seriesName(
  labels: Record<string, string>,
  fallback: string,
  index: number,
  count: number,
): string {
  if (labels.path) {
    return `${fallback} · ${labels.path}`;
  }
  return count > 1 ? `${fallback} ${index + 1}` : fallback;
}

export function adminResponseToTrendSeries(
  response: Pick<AdminMonitoringQueryResponse, "series">,
  fallbackName: string,
): TrendSeries[] {
  return response.series.map((series, index) => ({
    name: seriesName(
      series.labels,
      fallbackName,
      index,
      response.series.length,
    ),
    labels: series.labels,
    points: series.points.flatMap((point) => {
      const ts = Date.parse(point.ts);
      if (!Number.isFinite(ts)) {
        return [];
      }
      return [{ ts, value: point.value }];
    }),
  }));
}

export function maxAdminSample(
  response: Pick<AdminMonitoringQueryResponse, "series">,
): number | null {
  let peak: number | null = null;
  for (const series of response.series) {
    for (const point of series.points) {
      if (point.value === null || !Number.isFinite(point.value)) {
        continue;
      }
      if (peak === null || point.value > peak) {
        peak = point.value;
      }
    }
  }
  return peak;
}
