import axios from "axios";
import {
  parsePrometheusPayload,
  type PrometheusQueryResult,
} from "@/api/http/prometheus";

/**
 * Current Contract, intranet-development-only direct Prometheus access.
 * Queries must be fixed templates chosen by the app; do not expose an
 * arbitrary PromQL editor. Admin API mode uses the authenticated monitoring
 * proxy instead and must not fall back here.
 *
 * The default base path is the same-origin `/prometheus` proxy configured in
 * `vite.config.ts` (development) and by the production static host. Prometheus
 * itself serves its HTTP API at `/api/v1/...` under that prefix.
 */
export const DEFAULT_PROMETHEUS_BASE_PATH = "/prometheus";

const PROMETHEUS_BASE_URL =
  import.meta.env.VITE_PROMETHEUS_BASE_URL ?? DEFAULT_PROMETHEUS_BASE_PATH;

const prometheusClient = axios.create({
  baseURL: PROMETHEUS_BASE_URL,
  timeout: 15_000,
  // Prometheus reports query failures as a JSON body with status "error".
  // Parse that body rather than losing it to a generic Axios rejection.
  validateStatus: () => true,
});

export interface PrometheusRangeQuery {
  query: string;
  /** Unix seconds. */
  start: number;
  /** Unix seconds. */
  end: number;
  /** Prometheus duration, e.g. `30s`. */
  step: string;
}

export interface PrometheusInstantQuery {
  query: string;
  /** Unix seconds; omitted means "now" on the Prometheus server. */
  time?: number;
}

export async function queryPrometheusRange(
  params: PrometheusRangeQuery,
  signal?: AbortSignal,
): Promise<PrometheusQueryResult> {
  const response = await prometheusClient.get<unknown>(
    "/api/v1/query_range",
    { params, signal },
  );
  return parsePrometheusPayload(response.data);
}

export async function queryPrometheusInstant(
  params: PrometheusInstantQuery,
  signal?: AbortSignal,
): Promise<PrometheusQueryResult> {
  const response = await prometheusClient.get<unknown>("/api/v1/query", {
    params,
    signal,
  });
  return parsePrometheusPayload(response.data);
}

export function isDirectPrometheusConfigured(): boolean {
  return PROMETHEUS_BASE_URL.length > 0;
}

export function getPrometheusBaseUrl(): string {
  return PROMETHEUS_BASE_URL;
}
