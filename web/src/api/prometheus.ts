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
 */
const PROMETHEUS_BASE_URL = import.meta.env.VITE_PROMETHEUS_BASE_URL ?? "";

const prometheusClient = axios.create({
  baseURL: PROMETHEUS_BASE_URL,
  timeout: 15_000,
});

export interface PrometheusRangeQuery {
  query: string;
  start: number;
  end: number;
  step: string;
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

export function isDirectPrometheusConfigured(): boolean {
  return PROMETHEUS_BASE_URL.length > 0;
}
