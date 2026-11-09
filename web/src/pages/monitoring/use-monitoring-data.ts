import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import { useMemo } from "react";
import type { ServiceInstance } from "@/api/endpoints/admin";
import type { InstrumentsResponse } from "@/api/endpoints/instruments";
import {
  fetchDatasetStatus,
  fetchApiReady,
  fetchApiStatus,
  fetchCollectorReady,
  fetchCollectorStatus,
  fetchReplayReady,
  fetchReplayStatus,
  type ApiStatus,
  type CollectorStatus,
  type DatasetStatus,
  type ReadyPayload,
  type ReplayStatus,
} from "@/api/endpoints/ops";
import { fetchInstruments } from "@/api/endpoints/instruments";
import {
  adminServiceInstanceToRow,
  fetchAdminServiceInstances,
} from "@/api/endpoints/admin-service-instances";
import {
  adminMonitoringAvailability,
  adminResponseToTrendSeries,
  buildAdminQueryParams,
  fetchAdminMonitoringTemplates,
  maxAdminSample,
  queryAdminMonitoring,
  type AdminMonitoringQueryResponse,
  type AdminMonitoringTemplate,
} from "@/api/monitoring/admin-proxy";
import type { ServiceInstanceRow } from "@/components/monitoring/service-instance-list";
import {
  ADMIN_SERVICE_INSTANCE_GAP,
  serviceInstanceSource,
  type ServiceInstanceSource,
} from "@/runtime/service-instance-source";
import {
  fetchLatencyTrends,
  fetchPeakQps,
  fetchQpsTrend,
} from "@/api/monitoring/direct";
import type { TrendSeries } from "@/api/monitoring/types";
import {
  PROMETHEUS_POLL_INTERVAL_MS,
  STATUS_POLL_INTERVAL_MS,
  peakRangeLabel,
  type MonitoringRangeOption,
} from "@/lib/monitoring-config";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import type { DisabledReason } from "@/runtime/permissions";

export const MONITORING_QUERY_KEYS = {
  datasetStatus: ["monitoring", "status", "dataset"] as const,
  apiStatus: ["monitoring", "status", "api"] as const,
  apiReady: ["monitoring", "ready", "api"] as const,
  collectorStatus: ["monitoring", "status", "collector"] as const,
  collectorReady: ["monitoring", "ready", "collector"] as const,
  replayStatus: ["monitoring", "status", "replay"] as const,
  replayReady: ["monitoring", "ready", "replay"] as const,
  instruments: ["monitoring", "instruments"] as const,
  qpsTrend: ["monitoring", "trend", "qps"] as const,
  peakQps: ["monitoring", "trend", "peak-qps"] as const,
  latencyTrend: ["monitoring", "trend", "latency"] as const,
  adminTemplates: ["monitoring", "admin", "templates"] as const,
  adminTrend: ["monitoring", "admin", "trend"] as const,
  adminServiceInstances: ["monitoring", "admin", "service-instances"] as const,
};

export type SourceState = "loading" | "ready" | "error";

export function sourceState<T>(query: UseQueryResult<T>): SourceState {
  if (query.isPending) {
    return "loading";
  }
  return query.isError ? "error" : "ready";
}

/** Number of the last successful update, or null if none ever happened. */
export function lastSuccessAt<T>(query: UseQueryResult<T>): number | null {
  if (query.data === undefined || query.dataUpdatedAt <= 0) {
    return null;
  }
  return query.dataUpdatedAt;
}

export function errorText(error: unknown): string {
  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }
  return "数据源不可用";
}

function useStatusQuery<T>(
  queryKey: readonly unknown[],
  queryFn: (signal?: AbortSignal) => Promise<T>,
  enabled = true,
) {
  return useQuery({
    queryKey,
    queryFn: ({ signal }) => queryFn(signal),
    refetchInterval: STATUS_POLL_INTERVAL_MS,
    staleTime: 0,
    retry: false,
    enabled,
  });
}

/**
 * The per-Role `/status`, `/readyz` and `/ops/*` endpoints are Current
 * Contract only. Admin mode must not call them; the deployment denies those
 * paths, and the UI shows an explicit capability gap instead of a fallback.
 */
export interface MonitoringSources {
  dataset: UseQueryResult<DatasetStatus>;
  api: UseQueryResult<ApiStatus>;
  apiReady: UseQueryResult<ReadyPayload>;
  collector: UseQueryResult<CollectorStatus>;
  collectorReady: UseQueryResult<ReadyPayload>;
  replay: UseQueryResult<ReplayStatus>;
  replayReady: UseQueryResult<ReadyPayload>;
  instruments: UseQueryResult<InstrumentsResponse>;
  adminInstances: UseQueryResult<ServiceInstance[]>;
  directOpsEnabled: boolean;
  serviceSource: ServiceInstanceSource;
  opsGapReason: string | null;
}

export function useMonitoringSources(): MonitoringSources {
  const { capability, role } = useRuntimeMode();
  const directOpsEnabled = capability?.mode === "current-contract";
  const serviceSource = serviceInstanceSource(capability);
  const adminInstances = useQuery({
    queryKey: MONITORING_QUERY_KEYS.adminServiceInstances,
    queryFn: ({ signal }) => fetchAdminServiceInstances(signal),
    refetchInterval: STATUS_POLL_INTERVAL_MS,
    staleTime: 0,
    retry: false,
    enabled: serviceSource.kind === "admin" && role !== null,
  });

  const opsGapReason = directOpsEnabled
    ? null
    : capability === null || capability.mode === "capabilities-unknown"
      ? "能力未知，暂不可用"
      : `${ADMIN_SERVICE_INSTANCE_GAP}；Admin 模式不暴露 /status、/ops/* 或 /prometheus`;

  return {
    directOpsEnabled,
    serviceSource,
    opsGapReason,
    adminInstances,
    dataset: useStatusQuery(
      MONITORING_QUERY_KEYS.datasetStatus,
      fetchDatasetStatus,
    ),
    api: useStatusQuery(
      MONITORING_QUERY_KEYS.apiStatus,
      fetchApiStatus,
      directOpsEnabled,
    ),
    apiReady: useStatusQuery(
      MONITORING_QUERY_KEYS.apiReady,
      fetchApiReady,
      directOpsEnabled,
    ),
    collector: useStatusQuery(
      MONITORING_QUERY_KEYS.collectorStatus,
      fetchCollectorStatus,
      directOpsEnabled,
    ),
    collectorReady: useStatusQuery(
      MONITORING_QUERY_KEYS.collectorReady,
      fetchCollectorReady,
      directOpsEnabled,
    ),
    replay: useStatusQuery(
      MONITORING_QUERY_KEYS.replayStatus,
      fetchReplayStatus,
      directOpsEnabled,
    ),
    replayReady: useStatusQuery(
      MONITORING_QUERY_KEYS.replayReady,
      fetchReplayReady,
      directOpsEnabled,
    ),
    instruments: useStatusQuery(
      MONITORING_QUERY_KEYS.instruments,
      (signal) => fetchInstruments({ include_delisted: true }, signal),
    ),
  };
}

/**
 * Contract-target mapping from the Admin service-instance list to the same
 * ledger row shape used by Current Contract. Only fields the Admin contract
 * actually returns are rendered; uptime is not inferred from `started_at`.
 */
export function adminInstanceToServiceRow(
  instance: ServiceInstance,
): ServiceInstanceRow {
  const row = adminServiceInstanceToRow(instance);
  const observed = Date.parse(row.lastObservedAt);
  return {
    role: row.role,
    service: row.role,
    ready: row.ready,
    readyReason: row.readyReason || null,
    uptimeS: null,
    statusState: "ready",
    lastSuccessAt: Number.isFinite(observed) ? observed : null,
    details: [
      `Instance ${row.instanceId}`,
      `Desired ${row.desiredState}`,
      `Observed ${row.observedState}`,
      `Health ${row.health}`,
    ],
    errorMessage: row.lastError?.message ?? null,
  };
}

export type TrendState = "loading" | "ready" | "empty" | "error" | "unknown";

export interface TrendBlock<T> {
  value: T;
  state: TrendState;
  lastSuccessAt: number | null;
  error: string | null;
}

export interface TrendSourceView {
  label: string;
  windowLabel: string;
  rateWindow: string;
  peakLabel: string;
  state: "loading" | "ready" | "error" | "unknown";
  lastSuccessAt: number | null;
  disabledReason: DisabledReason | null;
}

export interface MonitoringTrends {
  qps: TrendBlock<TrendSeries[]>;
  peak: TrendBlock<number | null>;
  latency: TrendBlock<TrendSeries[]>;
  source: TrendSourceView;
}

function hasFinitePoint(series: TrendSeries[]): boolean {
  return series.some((item) =>
    item.points.some(
      (point) => point.value !== null && Number.isFinite(point.value),
    ),
  );
}

function blockState<T>(
  query: UseQueryResult<T>,
  hasData: boolean,
): TrendState {
  if (query.isPending) {
    return "loading";
  }
  if (query.isError) {
    return "error";
  }
  return hasData ? "ready" : "empty";
}

function secondsToMilliseconds(series: TrendSeries[]): TrendSeries[] {
  return series.map((item) => ({
    ...item,
    points: item.points.map((point) => ({
      ...point,
      value:
        point.value === null || !Number.isFinite(point.value)
          ? null
          : point.value * 1000,
    })),
  }));
}

function adminSeriesToMs(
  response: AdminMonitoringQueryResponse,
  fallback: string,
): TrendSeries[] {
  const series = adminResponseToTrendSeries(response, fallback);
  return response.unit === "seconds"
    ? secondsToMilliseconds(series)
    : series;
}

interface AdminTemplateQueryOptions {
  template: AdminMonitoringTemplate | undefined;
  templateName: string;
  range: MonitoringRangeOption;
  enabled: boolean;
}

function useAdminTemplateQuery({
  template,
  templateName,
  range,
  enabled,
}: AdminTemplateQueryOptions) {
  return useQuery({
    queryKey: [
      ...MONITORING_QUERY_KEYS.adminTrend,
      templateName,
      range.id,
    ],
    queryFn: async ({ signal }) => {
      if (!template) {
        throw new Error(`监控模板 ${templateName} 未提供`);
      }
      const endMs = Date.now();
      return queryAdminMonitoring(
        {
          template: template.name,
          params: buildAdminQueryParams(template, {
            service_role: "api",
          }),
          from: new Date(endMs - range.durationMs).toISOString(),
          to: new Date(endMs).toISOString(),
          step: `${range.stepSeconds}s`,
        },
        signal,
      );
    },
    enabled: enabled && Boolean(template),
    refetchInterval: PROMETHEUS_POLL_INTERVAL_MS,
    staleTime: 0,
    retry: false,
  });
}

function disabledTrend<T>(value: T): TrendBlock<T> {
  return { value, state: "unknown", lastSuccessAt: null, error: null };
}

function loadingTrend<T>(value: T): TrendBlock<T> {
  return { value, state: "loading", lastSuccessAt: null, error: null };
}

/**
 * Trend source selection is capability-driven, never inferred from cached
 * data:
 * - Current Contract -> fixed queries against the same-origin `/prometheus`.
 * - Admin API -> only the authenticated template proxy; no direct PromQL.
 * - Capability unknown/unavailable -> explicit disabled/unknown state.
 */
export function useMonitoringTrends(
  range: MonitoringRangeOption,
): MonitoringTrends {
  const { capability, role, sessionLoading } = useRuntimeMode();
  const mode = capability?.mode ?? null;
  const discoveryPending = mode === null;
  const authPending = mode === "admin-api" && sessionLoading;
  const directEnabled = mode === "current-contract";
  const availability = capability
    ? adminMonitoringAvailability(capability)
    : null;
  const authenticated = role !== null;
  const adminEnabled =
    mode === "admin-api" &&
    availability?.kind === "enabled" &&
    authenticated;

  const templatesQuery = useQuery({
    queryKey: MONITORING_QUERY_KEYS.adminTemplates,
    queryFn: ({ signal }) => fetchAdminMonitoringTemplates(signal),
    enabled: adminEnabled,
    staleTime: 60_000,
    retry: false,
  });

  const templates = useMemo(
    () => templatesQuery.data ?? [],
    [templatesQuery.data],
  );
  const template = (name: string) =>
    templates.find((item) => item.name === name);

  const qpsQuery = useQuery({
    queryKey: [...MONITORING_QUERY_KEYS.qpsTrend, range.id],
    queryFn: ({ signal }) => {
      const endMs = Date.now();
      return fetchQpsTrend(
        {
          startMs: endMs - range.durationMs,
          endMs,
          stepSeconds: range.stepSeconds,
        },
        range.qpsRateWindow,
        signal,
      );
    },
    enabled: directEnabled,
    refetchInterval: PROMETHEUS_POLL_INTERVAL_MS,
    staleTime: 0,
    retry: false,
  });

  const peakQuery = useQuery({
    queryKey: [...MONITORING_QUERY_KEYS.peakQps, range.id],
    queryFn: ({ signal }) =>
      fetchPeakQps(
        {
          endMs: Date.now(),
          lookbackMs: range.durationMs,
          rateWindow: range.peakRateWindow,
          stepSeconds: range.peakStepSeconds,
        },
        signal,
      ),
    enabled: directEnabled,
    refetchInterval: PROMETHEUS_POLL_INTERVAL_MS,
    staleTime: 0,
    retry: false,
  });

  const latencyQuery = useQuery({
    queryKey: [...MONITORING_QUERY_KEYS.latencyTrend, range.id],
    queryFn: ({ signal }) => {
      const endMs = Date.now();
      return fetchLatencyTrends(
        {
          startMs: endMs - range.durationMs,
          endMs,
          stepSeconds: range.stepSeconds,
        },
        range.qpsRateWindow,
        signal,
      );
    },
    enabled: directEnabled,
    refetchInterval: PROMETHEUS_POLL_INTERVAL_MS,
    staleTime: 0,
    retry: false,
  });

  const adminQpsQuery = useAdminTemplateQuery({
    template: template("http_qps"),
    templateName: "http_qps",
    range,
    enabled: adminEnabled,
  });
  const adminPeakQuery = useAdminTemplateQuery({
    template: template("http_peak_qps"),
    templateName: "http_peak_qps",
    range,
    enabled: adminEnabled,
  });
  const adminP50Query = useAdminTemplateQuery({
    template: template("http_latency_p50"),
    templateName: "http_latency_p50",
    range,
    enabled: adminEnabled,
  });
  const adminP95Query = useAdminTemplateQuery({
    template: template("http_latency_p95"),
    templateName: "http_latency_p95",
    range,
    enabled: adminEnabled,
  });
  const adminP99Query = useAdminTemplateQuery({
    template: template("http_latency_p99"),
    templateName: "http_latency_p99",
    range,
    enabled: adminEnabled,
  });

  return useMemo(() => {
    const windowLabel = `最近 ${range.label}`;
    const loginRequiredReason: DisabledReason | null =
      mode === "admin-api" &&
      availability?.kind === "enabled" &&
      !authenticated
        ? { code: "requires-login", message: "需要登录" }
        : null;
    const disabledReason =
      availability?.kind === "disabled"
        ? availability.reason
        : loginRequiredReason;

    const sourceBase = {
      label:
        mode === "admin-api"
          ? "Admin API 监控代理"
          : "Prometheus（/prometheus）",
      windowLabel,
      rateWindow: range.qpsRateWindow,
      peakLabel: `${peakRangeLabel(range)} · ${range.peakRateWindow} rate · ${range.peakStepSeconds}s step`,
    };

    if (directEnabled) {
      const qpsSeries = qpsQuery.data ?? [];
      const latencySeries = latencyQuery.data ?? [];
      const lastSuccesses = [
        lastSuccessAt(qpsQuery),
        lastSuccessAt(peakQuery),
        lastSuccessAt(latencyQuery),
      ].filter((value): value is number => value !== null);

      return {
        qps: {
          value: qpsSeries,
          state: blockState(qpsQuery, hasFinitePoint(qpsSeries)),
          lastSuccessAt: lastSuccessAt(qpsQuery),
          error: qpsQuery.isError ? errorText(qpsQuery.error) : null,
        },
        peak: {
          value: peakQuery.data ?? null,
          state: blockState(peakQuery, peakQuery.data !== null && peakQuery.data !== undefined),
          lastSuccessAt: lastSuccessAt(peakQuery),
          error: peakQuery.isError ? errorText(peakQuery.error) : null,
        },
        latency: {
          value: latencySeries,
          state: blockState(latencyQuery, hasFinitePoint(latencySeries)),
          lastSuccessAt: lastSuccessAt(latencyQuery),
          error: latencyQuery.isError
            ? errorText(latencyQuery.error)
            : null,
        },
        source: {
          ...sourceBase,
          state:
            qpsQuery.isError || peakQuery.isError || latencyQuery.isError
              ? "error"
              : qpsQuery.isPending ||
                  peakQuery.isPending ||
                  latencyQuery.isPending
                ? "loading"
                : "ready",
          lastSuccessAt:
            lastSuccesses.length > 0 ? Math.max(...lastSuccesses) : null,
          disabledReason: null,
        },
      } satisfies MonitoringTrends;
    }

    if (adminEnabled) {
      const qpsSeries = adminQpsQuery.data
        ? adminSeriesToMs(adminQpsQuery.data, "QPS")
        : [];
      const latencySeries = [
        { query: adminP50Query, name: "P50" },
        { query: adminP95Query, name: "P95" },
        { query: adminP99Query, name: "P99" },
      ].flatMap(({ query, name }) =>
        query.data ? adminSeriesToMs(query.data, name) : [],
      );
      const adminError =
        templatesQuery.isError ||
        adminQpsQuery.isError ||
        adminPeakQuery.isError ||
        adminP50Query.isError ||
        adminP95Query.isError ||
        adminP99Query.isError;
      const templateMissing = (name: string) =>
        templatesQuery.isSuccess &&
        !templates.some((item) => item.name === name);
      const missingTemplates = [
        "http_qps",
        "http_peak_qps",
        "http_latency_p50",
        "http_latency_p95",
        "http_latency_p99",
      ].filter((name) => templateMissing(name));
      const adminSuccesses = [
        lastSuccessAt(templatesQuery),
        lastSuccessAt(adminQpsQuery),
        lastSuccessAt(adminPeakQuery),
        lastSuccessAt(adminP50Query),
        lastSuccessAt(adminP95Query),
        lastSuccessAt(adminP99Query),
      ].filter((value): value is number => value !== null);

      return {
        qps: {
          value: qpsSeries,
          state: templateMissing("http_qps")
            ? "unknown"
            : blockState(adminQpsQuery, hasFinitePoint(qpsSeries)),
          lastSuccessAt: lastSuccessAt(adminQpsQuery),
          error: adminQpsQuery.isError
            ? errorText(adminQpsQuery.error)
            : null,
        },
        peak: {
          value: adminPeakQuery.data
            ? maxAdminSample(adminPeakQuery.data)
            : null,
          state: templateMissing("http_peak_qps")
            ? "unknown"
            : blockState(
                adminPeakQuery,
                adminPeakQuery.data !== undefined &&
                  maxAdminSample(adminPeakQuery.data) !== null,
              ),
          lastSuccessAt: lastSuccessAt(adminPeakQuery),
          error: adminPeakQuery.isError
            ? errorText(adminPeakQuery.error)
            : null,
        },
        latency: {
          value: latencySeries,
          state:
            templateMissing("http_latency_p50") ||
            templateMissing("http_latency_p95") ||
            templateMissing("http_latency_p99")
              ? "unknown"
              : blockState(
                  adminP50Query,
                  latencySeries.some((item) => hasFinitePoint([item])),
                ),
          lastSuccessAt: lastSuccessAt(adminP50Query),
          error: adminP50Query.isError
            ? errorText(adminP50Query.error)
            : null,
        },
        source: {
          ...sourceBase,
          state: adminError
            ? "error"
            : missingTemplates.length > 0
              ? "unknown"
              : templatesQuery.isPending ||
                  adminQpsQuery.isPending ||
                  adminPeakQuery.isPending
                ? "loading"
                : "ready",
          lastSuccessAt:
            adminSuccesses.length > 0 ? Math.max(...adminSuccesses) : null,
          disabledReason:
            missingTemplates.length > 0
              ? {
                  code: "backend-unsupported",
                  message: `监控模板未提供：${missingTemplates.join(", ")}`,
                }
              : null,
        },
      } satisfies MonitoringTrends;
    }

    if (discoveryPending || authPending) {
      return {
        qps: loadingTrend<TrendSeries[]>([]),
        peak: loadingTrend<number | null>(null),
        latency: loadingTrend<TrendSeries[]>([]),
        source: {
          ...sourceBase,
          state: "loading",
          lastSuccessAt: null,
          disabledReason: null,
        },
      } satisfies MonitoringTrends;
    }

    const reason: DisabledReason =
      disabledReason ??
      (mode === "capabilities-unknown"
        ? { code: "capabilities-unknown", message: "能力未知，暂不可用" }
        : { code: "requires-admin-api", message: "需要 Admin API" });

    return {
      qps: disabledTrend<TrendSeries[]>([]),
      peak: disabledTrend<number | null>(null),
      latency: disabledTrend<TrendSeries[]>([]),
      source: {
        ...sourceBase,
        state: "unknown",
        lastSuccessAt: null,
        disabledReason: reason,
      },
    } satisfies MonitoringTrends;
  }, [
    directEnabled,
    adminEnabled,
    availability,
    authenticated,
    discoveryPending,
    authPending,
    mode,
    range,
    templates,
    qpsQuery,
    peakQuery,
    latencyQuery,
    adminQpsQuery,
    adminPeakQuery,
    adminP50Query,
    adminP95Query,
    adminP99Query,
    templatesQuery,
  ]);
}
