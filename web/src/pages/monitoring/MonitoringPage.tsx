import type * as React from "react";
import {
  Activity,
  Database,
  Timer,
  TrendingUp,
} from "lucide-react";
import { useSearchParams } from "react-router-dom";
import type { TrendSeries } from "@/api/monitoring/types";
import { AlertsCard } from "@/components/monitoring/alerts-card";
import { DatasetFreshness } from "@/components/monitoring/dataset-freshness";
import { FeedFreshnessTable } from "@/components/monitoring/feed-freshness-table";
import {
  ServiceInstanceList,
  type ServiceInstanceRow,
  type ServiceInstanceSourceKind,
} from "@/components/monitoring/service-instance-list";
import { SourcePanel } from "@/components/monitoring/source-panel";
import type { SourceStatusProps } from "@/components/monitoring/source-status";
import { TrendChart } from "@/components/monitoring/trend-chart";
import { UpstreamStatus } from "@/components/monitoring/upstream-status";
import { UpstreamTable } from "@/components/monitoring/upstream-table";
import { FreshnessMatrix } from "@/components/freshness-matrix";
import { HelpTrigger } from "@/components/help-trigger";
import {
  MetricCard,
} from "@/components/metric-card";
import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { alignTrendPoints } from "@/components/monitoring/chart-option";
import {
  latestSampleTime,
  latestSeriesSum,
  latestSeriesValue,
} from "@/components/monitoring/series-metrics";
import {
  summarizeFeedFreshness,
  toDatasetFreshnessRows,
  toFeedFreshnessRow,
  toFeedFreshnessView,
  type FeedFreshnessRow,
} from "@/lib/freshness";
import { deriveVenueRows } from "@/lib/venues";
import {
  formatDurationSeconds,
  formatNumber,
  formatUtcDateTime,
  formatUtcTime,
} from "@/lib/format";
import {
  PROMETHEUS_POLL_INTERVAL_MS,
  STATUS_POLL_INTERVAL_MS,
  monitoringRangeFromParams,
  peakRangeLabel,
} from "@/lib/monitoring-config";
import {
  adminInstanceToServiceRow,
  errorText,
  lastSuccessAt,
  sourceState,
  useMonitoringSources,
  useMonitoringTrends,
  type TrendBlock,
  type TrendSourceView,
} from "@/pages/monitoring/use-monitoring-data";
import {
  deriveMonitoringAlerts,
  type MonitoringAlert,
  type SourceFailureInput,
} from "@/pages/monitoring/monitoring-alerts";
import { RangeControl } from "@/pages/monitoring/range-control";

type CardState = "loading" | "ready" | "error" | "unknown";

function maxOrNull(values: Array<number | null>): number | null {
  const finite = values.filter((value): value is number => value !== null);
  return finite.length > 0 ? Math.max(...finite) : null;
}

function lastSuccessLabel(value: number | null): string {
  return `最后成功 ${formatUtcDateTime(value)}`;
}

function ageSeconds(iso: string | null | undefined, nowMs: number): number | null {
  if (!iso) {
    return null;
  }
  const parsed = Date.parse(iso);
  return Number.isFinite(parsed) ? (nowMs - parsed) / 1000 : null;
}

function trendUnknownReason(
  block: TrendBlock<unknown>,
  source: TrendSourceView,
): string {
  switch (block.state) {
    case "ready":
      return "当前值不可用";
    case "loading":
      return "正在加载";
    case "empty":
      return "当前窗口没有样本";
    case "error":
      return "加载失败";
    case "unknown":
      return source.disabledReason?.message ?? "能力未知";
  }
}

function chartError(block: TrendBlock<unknown>): React.ReactNode | undefined {
  if (block.state !== "error") {
    return undefined;
  }
  return (
    <span>
      查询失败。最后成功 {formatUtcDateTime(block.lastSuccessAt)}。
      {block.error ? (
        <>
          <span className="text-muted-foreground"> Raw error </span>
          <span data-raw-evidence="true">{block.error}</span>
        </>
      ) : null}
    </span>
  );
}

function chartSeries(block: TrendBlock<TrendSeries[]>): TrendSeries[] {
  return block.state === "ready" ? block.value : [];
}

/** QPS is additive across route lines; charts and the sparkline agree. */
function qpsSparklineValues(series: TrendSeries[]): number[] {
  return alignTrendPoints(series).rows
    .map((row) =>
      row.values.filter(
        (value): value is number => value !== null && Number.isFinite(value),
      ),
    )
    .filter((values) => values.length > 0)
    .map((values) => values.reduce((total, value) => total + value, 0));
}

function MiniSparkline({ values }: { values: number[] }) {
  if (values.length < 2) {
    return null;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const points = values
    .map((value, index) => {
      const x = (index / (values.length - 1)) * 100;
      const y = 18 - ((value - min) / span) * 16;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <svg
      viewBox="0 0 100 20"
      preserveAspectRatio="none"
      role="img"
      aria-label="Realtime QPS 最近样本趋势"
      className="block h-7 w-full"
    >
      <line
        x1="0"
        y1="18"
        x2="100"
        y2="18"
        className="stroke-card-border"
        strokeWidth="1"
      />
      <polyline
        points={points}
        fill="none"
        className="stroke-chart-series-1"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  );
}

export function MonitoringPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const range = monitoringRangeFromParams(searchParams);
  const sources = useMonitoringSources();
  const trends = useMonitoringTrends(range);
  const nowMs = Date.now();
  const statusPollWindow = `当前快照（${STATUS_POLL_INTERVAL_MS / 1000} 秒轮询）`;

  const setRange = (id: string) => {
    const next = new URLSearchParams(searchParams);
    next.set("range", id);
    setSearchParams(next, { replace: true });
  };

  const collectorTransient = sourceState(sources.collector);
  const collectorHealth = sources.collector.data?.health ?? null;
  const feedReady =
    sources.directOpsEnabled &&
    !sources.collector.isError &&
    collectorTransient === "ready";
  const feedSummary = feedReady && collectorHealth
    ? summarizeFeedFreshness(collectorHealth)
    : null;
  const feedViews = feedReady && collectorHealth
    ? collectorHealth.tracked.map(toFeedFreshnessView)
    : [];
  const feedRows: FeedFreshnessRow[] = feedViews.map(toFeedFreshnessRow);

  const feedState: CardState = !sources.directOpsEnabled
    ? "unknown"
    : sources.collector.isError
      ? "error"
      : collectorTransient === "loading"
        ? "loading"
        : "ready";
  const feedLastSuccessAt = lastSuccessAt(sources.collector);

  const datasetState = sourceState(sources.dataset);
  const datasetRows =
    datasetState === "ready" && sources.dataset.data
      ? toDatasetFreshnessRows(sources.dataset.data, nowMs)
      : [];
  const datasetLastFlushed =
    datasetState === "ready"
      ? sources.dataset.data?.last_flushed_ts ?? null
      : null;
  const datasetAgeS = ageSeconds(datasetLastFlushed, nowMs);
  const datasetValue =
    datasetAgeS === null ? null : formatDurationSeconds(datasetAgeS);
  const datasetUnknownReason =
    datasetValue !== null
      ? undefined
      : datasetState === "loading"
        ? "正在加载"
        : datasetState === "error"
          ? "加载失败"
          : "尚无 flush 记录";

  const instrumentsState = sourceState(sources.instruments);
  // Feed Freshness must not survive a failed collector refetch: derive venue
  // freshness only from a collector snapshot that is currently ready.
  const venueFreshnessHealth = feedReady ? collectorHealth : null;
  const venueRows =
    instrumentsState === "ready" && sources.instruments.data
      ? deriveVenueRows(
          sources.instruments.data.instruments,
          venueFreshnessHealth,
        )
      : [];
  const venueStaleCount = venueRows.filter(
    (row) => row.staleCount > 0,
  ).length;

  const directServiceRows: ServiceInstanceRow[] = [
    {
      role: "collector",
      instanceId: null,
      service: sources.collector.data?.service ?? null,
      ready:
        sources.collectorReady.data?.ready ??
        sources.collector.data?.ready ??
        null,
      readyReason: sources.collectorReady.data?.reason ?? null,
      uptimeS: sources.collector.data?.uptime_s ?? null,
      statusState: sourceState(sources.collector),
      lastSuccessAt: lastSuccessAt(sources.collector),
      details: sources.collector.data
        ? [
            `Service ${sources.collector.data.service}`,
            `Feed ${sources.collector.data.feed ?? "未暴露"}`,
            `Bars 写入 ${sources.collector.data.bars_written}`,
          ]
        : [],
      errorMessage: sources.collector.isError
        ? errorText(sources.collector.error)
        : null,
    },
    {
      role: "api",
      instanceId: null,
      service: sources.api.data?.service ?? null,
      ready: sources.apiReady.data?.ready ?? null,
      readyReason: sources.apiReady.data?.reason ?? null,
      uptimeS: sources.api.data?.uptime_s ?? null,
      statusState: sourceState(sources.api),
      lastSuccessAt: lastSuccessAt(sources.api),
      details: sources.api.data
        ? [
            `Service ${sources.api.data.service}`,
            `HTTP ${sources.api.data.http.host}:${
              sources.api.data.http.port ?? "—"
            }`,
            `ZMQ 消息 ${sources.api.data.messages_received} 收 / ${sources.api.data.messages_forwarded} 转发`,
          ]
        : [],
      errorMessage: sources.api.isError ? errorText(sources.api.error) : null,
    },
    {
      role: "replay",
      instanceId: null,
      service: sources.replay.data?.service ?? null,
      ready: sources.replayReady.data?.ready ?? null,
      readyReason: sources.replayReady.data?.reason ?? null,
      uptimeS: sources.replay.data?.uptime_s ?? null,
      statusState: sourceState(sources.replay),
      lastSuccessAt: lastSuccessAt(sources.replay),
      details: sources.replay.data
        ? [
            `Service ${sources.replay.data.service}`,
            `Sessions ${sources.replay.data.sessions.length}`,
            `Instrument Snapshot ${
              sources.replay.data.instrument_snapshot.loaded
                ? "loaded"
                : "missing"
            }`,
          ]
        : [],
      errorMessage: sources.replay.isError
        ? errorText(sources.replay.error)
        : null,
    },
  ];

  const adminServiceState = sourceState(sources.adminInstances);
  const serviceRows: ServiceInstanceRow[] =
    sources.serviceSource.kind === "admin"
      ? (sources.adminInstances.data ?? []).map((instance) => ({
          ...adminInstanceToServiceRow(instance),
          // Cached Admin rows must not keep a Ready badge after a failed
          // refetch; their state follows the query that produced them.
          statusState: adminServiceState === "ready" ? "ready" : adminServiceState,
        }))
      : sources.serviceSource.kind === "gap"
        ? []
        : directServiceRows;
  const serviceState: CardState =
    sources.serviceSource.kind === "gap"
      ? "unknown"
      : sources.serviceSource.kind === "admin"
        ? adminServiceState
        : directServiceRows.some((row) => row.statusState === "error")
          ? "error"
          : directServiceRows.some((row) => row.statusState === "loading")
            ? "loading"
            : "ready";
  const serviceLastSuccessAt =
    sources.serviceSource.kind === "admin"
      ? lastSuccessAt(sources.adminInstances)
      : maxOrNull(directServiceRows.map((row) => row.lastSuccessAt));
  const serviceErrorMessage =
    sources.serviceSource.kind === "admin" && sources.adminInstances.isError
      ? errorText(sources.adminInstances.error)
      : null;
  const notReadyServices = serviceRows.filter(
    (row) => row.statusState === "ready" && row.ready === false,
  );
  const readyServices = serviceRows.filter(
    (row) => row.statusState === "ready" && row.ready === true,
  );
  // Current Contract rows are one process per Service Role. Admin API rows
  // are concrete Service Instances. Never call one the other.
  const serviceSourceKind: ServiceInstanceSourceKind =
    sources.serviceSource.kind === "direct" ? "role" : "instance";
  const serviceSubject =
    serviceSourceKind === "role" ? "Service Role" : "Service Instance";

  const qpsSeries = chartSeries(trends.qps);
  const latencySeries = chartSeries(trends.latency);
  const qpsValue =
    trends.qps.state === "ready" ? latestSeriesSum(trends.qps.value) : null;
  const peakValue =
    trends.peak.state === "ready" ? trends.peak.value : null;
  const p99 =
    trends.latency.state === "ready"
      ? latestSeriesValue(trends.latency.value, "p99")
      : null;
  const p95 =
    trends.latency.state === "ready"
      ? latestSeriesValue(trends.latency.value, "p95")
      : null;
  const p50 =
    trends.latency.state === "ready"
      ? latestSeriesValue(trends.latency.value, "p50")
      : null;
  const qpsSampleTime =
    trends.qps.state === "ready" ? latestSampleTime(trends.qps.value) : null;

  const qpsUnknown =
    qpsValue === null ? trendUnknownReason(trends.qps, trends.source) : undefined;
  const peakUnknown =
    peakValue === null
      ? trendUnknownReason(trends.peak, trends.source)
      : undefined;
  const latencyUnknown =
    p99 === null
      ? trendUnknownReason(trends.latency, trends.source)
      : undefined;

  const feedOverallStatus = !sources.directOpsEnabled ? (
    <StatusBadge tone="warning">需要 Admin API</StatusBadge>
  ) : feedState === "error" ? (
    <StatusBadge tone="critical">未知</StatusBadge>
  ) : feedState === "loading" ? (
    <StatusBadge tone="unknown">加载中</StatusBadge>
  ) : feedSummary && feedSummary.stale > 0 ? (
    <StatusBadge tone="critical" live>
      {feedSummary.tracked} 项跟踪，{feedSummary.stale} 项失活
    </StatusBadge>
  ) : feedSummary ? (
    <StatusBadge tone="good" live>
      {feedSummary.tracked} 项跟踪
    </StatusBadge>
  ) : (
    <StatusBadge tone="unknown">无跟踪项</StatusBadge>
  );

  const feedSummaryBadge = !sources.directOpsEnabled ? (
    <StatusBadge tone="warning">Feed Freshness 能力未知</StatusBadge>
  ) : feedState === "error" ? (
    <StatusBadge tone="warning">Feed Freshness 加载失败</StatusBadge>
  ) : feedState === "loading" ? (
    <StatusBadge tone="unknown">Feed Freshness 加载中</StatusBadge>
  ) : feedSummary && feedSummary.stale > 0 ? (
    <StatusBadge tone="critical" live>
      {feedSummary.stale} 项 Feed Freshness 失活
    </StatusBadge>
  ) : feedSummary && feedSummary.unknown > 0 ? (
    <StatusBadge tone="warning">
      {feedSummary.unknown} 项 Feed Freshness 未知
    </StatusBadge>
  ) : feedSummary && feedSummary.tracked > 0 ? (
    <StatusBadge tone="good" live>
      {feedSummary.tracked} 项 Feed Freshness 跟踪
    </StatusBadge>
  ) : (
    <StatusBadge tone="unknown">Feed Freshness 无跟踪项</StatusBadge>
  );

  const serviceSummaryBadge =
    sources.serviceSource.kind === "gap" ? (
      <StatusBadge tone="warning">{serviceSubject} 需要 Admin API</StatusBadge>
    ) : serviceState === "error" ? (
      <StatusBadge tone="warning">{serviceSubject} 状态未知</StatusBadge>
    ) : serviceState === "loading" ? (
      <StatusBadge tone="unknown">{serviceSubject} 加载中</StatusBadge>
    ) : notReadyServices.length > 0 ? (
      <StatusBadge tone="critical" live>
        {notReadyServices.length} 个 {serviceSubject} 未就绪
      </StatusBadge>
    ) : serviceRows.length === 0 ? (
      <StatusBadge tone="unknown">{serviceSubject} 无运行实例</StatusBadge>
    ) : (
      <StatusBadge tone="good" live>
        {readyServices.length} / {serviceRows.length} {serviceSubject} Ready
      </StatusBadge>
    );

  const upstreamSummaryBadge =
    instrumentsState === "loading" ? (
      <StatusBadge tone="unknown">上游观测加载中</StatusBadge>
    ) : instrumentsState === "error" ? (
      <StatusBadge tone="warning">上游观测加载失败</StatusBadge>
    ) : venueStaleCount > 0 ? (
      <StatusBadge tone="critical">{venueStaleCount} 个 Venue Feed 失活</StatusBadge>
    ) : sources.directOpsEnabled && sources.collector.isError ? (
      <StatusBadge tone="warning">上游 Feed Freshness 未知</StatusBadge>
    ) : venueRows.length === 0 ? (
      <StatusBadge tone="unknown">上游观测无 Instrument</StatusBadge>
    ) : (
      <StatusBadge tone="good">{venueRows.length} 个 Venue 已观测</StatusBadge>
    );

  const statusLastSuccess = maxOrNull([
    lastSuccessAt(sources.collector),
    lastSuccessAt(sources.dataset),
    lastSuccessAt(sources.instruments),
    trends.source.lastSuccessAt,
  ]);

  const sourceFailures: SourceFailureInput[] = [
    {
      when: sources.collector.isError,
      label: "Collector /status",
      error: sources.collector.isError
        ? errorText(sources.collector.error)
        : null,
      lastSuccessAt: lastSuccessAt(sources.collector),
    },
    {
      when: sources.api.isError,
      label: "Service /status",
      error: sources.api.isError ? errorText(sources.api.error) : null,
      lastSuccessAt: lastSuccessAt(sources.api),
    },
    {
      when: sources.replay.isError,
      label: "Replay /status",
      error: sources.replay.isError ? errorText(sources.replay.error) : null,
      lastSuccessAt: lastSuccessAt(sources.replay),
    },
    {
      when: sources.dataset.isError,
      label: "Dataset /api/v1/status",
      error: sources.dataset.isError
        ? errorText(sources.dataset.error)
        : null,
      lastSuccessAt: lastSuccessAt(sources.dataset),
    },
    {
      when: sources.instruments.isError,
      label: "Instrument Reference /api/v1/instruments",
      error: sources.instruments.isError
        ? errorText(sources.instruments.error)
        : null,
      lastSuccessAt: lastSuccessAt(sources.instruments),
    },
    {
      when:
        sources.serviceSource.kind === "admin" &&
        sources.adminInstances.isError,
      label: "Service Instance /api/v1/admin/service-instances",
      error: sources.adminInstances.isError
        ? errorText(sources.adminInstances.error)
        : null,
      lastSuccessAt: lastSuccessAt(sources.adminInstances),
    },
    {
      when: trends.source.state === "error",
      label: trends.source.label,
      error:
        trends.qps.error ?? trends.peak.error ?? trends.latency.error,
      lastSuccessAt: trends.source.lastSuccessAt,
    },
  ]
    .filter((failure) => failure.when)
    .map(({ label, error, lastSuccessAt: lastSuccess }) => ({
      label,
      error,
      lastSuccessAt: lastSuccess,
    }));

  const capabilityGaps: Array<{ title: string; reason: string }> = [];
  if (trends.source.state === "unknown" && trends.source.disabledReason) {
    capabilityGaps.push({
      title: "监控趋势能力未知",
      reason: trends.source.disabledReason.message,
    });
  }

  const alerts: MonitoringAlert[] = deriveMonitoringAlerts({
    directOpsEnabled: sources.directOpsEnabled,
    opsGapReason: sources.opsGapReason,
    staleFeeds: feedRows.filter((row) => row.stale),
    staleVenues: venueRows
      .filter((row) => row.staleCount > 0)
      .map((row) => ({ venue: row.venue, staleCount: row.staleCount })),
    notReadyServices: notReadyServices.map((row) => ({
      role: row.role,
      instanceId: row.instanceId ?? null,
      reason: row.readyReason,
    })),
    sourceFailures,
    capabilityGaps,
  });

  const qpsDescription = [
    `数据源 ${trends.source.label}。`,
    `统计窗口 ${trends.source.windowLabel}。`,
    `QPS rate 窗口 ${trends.source.rateWindow}。`,
    "HTTP counter 完成时记录，进行中的流式下载不计入。",
    `最后成功 ${formatUtcDateTime(trends.qps.lastSuccessAt)}。`,
  ].join("");
  const latencyDescription = [
    `数据源 ${trends.source.label}。`,
    `统计窗口 ${trends.source.windowLabel}。`,
    `histogram rate 窗口 ${trends.source.rateWindow}。`,
    "含完整响应流耗时。",
    `最后成功 ${formatUtcDateTime(trends.latency.lastSuccessAt)}。`,
  ].join("");

  const sourceRows: SourceStatusProps[] = [
    {
      source: "Service /status",
      window: statusPollWindow,
      state: sources.directOpsEnabled ? sourceState(sources.api) : ("unknown" as const),
      lastSuccessAt: sources.directOpsEnabled ? lastSuccessAt(sources.api) : null,
      reason: sources.directOpsEnabled ? null : sources.opsGapReason,
    },
    {
      source: "Collector /status",
      window: statusPollWindow,
      state: sources.directOpsEnabled
        ? collectorTransient
        : ("unknown" as const),
      lastSuccessAt: sources.directOpsEnabled
        ? lastSuccessAt(sources.collector)
        : null,
      reason: sources.directOpsEnabled ? null : sources.opsGapReason,
    },
    {
      source: "Replay /status",
      window: statusPollWindow,
      state: sources.directOpsEnabled
        ? sourceState(sources.replay)
        : ("unknown" as const),
      lastSuccessAt: sources.directOpsEnabled
        ? lastSuccessAt(sources.replay)
        : null,
      reason: sources.directOpsEnabled ? null : sources.opsGapReason,
    },
    {
      source: "Dataset /api/v1/status",
      window: statusPollWindow,
      state: datasetState,
      lastSuccessAt: lastSuccessAt(sources.dataset),
      reason: null,
    },
    {
      source: "Instrument Reference /api/v1/instruments",
      window: statusPollWindow,
      state: instrumentsState,
      lastSuccessAt: lastSuccessAt(sources.instruments),
      reason: null,
    },
    {
      source: trends.source.label,
      window: trends.source.windowLabel,
      state:
        trends.source.state === "loading"
          ? ("loading" as const)
          : trends.source.state === "error"
            ? ("error" as const)
            : trends.source.state === "unknown"
              ? ("unknown" as const)
              : ("ready" as const),
      lastSuccessAt: trends.source.lastSuccessAt,
      reason:
        trends.source.disabledReason?.message ??
        (trends.source.state === "error" ? "查询失败" : null),
    },
  ];

  const snapshot = (
    <span className="inline-flex items-center gap-1 text-[11px] text-faint-foreground">
      <Activity className="size-3.5 shrink-0" aria-hidden="true" />
      {lastSuccessLabel(statusLastSuccess)}
    </span>
  );

  return (
    <>
      <PageHeader
        title="监控总览"
        help={
          <HelpTrigger
            label="监控总览说明"
            content={`趋势来自 ${trends.source.label}。状态来自 per-Role /status 与 Dataset /api/v1/status。Feed Freshness 与 Dataset Freshness 是不同信号。Freshness 不代表连接健康。`}
            side="bottom"
          />
        }
        status={
          <>
            {feedSummaryBadge}
            {serviceSummaryBadge}
            {upstreamSummaryBadge}
            <StatusBadge tone="unknown">上游连接 未暴露</StatusBadge>
            {snapshot}
          </>
        }
        actions={
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-xs font-medium text-muted-foreground">
              时间范围
            </span>
            <RangeControl value={range.id} onChange={setRange} />
            <HelpTrigger
              label="时间范围说明"
              content={`所有趋势与 Peak QPS 共用该时间范围。Peak QPS 范围 ${peakRangeLabel(
                range,
              )}，${range.peakRateWindow} rate，${range.peakStepSeconds}s step。`}
              side="bottom"
            />
          </div>
        }
      />

      <section
        aria-label="关键指标"
        className="mt-5 grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4"
      >
        <MetricCard
          label="Realtime QPS"
          value={qpsValue === null ? null : formatNumber(qpsValue, 2)}
          unit="req/s"
          icon={Activity}
          help={{
            label: "Realtime QPS 说明",
            content: `1 分钟滑动窗口的请求速率，取最近一个成功样本。${qpsDescription}`,
          }}
          unknownReason={qpsUnknown}
          detail={
            qpsSampleTime === null
              ? `最近样本 未暴露，rate 窗口 ${trends.source.rateWindow}`
              : `最近样本 ${formatUtcTime(
                  qpsSampleTime,
                )}，rate 窗口 ${trends.source.rateWindow}`
          }
          evidence={lastSuccessLabel(trends.qps.lastSuccessAt)}
          visual={<MiniSparkline values={qpsSparklineValues(qpsSeries)} />}
        />
        <MetricCard
          label="Peak QPS"
          value={peakValue === null ? null : formatNumber(peakValue, 2)}
          unit="req/s"
          icon={TrendingUp}
          help={{
            label: "Peak QPS 说明",
            content: `当前范围 ${peakRangeLabel(range)} 内 ${range.peakRateWindow} rate 的最大采样值，采样步长 ${range.peakStepSeconds}s。不是瞬时峰值。最后成功 ${formatUtcDateTime(
              trends.peak.lastSuccessAt,
            )}。`,
          }}
          unknownReason={peakUnknown}
          detail={`${peakRangeLabel(range)}，${range.peakRateWindow} rate，${range.peakStepSeconds}s step。采样最大值，不是瞬时峰值。`}
          evidence={lastSuccessLabel(trends.peak.lastSuccessAt)}
        />
        <MetricCard
          label="HTTP P99"
          value={p99 === null ? null : formatNumber(p99, 2)}
          unit="ms"
          icon={Timer}
          help={{
            label: "HTTP P99 说明",
            content: `P50/P95/P99 来自 histogram，含完整响应流耗时。${latencyDescription}`,
          }}
          unknownReason={latencyUnknown}
          detail={`P50 ${formatNumber(
            p50,
            2,
          )} ms，P95 ${formatNumber(p95, 2)} ms。含完整响应流耗时。`}
          evidence={lastSuccessLabel(trends.latency.lastSuccessAt)}
        />
        <MetricCard
          label="Dataset Freshness"
          value={datasetValue}
          unit={datasetValue === null ? undefined : "前"}
          icon={Database}
          help={{
            label: "Dataset Freshness 指标说明",
            content:
              "最近一次 Dataset 落盘距今的时间。独立于 Feed Freshness。契约不提供 Dataset 阈值，不判定 stale。",
          }}
          unknownReason={datasetUnknownReason}
          detail={
            datasetLastFlushed === null
              ? "最近落盘 未暴露"
              : `最近落盘 ${formatUtcDateTime(datasetLastFlushed)}`
          }
          evidence={lastSuccessLabel(lastSuccessAt(sources.dataset))}
        />
      </section>

      <section
        aria-label="趋势"
        className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2"
      >
        <TrendChart
          className="min-w-0"
          title="QPS 趋势"
          help={{ label: "QPS 趋势说明", content: qpsDescription }}
          unit="req/s"
          series={qpsSeries}
          state={trends.qps.state}
          unknownCause={trends.source.disabledReason ? "not-exposed" : "failed"}
          errorMessage={chartError(trends.qps)}
        />
        <TrendChart
          className="min-w-0"
          title="HTTP 延迟趋势"
          help={{ label: "HTTP 延迟趋势说明", content: latencyDescription }}
          unit="ms"
          series={latencySeries}
          state={trends.latency.state}
          unknownCause={trends.source.disabledReason ? "not-exposed" : "failed"}
          errorMessage={chartError(trends.latency)}
        />
      </section>

      <section className="mt-8" aria-labelledby="anomaly-title">
        <h2
          id="anomaly-title"
          className="text-lg font-semibold tracking-tight text-foreground"
        >
          异常定位
        </h2>
        <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-12">
          <FreshnessMatrix
            className="xl:col-span-7"
            rows={feedRows}
            state={feedState}
            unknownReason={sources.opsGapReason ?? "Feed Freshness 能力未知"}
            errorMessage={
              sources.collector.isError
                ? errorText(sources.collector.error)
                : null
            }
            lastSuccessAt={feedLastSuccessAt}
            overallStatus={feedOverallStatus}
            description={`来自 collector /status.health.tracked。倒计时 = threshold_s - age_s。stale 以服务端为准。${lastSuccessLabel(
              feedLastSuccessAt,
            )}。`}
            help={{
              label: "Feed Freshness 说明",
              content:
                "age 超过服务端 threshold 即标记失活。Freshness 只表示最近事件年龄，不等同于连接健康。",
            }}
          />
          <ServiceInstanceList
            className="xl:col-span-5"
            sourceKind={serviceSourceKind}
            rows={serviceRows}
            state={serviceState}
            unknownReason={sources.opsGapReason ?? `${serviceSubject} 能力未知`}
            lastSuccessAt={serviceLastSuccessAt}
            errorMessage={serviceErrorMessage}
            overallStatus={serviceSummaryBadge}
            help={{
              label: `${serviceSubject} 说明`,
              content:
                "Service Role 是进程职责 collector/api/replay。Service Instance 是某个 Role 的一个实际运行实例。Current Contract 只暴露每个 Role 的当前进程信息，Admin API 可暴露实例 ID。没有集群、Leader 或分片推断。",
            }}
          />
          <AlertsCard
            className="xl:col-span-5"
            alerts={alerts}
            help={{
              label: "页面内告警说明",
              content:
                "页面内事件时间线，只由已加载的真实状态与错误派生，不发送外部通知。",
            }}
          />
          <UpstreamStatus
            className="xl:col-span-7"
            rows={venueRows}
            state={instrumentsState}
            lastSuccessAt={lastSuccessAt(sources.instruments)}
            errorMessage={
              sources.instruments.isError
                ? errorText(sources.instruments.error)
                : null
            }
            help={{
              label: "上游状态说明",
              content:
                "连接、心跳、重连计数与网络能力未暴露。Feed Freshness 只表示最近事件年龄，不证明连接健康。",
            }}
          />
        </div>
      </section>

      <section className="mt-8" aria-labelledby="detail-title">
        <h2
          id="detail-title"
          className="text-lg font-semibold tracking-tight text-foreground"
        >
          数据明细
        </h2>
        <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-2">
          <FeedFreshnessTable
            rows={feedRows}
            state={feedState}
            unknownReason={sources.opsGapReason ?? "Feed Freshness 能力未知"}
            errorMessage={
              sources.collector.isError
                ? errorText(sources.collector.error)
                : null
            }
            lastSuccessAt={feedLastSuccessAt}
            help={{
              label: "Feed Freshness 明细说明",
              content:
                "表格保留服务端原始 age_s、threshold_s、stale 与最后事件时间。",
            }}
          />
          <DatasetFreshness
            state={datasetState}
            lastSuccessAt={lastSuccessAt(sources.dataset)}
            errorMessage={
              sources.dataset.isError
                ? errorText(sources.dataset.error)
                : null
            }
            rows={datasetRows}
            help={{
              label: "Dataset Freshness 明细说明",
              content:
                "最近一次 Dataset 落盘距今的时间。独立于 Feed Freshness。契约不提供 Dataset 阈值，不判定 stale。",
            }}
          />
          <UpstreamTable
            className="xl:col-span-2"
            state={instrumentsState}
            lastSuccessAt={lastSuccessAt(sources.instruments)}
            errorMessage={
              sources.instruments.isError
                ? errorText(sources.instruments.error)
                : null
            }
            rows={venueRows}
            help={{
              label: "上游观测明细说明",
              content:
                "Venue 来自 /api/v1/instruments。连接、心跳、重连计数与网络能力未暴露，不从 Freshness 推断。",
            }}
          />
          <SourcePanel
            className="xl:col-span-2"
            rows={sourceRows}
            note={`Service /status 每 ${
              STATUS_POLL_INTERVAL_MS / 1000
            } 秒轮询。Prometheus 每 ${
              PROMETHEUS_POLL_INTERVAL_MS / 1000
            } 秒轮询。所有趋势共用同一时间范围。`}
          />
        </div>
      </section>
    </>
  );
}
