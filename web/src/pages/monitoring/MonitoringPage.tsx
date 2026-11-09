import { useSearchParams } from "react-router-dom";
import { DatasetFreshness } from "@/components/monitoring/dataset-freshness";
import { ServiceInstanceList, type ServiceInstanceRow } from "@/components/monitoring/service-instance-list";
import { SourcePanel } from "@/components/monitoring/source-panel";
import { SourceStatus } from "@/components/monitoring/source-status";
import { TrendChart } from "@/components/monitoring/trend-chart";
import { UpstreamTable } from "@/components/monitoring/upstream-table";
import { FreshnessMatrix, type FreshnessRow } from "@/components/freshness-matrix";
import { KpiCell } from "@/components/kpi-cell";
import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { ErrorState, LoadingState } from "@/components/states";
import { FilterRow } from "@/components/toolbar";
import { PanelDescription, PanelHeader, PanelTitle } from "@/components/ui/panel";
import type { TrendSeries } from "@/api/monitoring/types";
import {
  summarizeFeedFreshness,
  toDatasetFreshnessRows,
  toFeedFreshnessView,
} from "@/lib/freshness";
import { deriveVenueRows } from "@/lib/venues";
import {
  formatNumber,
  formatUtcDateTime,
  formatUtcTime,
} from "@/lib/format";
import {
  DEFAULT_MONITORING_RANGE_ID,
  PROMETHEUS_POLL_INTERVAL_MS,
  STATUS_POLL_INTERVAL_MS,
  getMonitoringRange,
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
import { RangeControl } from "@/pages/monitoring/range-control";

function latestFiniteValue(series: TrendSeries): number | null {
  for (let index = series.points.length - 1; index >= 0; index -= 1) {
    const value = series.points[index]?.value;
    if (value !== null && value !== undefined && Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

/** QPS series are additive across route labels. */
function latestFiniteSum(series: TrendSeries[]): number | null {
  const values = series
    .map(latestFiniteValue)
    .filter((value): value is number => value !== null);
  if (values.length === 0) {
    return null;
  }
  return values.reduce((total, value) => total + value, 0);
}

function latestNamedValue(series: TrendSeries[], name: string): number | null {
  const matches = series.filter(
    (item) => item.name === name || item.name.startsWith(`${name} ·`),
  );
  const values = matches
    .map(latestFiniteValue)
    .filter((value): value is number => value !== null);
  return values.length > 0 ? Math.max(...values) : null;
}

function latestSampleTime(series: TrendSeries[]): number | null {
  let latest: number | null = null;
  for (const item of series) {
    for (const point of item.points) {
      if (point.value === null || !Number.isFinite(point.value)) {
        continue;
      }
      if (latest === null || point.ts > latest) {
        latest = point.ts;
      }
    }
  }
  return latest;
}

function trendUnknownReason(
  block: TrendBlock<unknown>,
  source: TrendSourceView,
): string | null {
  switch (block.state) {
    case "ready":
      return null;
    case "loading":
      return "正在加载…";
    case "empty":
      return "当前窗口没有样本";
    case "error":
      return "能力未知";
    case "unknown":
      return source.disabledReason?.message ?? "能力未知";
  }
}

function trendChartState(block: TrendBlock<unknown>) {
  return block.state;
}

function chartError(block: TrendBlock<unknown>): string | undefined {
  if (block.state === "error") {
    return `${block.error ?? "查询失败"}。最后成功：${formatUtcDateTime(
      block.lastSuccessAt,
    )}`;
  }
  return undefined;
}

function chartSeries(block: TrendBlock<TrendSeries[]>): TrendSeries[] {
  return block.state === "ready" ? block.value : [];
}

export function MonitoringPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const range = monitoringRangeFromParams(searchParams);
  const sources = useMonitoringSources();
  const trends = useMonitoringTrends(range);

  const setRange = (id: string) => {
    const next = new URLSearchParams(searchParams);
    next.set("range", id);
    setSearchParams(next, { replace: true });
  };

  const collectorHealth = sources.collector.data?.health ?? null;
  const collectorError = sources.collector.isError;
  const collectorReadyState = sourceState(sources.collector);
  const feedSummary =
    collectorHealth && !collectorError
      ? summarizeFeedFreshness(collectorHealth)
      : null;
  const feedViews =
    collectorHealth && !collectorError
      ? collectorHealth.tracked.map(toFeedFreshnessView)
      : [];
  const feedRows: FreshnessRow[] = feedViews.map((view) => ({
    instrumentId: view.instrumentId,
    feed: view.feed,
    interval: view.interval as FreshnessRow["interval"],
    ageS: view.ageS,
    thresholdS: view.thresholdS,
    stale: view.tone === "unknown" ? null : view.stale,
  }));

  const datasetState = sourceState(sources.dataset);
  const datasetRows =
    datasetState === "ready" && sources.dataset.data
      ? toDatasetFreshnessRows(sources.dataset.data, Date.now())
      : [];

  const instrumentsState = sourceState(sources.instruments);
  const venueRows =
    instrumentsState === "ready" && sources.instruments.data
      ? deriveVenueRows(sources.instruments.data.instruments, collectorHealth)
      : [];

  const directServiceRows: ServiceInstanceRow[] = [
    {
      role: "collector",
      service: sources.collector.data?.service ?? null,
      ready:
        sources.collectorReady.data?.ready ??
        sources.collector.data?.ready ??
        null,
      readyReason: sources.collectorReady.data?.reason ?? null,
      uptimeS: sources.collector.data?.uptime_s ?? null,
      statusState: collectorReadyState,
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
      errorMessage: sources.api.isError
        ? errorText(sources.api.error)
        : null,
    },
    {
      role: "replay",
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
            `Snapshot ${
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

  const serviceRows: ServiceInstanceRow[] =
    sources.serviceSource.kind === "admin"
      ? (sources.adminInstances.data ?? []).map(adminInstanceToServiceRow)
      : sources.serviceSource.kind === "gap"
        ? []
        : directServiceRows;

  const qpsValue =
    trends.qps.state === "ready" ? latestFiniteSum(trends.qps.value) : null;
  const peakValue =
    trends.peak.state === "ready" ? trends.peak.value : null;
  const p99 =
    trends.latency.state === "ready"
      ? latestNamedValue(trends.latency.value, "P99")
      : null;
  const p95 =
    trends.latency.state === "ready"
      ? latestNamedValue(trends.latency.value, "P95")
      : null;
  const p50 =
    trends.latency.state === "ready"
      ? latestNamedValue(trends.latency.value, "P50")
      : null;
  const qpsSampleTime =
    trends.qps.state === "ready" ? latestSampleTime(trends.qps.value) : null;

  const qpsUnknown = qpsValue === null ? trendUnknownReason(trends.qps, trends.source) : null;
  const peakUnknown =
    peakValue === null ? trendUnknownReason(trends.peak, trends.source) : null;
  const latencyUnknown =
    p99 === null
      ? trendUnknownReason(trends.latency, trends.source)
      : null;
  const feedUnknown = !sources.directOpsEnabled
    ? (sources.opsGapReason ?? "能力未知")
    : feedSummary
      ? null
      : collectorError
        ? "能力未知"
        : "正在加载…";

  const headerStatus = !sources.directOpsEnabled ? (
    <StatusBadge tone="warning" live>
      Feed Freshness 需要 Admin API 接口
    </StatusBadge>
  ) : collectorError ? (
    <StatusBadge tone="warning" live>
      Feed Freshness 未知
    </StatusBadge>
  ) : feedSummary ? (
    feedSummary.stale > 0 ? (
      <StatusBadge tone="critical" live>
        {feedSummary.stale} 项 Feed Freshness 失活
      </StatusBadge>
    ) : feedSummary.tracked > 0 ? (
      <StatusBadge tone="good" live>
        {feedSummary.tracked} 项 Feed Freshness 跟踪
      </StatusBadge>
    ) : (
      <StatusBadge tone="unknown">Feed Freshness 无跟踪项</StatusBadge>
    )
  ) : (
    <StatusBadge tone="unknown">等待 Feed Freshness</StatusBadge>
  );

  const feedOverallStatus = !sources.directOpsEnabled ? (
    <StatusBadge tone="warning">需要 Admin API 接口</StatusBadge>
  ) : collectorError || !feedSummary ? (
      <StatusBadge tone="critical">未知</StatusBadge>
    ) : feedSummary.stale > 0 ? (
      <StatusBadge tone="critical" live>
        {feedSummary.tracked} 项跟踪 · {feedSummary.stale} 项失活
      </StatusBadge>
    ) : (
      <StatusBadge tone="good" live>
        {feedSummary.tracked} 项跟踪
      </StatusBadge>
    );

  const opsSourceRow = (
    source: string,
    state: "loading" | "ready" | "error",
    lastSuccessAtValue: number | null,
  ) => ({
    source,
    window: "当前快照（5 秒轮询）",
    state: sources.directOpsEnabled ? state : ("unknown" as const),
    lastSuccessAt: sources.directOpsEnabled ? lastSuccessAtValue : null,
    reason: sources.directOpsEnabled ? null : sources.opsGapReason,
  });

  const sourceRows = [
    opsSourceRow(
      "Service /status",
      sourceState(sources.api),
      lastSuccessAt(sources.api),
    ),
    opsSourceRow(
      "Collector /status",
      collectorReadyState,
      lastSuccessAt(sources.collector),
    ),
    opsSourceRow(
      "Replay /status",
      sourceState(sources.replay),
      lastSuccessAt(sources.replay),
    ),
    {
      source: "Dataset /api/v1/status",
      window: "当前快照（5 秒轮询）",
      state: datasetState,
      lastSuccessAt: lastSuccessAt(sources.dataset),
    },
    {
      source: "Instrument Reference /api/v1/instruments",
      window: "当前快照（5 秒轮询）",
      state: instrumentsState,
      lastSuccessAt: lastSuccessAt(sources.instruments),
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

  const qpsDescription = [
    `数据源：${trends.source.label}`,
    `统计窗口：${trends.source.windowLabel}`,
    `QPS rate 窗口：${trends.source.rateWindow}`,
    `最后成功：${formatUtcDateTime(trends.qps.lastSuccessAt)}`,
  ].join(" · ");
  const latencyDescription = [
    `数据源：${trends.source.label}`,
    `统计窗口：${trends.source.windowLabel}`,
    `histogram rate 窗口：${trends.source.rateWindow}`,
    "含完整响应流耗时（包含流式响应体）",
    `最后成功：${formatUtcDateTime(trends.latency.lastSuccessAt)}`,
  ].join(" · ");

  const sourceNote = `Service /status 每 ${
    STATUS_POLL_INTERVAL_MS / 1000
  } 秒 · Prometheus/监控代理每 ${
    PROMETHEUS_POLL_INTERVAL_MS / 1000
  } 秒 · 所有趋势共用同一时间范围`;

  return (
    <>
      <PageHeader
        title="监控分析工作台"
        description="统一窗口、可核对指标、没有隐藏的状态推断。"
        status={headerStatus}
        meta={
          <span className="text-xs text-faint-foreground">
            数据源：Service /status {STATUS_POLL_INTERVAL_MS / 1000} 秒 ·
            Prometheus {PROMETHEUS_POLL_INTERVAL_MS / 1000} 秒
          </span>
        }
      />

      <FilterRow
        note={`默认最近 ${
          getMonitoringRange(DEFAULT_MONITORING_RANGE_ID).label
        } · QPS rate 窗口 ${range.qpsRateWindow} · Peak QPS 范围：${peakRangeLabel(
          range,
        )}（${range.peakRateWindow} rate · ${range.peakStepSeconds}s step）`}
      >
        <span className="text-xs font-medium text-muted-foreground">
          统一时间范围
        </span>
        <RangeControl value={range.id} onChange={setRange} />
      </FilterRow>

      <dl
        className="mt-5 grid grid-cols-2 border-y border-border bg-card lg:grid-cols-4"
        aria-label="关键指标摘要"
      >
        <KpiCell
          className="border-r border-border"
          label="Realtime QPS"
          qualifier={`/ rate ${trends.source.rateWindow}`}
          value={qpsValue === null ? null : formatNumber(qpsValue, 2)}
          unit="req/s"
          unknownReason={qpsUnknown ?? undefined}
          detail={
            qpsSampleTime !== null
              ? `最近样本 ${formatUtcTime(qpsSampleTime)}`
              : "HTTP counter 完成时记录，进行中的流式下载不计入"
          }
          asOf={`最后成功：${formatUtcDateTime(trends.qps.lastSuccessAt)}`}
        />
        <KpiCell
          className="border-r border-border"
          label="Peak QPS"
          qualifier={`/ ${peakRangeLabel(range)}`}
          value={peakValue === null ? null : formatNumber(peakValue, 2)}
          unit="req/s"
          unknownReason={peakUnknown ?? undefined}
          detail={`${range.peakRateWindow} rate · ${range.peakStepSeconds}s step · 采样窗口速率的最大值，不是瞬时峰值`}
          asOf={`最后成功：${formatUtcDateTime(trends.peak.lastSuccessAt)}`}
        />
        <KpiCell
          className="border-r border-border"
          label="HTTP P99"
          qualifier="/ histogram"
          value={p99 === null ? null : formatNumber(p99, 2)}
          unit="ms"
          unknownReason={latencyUnknown ?? undefined}
          detail={
            <span>
              P95 {formatNumber(p95, 2)} ms · P50 {formatNumber(p50, 2)} ms ·
              含完整响应流耗时
            </span>
          }
          asOf={`最后成功：${formatUtcDateTime(trends.latency.lastSuccessAt)}`}
        />
        <KpiCell
          label="Feed Freshness"
          qualifier="/ collector /status"
          value={feedSummary ? String(feedSummary.tracked) : null}
          unit="项"
          unknownReason={feedUnknown ?? undefined}
          detail={
            feedSummary
              ? `${feedSummary.stale} 项失活 · ${feedSummary.unknown} 项未知 · 倒计时 = threshold_s - age_s`
              : "Collector /status 不可用时保持未知"
          }
          asOf={`最后成功：${formatUtcDateTime(
            lastSuccessAt(sources.collector),
          )}`}
        />
      </dl>

      <div className="mt-6 grid grid-cols-1 gap-x-7 gap-y-6 xl:grid-cols-12">
        <TrendChart
          className="xl:col-span-8"
          title="HTTP QPS"
          description={qpsDescription}
          unit="req/s"
          series={chartSeries(trends.qps)}
          state={trendChartState(trends.qps)}
          unknownCause={trends.source.disabledReason ? "not-exposed" : "failed"}
          errorMessage={chartError(trends.qps)}
        />
        {sources.serviceSource.kind === "gap" ? (
          <section
            aria-label="Service Instance capability gap"
            data-testid="service-instance-gap"
            className="border-t-2 border-status-warning bg-card px-4 py-4 xl:col-span-4"
          >
            <PanelTitle>Service Instance ledger</PanelTitle>
            <p className="mt-2 text-sm font-medium">
              {sources.serviceSource.reason.message}
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              Admin API 模式不读取无认证 <code>/status</code> 或{" "}
              <code>/ops/*</code>。需要后端实现{" "}
              <code>GET /api/v1/admin/service-instances</code> 并声明{" "}
              <code>service_lifecycle</code> 能力。
            </p>
          </section>
        ) : (
          <ServiceInstanceList
            className="xl:col-span-4"
            rows={serviceRows}
          />
        )}
        <TrendChart
          className="xl:col-span-7"
          title="HTTP 延迟 P50 / P95 / P99"
          description={latencyDescription}
          unit="ms"
          series={chartSeries(trends.latency)}
          state={trendChartState(trends.latency)}
          unknownCause={trends.source.disabledReason ? "not-exposed" : "failed"}
          errorMessage={chartError(trends.latency)}
        />
        <div className="xl:col-span-5">
          <SourcePanel rows={sourceRows} note={sourceNote} />
        </div>
      </div>

      {!sources.directOpsEnabled ? (
        <section
          aria-label="Feed Freshness"
          data-testid="feed-freshness-gap"
          className="mt-7 border-y border-border bg-card py-4"
        >
          <PanelHeader className="px-4">
            <div>
              <PanelTitle>Feed Freshness</PanelTitle>
              <PanelDescription>
                Feed Freshness 来自 collector /status.health.tracked。Admin
                模式不暴露无认证 /ops/collector，因此该面板保持能力缺口。
              </PanelDescription>
            </div>
            {feedOverallStatus}
          </PanelHeader>
          <p className="mt-2 px-4 text-sm font-medium">
            {sources.opsGapReason}
          </p>
        </section>
      ) : collectorError ? (
        <section
          aria-label="Feed Freshness"
          className="mt-7 border-y border-border bg-card py-4"
        >
          <PanelHeader className="px-4">
            <div>
              <PanelTitle>Feed Freshness</PanelTitle>
              <PanelDescription>
                来自 collector /status.health.tracked · 倒计时 = threshold_s -
                age_s · stale 以服务端为准。
              </PanelDescription>
            </div>
            {feedOverallStatus}
          </PanelHeader>
          <div className="mt-2 px-4">
            <SourceStatus
              source="Collector /status"
              window="当前快照（5 秒轮询）"
              state="error"
              lastSuccessAt={lastSuccessAt(sources.collector)}
              reason={errorText(sources.collector.error)}
            />
          </div>
          <ErrorState
            className="mt-2"
            description={`Collector /status 加载失败：${errorText(
              sources.collector.error,
            )}。最后成功：${formatUtcDateTime(
              lastSuccessAt(sources.collector),
            )}。不会继续把旧 Freshness 标为健康。`}
          />
        </section>
      ) : collectorReadyState === "loading" ? (
        <section
          aria-label="Feed Freshness"
          className="mt-7 border-y border-border bg-card py-4"
        >
          <PanelHeader className="px-4">
            <div>
              <PanelTitle>Feed Freshness</PanelTitle>
              <PanelDescription>
                来自 collector /status.health.tracked · 倒计时 = threshold_s -
                age_s · stale 以服务端为准。
              </PanelDescription>
            </div>
            {feedOverallStatus}
          </PanelHeader>
          <LoadingState label="正在读取 Collector /status…" showSkeleton />
        </section>
      ) : (
        <FreshnessMatrix
          rows={feedRows}
          overallStatus={feedOverallStatus}
          description={`来自 collector /status.health.tracked · 倒计时 = threshold_s - age_s · stale 以服务端为准 · 最后成功：${formatUtcDateTime(
            lastSuccessAt(sources.collector),
          )}`}
        />
      )}

      <DatasetFreshness
        state={datasetState}
        lastSuccessAt={lastSuccessAt(sources.dataset)}
        errorMessage={
          sources.dataset.isError ? errorText(sources.dataset.error) : null
        }
        rows={datasetRows}
      />

      <UpstreamTable
        state={instrumentsState}
        lastSuccessAt={lastSuccessAt(sources.instruments)}
        errorMessage={
          sources.instruments.isError
            ? errorText(sources.instruments.error)
            : null
        }
        rows={venueRows}
      />
    </>
  );
}
