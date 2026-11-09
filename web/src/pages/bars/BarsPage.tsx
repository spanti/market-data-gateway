import { useMemo, useState, type FormEvent } from "react";
import { useSearchParams } from "react-router-dom";
import type { BarMessageEnvelope } from "@/api/endpoints/bars";
import { BarChart } from "@/components/bar-chart";
import { CsvDownloadPanel } from "@/components/csv-download-panel";
import { ExportJobPanel } from "@/components/export-job-panel";
import { LiveFollowPanel } from "@/components/live-follow-panel";
import { PageHeader } from "@/components/page-header";
import { RevisionTable } from "@/components/revision-table";
import { FilterRow } from "@/components/toolbar";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Panel, PanelDescription, PanelTitle } from "@/components/ui/panel";
import { Select } from "@/components/ui/select";
import { HttpError } from "@/api/http/types";
import { useBarsHistory } from "@/hooks/use-bars-history";
import { useBarLiveFeed } from "@/hooks/use-bar-live-feed";
import { useNow } from "@/hooks/use-now";
import { deriveMaxRevisionBars, type BarRow } from "@/lib/bar-data";
import { classifyExportRange, formatRangeDays } from "@/lib/export-policy";
import { loadPreferences, savePreferences } from "@/lib/preferences";
import {
  BAR_INTERVALS,
  formatDateTimeInput,
  parseBarQuery,
  parseDateTimeInput,
  serializeBarQuery,
  toIsoWithOffset,
  toQueryInstant,
  validateChartRange,
  type BarInterval,
  type TimeDisplayZone,
} from "@/lib/time-range";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import type { ChartPanelState } from "@/components/chart-panel";

function chartErrorMessage(error: HttpError): string {
  if (error.code === "range_too_large") {
    const rowCount =
      typeof error.details.row_count === "number"
        ? `（row_count=${error.details.row_count.toLocaleString("en-US")}）`
        : "";
    return `服务端拒绝 JSON 输出：原始行超过 100,000 行${rowCount}。请缩小时间范围，或使用 CSV / Export Job 路径。`;
  }
  if (error.code === "schema_version_mismatch") {
    return `Dataset schema 不匹配：${error.message}。请联系后端维护，不要重试。`;
  }
  return error.message;
}

export function BarsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const preferences = loadPreferences();
  const displayZone = preferences.timezone;

  // Draft controls are seeded from the URL. The applied query is re-parsed
  // whenever the URL changes; a stable skip rendering anchor keeps the
  // default 24h window from drifting on unrelated re-renders.
  const initialQuery = parseBarQuery(searchParams, { displayZone });
  const [instrumentId, setInstrumentId] = useState(
    initialQuery.instrumentId ?? "",
  );
  const [interval, setInterval] = useState<BarInterval | "">(
    initialQuery.interval ?? "",
  );
  const [startInput, setStartInput] = useState(
    formatDateTimeInput(initialQuery.start, displayZone),
  );
  const [endInput, setEndInput] = useState(
    formatDateTimeInput(initialQuery.end, displayZone),
  );
  const [zone, setZone] = useState<TimeDisplayZone>(displayZone);
  const [formError, setFormError] = useState<string | null>(null);

  const paramsKey = searchParams.toString();
  const applied = useMemo(
    () =>
      parseBarQuery(new URLSearchParams(paramsKey), {
        displayZone: zone,
        now: new Date(),
      }),
    [paramsKey, zone],
  );

  const now = useNow(30_000);
  const rangeViolation = validateChartRange(applied.start, applied.end);
  const decision = classifyExportRange(applied.start, applied.end);
  const identityReady =
    applied.instrumentId !== null && applied.interval !== null;

  const history = useBarsHistory({
    instrumentId: applied.instrumentId,
    interval: applied.interval,
    start: applied.start,
    end: applied.end,
    enabled: rangeViolation === null,
  });

  const live = useBarLiveFeed({
    instrumentId: applied.instrumentId,
    interval: applied.interval,
    start: applied.start,
    end: applied.end,
    rangeAllowed: rangeViolation === null,
    now,
  });

  const rawRows: BarRow[] = useMemo(
    () => [
      ...(history.data?.bars ?? []).map((bar) => ({
        bar,
        source: "rest" as const,
      })),
      ...live.rows,
    ],
    [history.data, live.rows],
  );
  const rawBars: BarMessageEnvelope[] = useMemo(
    () => rawRows.map((row) => row.bar),
    [rawRows],
  );
  const dedupedBars = useMemo(
    () => deriveMaxRevisionBars(rawBars),
    [rawBars],
  );

  const { subject, capability, session } = useRuntimeMode();
  const exportJobCapability =
    capability?.mode === "admin-api"
      ? (capability.document?.capabilities.export_job ?? null)
      : null;

  const csvQuery =
    applied.instrumentId !== null && applied.interval !== null
      ? {
          instrument: applied.instrumentId,
          interval: applied.interval,
          start: toQueryInstant(applied.start),
          end: toQueryInstant(applied.end),
        }
      : null;

  const exportQuery =
    applied.instrumentId !== null && applied.interval !== null
      ? {
          instrumentId: applied.instrumentId,
          interval: applied.interval,
          start: applied.start,
          end: applied.end,
        }
      : null;

  function apply(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const start = parseDateTimeInput(startInput, zone);
    const end = parseDateTimeInput(endInput, zone);
    if (!start || !end) {
      setFormError("请输入有效的开始与结束时间。");
      return;
    }
    if (end.getTime() < start.getTime()) {
      setFormError("开始时间必须早于结束时间。");
      return;
    }
    if (!instrumentId.trim() || !interval) {
      setFormError("请选择 InstrumentId 与 Bar Interval。");
      return;
    }
    setFormError(null);
    setSearchParams(
      serializeBarQuery({
        instrumentId: instrumentId.trim(),
        interval,
        start,
        end,
        displayZone: zone,
        isDefaultRange: false,
        invalidParams: [],
      }),
    );
  }

  function reset() {
    const defaults = parseBarQuery(new URLSearchParams(), {
      displayZone: zone,
    });
    setInstrumentId("");
    setInterval("");
    setStartInput(formatDateTimeInput(defaults.start, zone));
    setEndInput(formatDateTimeInput(defaults.end, zone));
    setFormError(null);
    setSearchParams(new URLSearchParams());
  }

  function changeZone(nextZone: TimeDisplayZone) {
    const start = parseDateTimeInput(startInput, zone);
    const end = parseDateTimeInput(endInput, zone);
    setZone(nextZone);
    if (start) {
      setStartInput(formatDateTimeInput(start, nextZone));
    }
    if (end) {
      setEndInput(formatDateTimeInput(end, nextZone));
    }
    savePreferences({ ...loadPreferences(), timezone: nextZone });
  }

  let chartState: ChartPanelState = "ready";
  let chartError: string | undefined;
  let chartRequestId: string | null = null;
  if (rangeViolation) {
    chartState = "error";
    chartError = `${rangeViolation.message}（当前范围 ${formatRangeDays(
      decision.rangeDays,
    )} 天）。图表与原始 Revision 仅支持 ≤31 个 UTC 天；请缩小范围，或使用下方 Export Job。`;
  } else if (!identityReady) {
    chartState = "empty";
  } else if (history.isPending) {
    chartState = "loading";
  } else if (history.isError) {
    chartState = "error";
    if (history.error instanceof HttpError) {
      chartError = chartErrorMessage(history.error);
      chartRequestId = history.error.requestId;
    } else {
      chartError = "Bar 历史加载失败。";
    }
  } else if (rawBars.length === 0) {
    chartState = "empty";
  }

  return (
    <>
      <PageHeader
        title="Bar 图与导出"
        description="按 InstrumentId、Bar Interval 和显式时间范围查询最大 Revision Bar；图表、原始 Revision 表与 CSV 共享同一 URL 查询状态。"
        meta={
          <span className="text-xs text-faint-foreground">
            默认 UTC · 图表/JSON ≤31 个 UTC 天 · 直接 CSV ≤31 天 · Export Job
            31–366 天 · &gt;366 天拆分
          </span>
        }
      />

      <FilterRow
        note="start 为闭区间、end 为开区间。URL 保存 instrument_id、interval、start、end（UTC 规范形式）。"
        className="items-start"
      >
        <form
          onSubmit={apply}
          aria-label="Bar 查询条件"
          className="flex flex-wrap items-end gap-3"
        >
          <div className="flex w-52 flex-col gap-1">
            <Label htmlFor="bar-instrument">InstrumentId</Label>
            <Input
              id="bar-instrument"
              value={instrumentId}
              onChange={(event) => setInstrumentId(event.target.value)}
              placeholder="binance:BTC/USDT"
              autoComplete="off"
              spellCheck={false}
            />
          </div>

          <div className="flex w-36 flex-col gap-1">
            <Label htmlFor="bar-interval">Bar Interval</Label>
            <Select
              id="bar-interval"
              value={interval}
              onChange={(event) =>
                setInterval(event.target.value as BarInterval | "")
              }
            >
              <option value="">选择 Bar Interval</option>
              {BAR_INTERVALS.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </Select>
          </div>

          <div className="flex w-52 flex-col gap-1">
            <Label htmlFor="bar-start">开始（闭区间）</Label>
            <Input
              id="bar-start"
              type="datetime-local"
              value={startInput}
              onChange={(event) => setStartInput(event.target.value)}
            />
          </div>

          <div className="flex w-52 flex-col gap-1">
            <Label htmlFor="bar-end">结束（开区间）</Label>
            <Input
              id="bar-end"
              type="datetime-local"
              value={endInput}
              onChange={(event) => setEndInput(event.target.value)}
            />
          </div>

          <div className="flex w-36 flex-col gap-1">
            <Label htmlFor="bar-zone">显示时区</Label>
            <Select
              id="bar-zone"
              value={zone}
              onChange={(event) =>
                changeZone(event.target.value as TimeDisplayZone)
              }
            >
              <option value="utc">UTC</option>
              <option value="local">浏览器本地时间</option>
            </Select>
          </div>

          <Button type="submit">应用筛选</Button>
          <Button type="button" variant="outline" onClick={reset}>
            重置为最近 24 小时
          </Button>
        </form>
      </FilterRow>

      {formError ? (
        <p role="alert" className="mt-3 text-xs text-status-critical">
          {formError}
        </p>
      ) : null}
      {applied.invalidParams.length > 0 ? (
        <p role="alert" className="mt-3 text-xs text-status-critical">
          URL 参数无效：{applied.invalidParams.join("、")}。请重新选择后应用筛选。
        </p>
      ) : null}
      {rangeViolation ? (
        <p role="alert" className="mt-3 text-xs text-status-critical">
          {rangeViolation.message}。图表与原始 Revision 仅支持 ≤31 个 UTC
          天；请缩小范围，或使用下方 Export Job。
        </p>
      ) : null}

      <Panel className="mt-6 p-5" aria-label="当前查询状态">
        <PanelTitle>当前查询状态</PanelTitle>
        <PanelDescription>
          该状态来自 URL，被图表、原始 Revision 表、CSV 与 Export Job 共用。
        </PanelDescription>
        <dl className="mt-3 grid grid-cols-1 gap-3 text-xs sm:grid-cols-2 xl:grid-cols-4">
          <div>
            <dt className="text-faint-foreground">InstrumentId</dt>
            <dd className="mt-1 font-mono" data-testid="applied-instrument">
              {applied.instrumentId ?? "未选择"}
            </dd>
          </div>
          <div>
            <dt className="text-faint-foreground">Bar Interval</dt>
            <dd className="mt-1 font-mono" data-testid="applied-interval">
              {applied.interval ?? "未选择"}
            </dd>
          </div>
          <div>
            <dt className="text-faint-foreground">start（闭区间）</dt>
            <dd className="mt-1 font-mono" data-testid="applied-start">
              {toIsoWithOffset(applied.start, zone)}
            </dd>
          </div>
          <div>
            <dt className="text-faint-foreground">end（开区间）</dt>
            <dd className="mt-1 font-mono" data-testid="applied-end">
              {toIsoWithOffset(applied.end, zone)}
            </dd>
          </div>
        </dl>
        <p className="mt-3 text-xs text-faint-foreground">
          当前范围：{formatRangeDays(decision.rangeDays)} 个 UTC 天 ·{" "}
          {decision.message}
        </p>
      </Panel>

      {history.data && !history.data.complete ? (
        <div
          role="status"
          className="mt-3 border-l-2 border-status-warning pl-2 text-xs text-status-warning"
        >
          {history.data.stop?.message} 已返回 {history.data.bars.length}{" "}
          条原始行 / {history.data.pages} 页；结果不完整。
        </div>
      ) : null}

      {history.data ? (
        <p
          className="mt-3 text-xs text-muted-foreground"
          data-testid="history-summary"
        >
          共 {history.data.bars.length} 条原始 Revision（未去重）·{" "}
          {dedupedBars.length} 个最大 Revision 点 · {history.data.pages} 页 ·{" "}
          {history.data.complete ? "分页完整" : "分页已停止（部分结果）"}
        </p>
      ) : null}

      <BarChart
        className="mt-6"
        bars={rawBars}
        state={chartState}
        errorMessage={chartError}
        requestId={chartRequestId}
        onRetry={
          history.isError ? () => void history.refetch() : undefined
        }
      />

      {rangeViolation ? (
        <Panel className="mt-6 p-5" aria-label="原始 Revision 表">
          <PanelTitle>原始 Revision 表（未去重）</PanelTitle>
          <PanelDescription>
            当前范围超过 31 个 UTC 天，未查询 JSON 原始 Revision。请缩小范围后查看，
            或使用下方 Export Job。
          </PanelDescription>
        </Panel>
      ) : (
        <RevisionTable rows={rawRows} />
      )}

      <CsvDownloadPanel query={csvQuery} decision={decision} />
      <ExportJobPanel
        query={exportQuery}
        decision={decision}
        subject={subject}
        capability={exportJobCapability}
        currentSubject={session?.user?.subject ?? null}
        now={now}
      />

      {rangeViolation === null ? (
        <LiveFollowPanel
          feed={live}
          onRestRefetch={() => void history.refetch()}
        />
      ) : null}
    </>
  );
}
