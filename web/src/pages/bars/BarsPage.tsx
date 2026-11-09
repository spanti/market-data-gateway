import { useState, type FormEvent } from "react";
import { useSearchParams } from "react-router-dom";
import { ChartPanel } from "@/components/chart-panel";
import { PageHeader } from "@/components/page-header";
import { FilterRow } from "@/components/toolbar";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Panel, PanelDescription, PanelTitle } from "@/components/ui/panel";
import { Select } from "@/components/ui/select";
import {
  BAR_INTERVALS,
  formatDateTimeInput,
  parseBarQuery,
  parseDateTimeInput,
  serializeBarQuery,
  toIsoWithOffset,
  validateChartRange,
  type BarInterval,
  type TimeDisplayZone,
} from "@/lib/time-range";
import { loadPreferences, savePreferences } from "@/lib/preferences";

export function BarsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const displayZone = loadPreferences().timezone;
  const query = parseBarQuery(searchParams, { displayZone });

  const [instrumentId, setInstrumentId] = useState(query.instrumentId ?? "");
  const [interval, setInterval] = useState<BarInterval | "">(
    query.interval ?? "",
  );
  const [startInput, setStartInput] = useState(
    formatDateTimeInput(query.start, displayZone),
  );
  const [endInput, setEndInput] = useState(
    formatDateTimeInput(query.end, displayZone),
  );
  const [zone, setZone] = useState<TimeDisplayZone>(displayZone);
  const [error, setError] = useState<string | null>(null);

  function apply(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const start = parseDateTimeInput(startInput, zone);
    const end = parseDateTimeInput(endInput, zone);
    if (!start || !end) {
      setError("请输入有效的开始与结束时间。");
      return;
    }
    const violation = validateChartRange(start, end);
    if (violation) {
      setError(violation.message);
      return;
    }
    setError(null);
    const next = serializeBarQuery({
      instrumentId: instrumentId.trim() || null,
      interval: interval || null,
      start,
      end,
      displayZone: zone,
      isDefaultRange: false,
      invalidParams: [],
    });
    setSearchParams(next);
  }

  function reset() {
    const defaults = parseBarQuery(new URLSearchParams(), {
      displayZone: zone,
    });
    setInstrumentId("");
    setInterval("");
    setStartInput(formatDateTimeInput(defaults.start, zone));
    setEndInput(formatDateTimeInput(defaults.end, zone));
    setError(null);
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

  return (
    <>
      <PageHeader
        title="Bar 图与导出"
        description="按 InstrumentId、Bar Interval 和显式时间范围查询最大 Revision Bar；图表、明细与 CSV 共享 URL 查询状态。"
        meta={
          <span className="text-xs text-faint-foreground">
            默认 UTC · 单次最多 31 个 UTC 天
          </span>
        }
      />

      <FilterRow
        note="start 为闭区间、end 为开区间；URL 保存 instrument_id、interval、start、end。"
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

      {error ? (
        <p role="alert" className="mt-3 text-xs text-status-critical">
          {error}
        </p>
      ) : null}
      {query.invalidParams.length > 0 ? (
        <p role="alert" className="mt-3 text-xs text-status-critical">
          URL 参数无效：{query.invalidParams.join("、")}。请重新选择后应用筛选。
        </p>
      ) : null}

      <Panel className="mt-6 p-5" aria-label="当前查询状态">
        <PanelTitle>当前查询状态</PanelTitle>
        <PanelDescription>
          该状态来自 URL，将被图表、Revision 表格和 CSV 共用。
        </PanelDescription>
        <dl className="mt-3 grid grid-cols-1 gap-3 text-xs sm:grid-cols-2 xl:grid-cols-4">
          <div>
            <dt className="text-faint-foreground">InstrumentId</dt>
            <dd className="mt-1 font-mono">
              {query.instrumentId ?? "未选择"}
            </dd>
          </div>
          <div>
            <dt className="text-faint-foreground">Bar Interval</dt>
            <dd className="mt-1 font-mono">{query.interval ?? "未选择"}</dd>
          </div>
          <div>
            <dt className="text-faint-foreground">start（闭区间）</dt>
            <dd className="mt-1 font-mono">
              {toIsoWithOffset(query.start, zone)}
            </dd>
          </div>
          <div>
            <dt className="text-faint-foreground">end（开区间）</dt>
            <dd className="mt-1 font-mono">
              {toIsoWithOffset(query.end, zone)}
            </dd>
          </div>
        </dl>
      </Panel>

      <ChartPanel
        className="mt-6"
        title="最大 Revision Bar 图"
        description="图表、Revision 明细表与 CSV 导出将在后续工单接入；此处不渲染模拟行情。"
        state="empty"
        dataTableSummary="查看数据表（尚无数据）"
      />
    </>
  );
}
