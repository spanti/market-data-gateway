import { describe, expect, it } from "vitest";
import type { BarMessageEnvelope } from "@/api/endpoints/bars";
import {
  BAR_CHART_SERIES,
  buildBarChartOption,
  formatUtcChartLabel,
} from "@/lib/bar-chart-option";

function bar(openTs: string, close: string, high = "2.0", low = "0.5") {
  return {
    schema_version: 1,
    data_type: "bar" as const,
    instrument_id: "binance:BTC/USDT",
    event_ts: openTs,
    ingest_ts: "2026-11-09T04:24:42.946489000Z",
    feed: "fake",
    seq: null,
    payload: {
      interval: "1m" as const,
      open_ts: openTs,
      close_ts: openTs,
      aggregation_source: "external" as const,
      open: "1.0",
      high,
      low,
      close,
      volume: "10.0",
      trade_count: 1,
      revision: 0,
    },
  } satisfies BarMessageEnvelope;
}

describe("Bar chart option", () => {
  it("uses one y-axis and three fixed-order line series with 2px lines", () => {
    const option = buildBarChartOption([
      bar("2024-01-01T00:00:00.000000000Z", "1.5"),
    ]);

    expect(Array.isArray(option.yAxis)).toBe(false);
    expect(option.series).toHaveLength(3);
    expect(
      (option.series as Array<{ name: string }>).map((series) => series.name),
    ).toEqual([...BAR_CHART_SERIES]);
    for (const series of option.series as Array<{
      lineStyle: { width: number };
      connectNulls: boolean;
      showSymbol: boolean;
    }>) {
      expect(series.lineStyle.width).toBe(2);
      expect(series.connectNulls).toBe(false);
      expect(series.showSymbol).toBe(false);
    }
  });

  it("converts only chart coordinates and leaves the envelope strings untouched", () => {
    const source = bar(
      "2024-01-01T00:00:00.000000000Z",
      "0.000000000000000001",
      "65010.0",
      "64990.0",
    );

    const option = buildBarChartOption([source]);
    const series = option.series as Array<{ data: Array<number | null> }>;

    expect(series[0]?.data).toEqual([0.000000000000000001]);
    expect(series[1]?.data).toEqual([65010]);
    expect(series[2]?.data).toEqual([64990]);
    expect(source.payload.close).toBe("0.000000000000000001");
    expect(source.payload.high).toBe("65010.0");
  });

  it("renders an invalid coordinate as a gap rather than zero", () => {
    const option = buildBarChartOption([
      bar("2024-01-01T00:00:00.000000000Z", "not-a-decimal"),
    ]);
    const series = option.series as Array<{ data: Array<number | null> }>;

    expect(series[0]?.data).toEqual([null]);
  });

  it("formats UTC labels without using the browser timezone", () => {
    expect(formatUtcChartLabel("2024-01-01T00:05:00.000000000Z")).toBe(
      "2024-01-01 00:05",
    );
    expect(formatUtcChartLabel("invalid")).toBe("invalid");
  });
});
