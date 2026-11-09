import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { BarMessageEnvelope } from "@/api/endpoints/bars";
import { BarChart } from "@/components/bar-chart";

vi.mock("echarts/core", () => ({
  use: vi.fn(),
  init: vi.fn(() => ({
    setOption: vi.fn(),
    dispatchAction: vi.fn(),
    resize: vi.fn(),
    dispose: vi.fn(),
  })),
}));

function envelope(
  openTs: string,
  close: string,
  revision = 0,
): BarMessageEnvelope {
  return {
    schema_version: 1,
    data_type: "bar",
    instrument_id: "binance:BTC/USDT",
    event_ts: openTs,
    ingest_ts: "2026-11-09T04:24:42.946489000Z",
    feed: "fake",
    seq: null,
    payload: {
      interval: "1m",
      open_ts: openTs,
      close_ts: openTs,
      aggregation_source: "external",
      open: "65000.1",
      high: "65010.0",
      low: "64990.0",
      close,
      volume: "12.34",
      trade_count: 128,
      revision,
    },
  };
}

const BARS = [
  envelope("2024-01-01T00:00:00.000000000Z", "65005.5"),
  envelope("2024-01-01T00:01:00.000000000Z", "0.000000000000000001"),
];

describe("BarChart accessibility fallbacks", () => {
  it("shows the exact current values in the legend and the same data in the table", async () => {
    const user = userEvent.setup();
    render(<BarChart bars={BARS} />);

    const panel = screen.getByRole("region", { name: "最大 Revision Bar 图" });
    expect(
      within(panel).getAllByText("0.000000000000000001").length,
    ).toBeGreaterThan(0);

    await user.click(
      within(panel).getByText("查看数据表（2 个时间点）"),
    );

    const table = within(panel).getByRole("table");
    expect(
      within(table).getByText(/数据表与图表使用同一组最大 Revision 数据/),
    ).toBeInTheDocument();
    expect(
      within(table).getByText("2024-01-01T00:00:00.000000000Z"),
    ).toBeInTheDocument();
    expect(
      within(table).getByText("2024-01-01T00:01:00.000000000Z"),
    ).toBeInTheDocument();
    expect(within(table).getAllByText("0.000000000000000001").length).toBe(1);
    expect(within(table).getByText("65005.5")).toBeInTheDocument();
  });

  it("provides a keyboard point readout without relying on color", () => {
    render(<BarChart bars={BARS} />);

    const chart = screen.getByRole("img", {
      name: /最大 Revision Bar 价格图/,
    });
    fireEvent.focus(chart);

    const readout = document.getElementById("bar-chart-readout");
    expect(readout).toHaveTextContent(
      "时间 2024-01-01T00:01:00.000000000Z",
    );
    expect(readout).toHaveTextContent("close 0.000000000000000001");

    fireEvent.keyDown(chart, { key: "ArrowLeft" });
    expect(readout).toHaveTextContent(
      "时间 2024-01-01T00:00:00.000000000Z",
    );
    expect(readout).toHaveTextContent("close 65005.5");

    fireEvent.keyDown(chart, { key: "End" });
    expect(readout).toHaveTextContent(
      "时间 2024-01-01T00:01:00.000000000Z",
    );
  });

  it("keeps the empty and error states explicit instead of drawing zero", () => {
    const { rerender } = render(<BarChart bars={[]} />);
    expect(screen.getByText("没有数据")).toBeInTheDocument();
    expect(
      screen.queryByRole("img", { name: /最大 Revision Bar 价格图/ }),
    ).not.toBeInTheDocument();

    rerender(
      <BarChart
        bars={[]}
        state="error"
        errorMessage="服务端拒绝 JSON 输出"
        requestId="request-123"
      />,
    );
    expect(screen.getByText("服务端拒绝 JSON 输出")).toBeInTheDocument();
    expect(screen.getByText(/request-123/)).toBeInTheDocument();
  });
});
