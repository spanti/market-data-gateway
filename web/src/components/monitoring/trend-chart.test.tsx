import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { TrendChart } from "@/components/monitoring/trend-chart";
import type { TrendSeries } from "@/api/monitoring/types";

const t0 = Date.parse("2026-11-09T11:00:00.000Z");
const t1 = Date.parse("2026-11-09T11:00:30.000Z");

const series: TrendSeries[] = [
  {
    name: "P50",
    points: [
      { ts: t0, value: 10 },
      { ts: t1, value: null },
    ],
  },
  {
    name: "P95",
    points: [
      { ts: t0, value: 20 },
      { ts: t1, value: 25 },
    ],
  },
];

describe("TrendChart accessibility contract", () => {
  it("shows an inline legend with the latest finite values", () => {
    render(<TrendChart title="HTTP latency" unit="ms" series={series} />);

    expect(screen.getByText("P50")).toBeInTheDocument();
    expect(screen.getAllByText("10").length).toBeGreaterThan(0);
    expect(screen.getByText("P95")).toBeInTheDocument();
    expect(screen.getAllByText("25").length).toBeGreaterThan(0);
  });

  it("always provides a data-table fallback with explicit gaps", () => {
    render(<TrendChart title="HTTP latency" unit="ms" series={series} />);

    expect(
      screen.getByText("查看数据表（2 个时间点）"),
    ).toBeInTheDocument();
    const table = screen.getByRole("table", { name: "HTTP latency 数据表" });
    expect(within(table).getByText("P50（ms）")).toBeInTheDocument();
    expect(within(table).getByText("—")).toBeInTheDocument();
  });

  it("supports keyboard point navigation with a live readout", async () => {
    const user = userEvent.setup();
    render(<TrendChart title="HTTP latency" unit="ms" series={series} />);

    const chart = screen.getByRole("img", {
      name: /HTTP latency 图表/,
    });
    await user.tab();
    expect(chart).toHaveFocus();

    await user.keyboard("{ArrowLeft}");
    expect(
      screen.getByText(/11:00:00 UTC · P50 10 ms · P95 20 ms/),
    ).toBeInTheDocument();

    await user.keyboard("{ArrowRight}");
    expect(
      screen.getByText(/11:00:30 UTC · P50 — ms · P95 25 ms/),
    ).toBeInTheDocument();
  });

  it("hides the stale table and shows an error state on failure", () => {
    render(
      <TrendChart
        title="HTTP latency"
        unit="ms"
        series={series}
        state="error"
        errorMessage="Prometheus 不可用"
      />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent("Prometheus 不可用");
    expect(screen.queryByText(/查看数据表/)).not.toBeInTheDocument();
  });

  it("shows the contract gap state without inventing a chart", () => {
    render(
      <TrendChart
        title="HTTP latency"
        unit="ms"
        series={[]}
        state="unknown"
        unknownCause="not-exposed"
      />,
    );

    expect(screen.getByText("契约未暴露")).toBeInTheDocument();
    expect(screen.queryByText(/查看数据表/)).not.toBeInTheDocument();
  });

  it("shows an empty state without auto-expanding the range", () => {
    render(
      <TrendChart title="QPS" unit="req/s" series={[]} state="empty" />,
    );

    expect(screen.getByText("没有数据")).toBeInTheDocument();
    expect(
      screen.getByText(/不会自动扩大时间范围/),
    ).toBeInTheDocument();
  });
});
