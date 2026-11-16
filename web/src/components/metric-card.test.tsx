import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { MetricCard } from "@/components/metric-card";
import { TooltipProvider } from "@/components/ui/tooltip";
import { installResizeObserverStub } from "@/test/tooltip-env";

installResizeObserverStub();

function renderMetric(ui: React.ReactNode) {
  return render(<TooltipProvider>{ui}</TooltipProvider>);
}

describe("MetricCard", () => {
  it("renders label, value, unit and real time evidence", () => {
    renderMetric(
      <MetricCard
        label="Realtime QPS"
        value="12.5"
        unit="req/s"
        evidence="最后成功 2026-11-09 12:00:00 UTC"
        help={{ label: "Realtime QPS 说明", content: "最近一个成功样本。" }}
      />,
    );

    const card = screen.getByRole("group", { name: "Realtime QPS" });
    expect(card).toHaveTextContent("12.5");
    expect(card).toHaveTextContent("req/s");
    expect(card).toHaveTextContent("最后成功 2026-11-09 12:00:00 UTC");
  });

  it("renders an explicit unknown instead of zero or a stale value", () => {
    renderMetric(
      <MetricCard
        label="HTTP P99"
        value={null}
        unit="ms"
        unknownReason="能力未知，暂不可用"
        evidence="最后成功 未成功过"
      />,
    );

    const card = screen.getByRole("group", { name: "HTTP P99" });
    expect(card).toHaveTextContent("未知");
    expect(card).toHaveTextContent("能力未知，暂不可用");
    expect(card).not.toHaveTextContent("0 ms");
  });

  it("puts only supplementary definitions in the help trigger", async () => {
    const user = userEvent.setup();
    renderMetric(
      <MetricCard
        label="Peak QPS"
        value="18.25"
        unit="req/s"
        help={{ label: "Peak QPS 说明", content: "范围内最大采样速率。" }}
      />,
    );

    const trigger = screen.getByRole("button", { name: "Peak QPS 说明" });
    await user.hover(trigger);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "范围内最大采样速率。",
    );
  });
});
