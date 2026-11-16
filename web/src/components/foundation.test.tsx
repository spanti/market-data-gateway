import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { PageHeader } from "@/components/page-header";
import {
  EmptyState,
  ErrorState,
  LoadingState,
  UnknownState,
} from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { expectNoBannedUiSymbols } from "@/test/ui-copy";

describe("D foundation contracts", () => {
  it("live status badges expose their status as text, not color alone", () => {
    const { container } = render(
      <StatusBadge tone="critical" live>
        Feed Freshness 失活
      </StatusBadge>,
    );

    const badge = screen.getByRole("status");
    expect(badge).toHaveTextContent("Feed Freshness 失活");
    expectNoBannedUiSymbols(container);
  });

  it("cards and page headers keep a single accessible title", () => {
    const { container } = render(
      <>
        <PageHeader title="监控总览" status={<StatusBadge tone="good">正常</StatusBadge>} />
        <Card>
          <CardHeader>
            <CardTitle>Feed Freshness</CardTitle>
          </CardHeader>
          <CardContent>真实状态</CardContent>
        </Card>
      </>,
    );

    expect(
      screen.getByRole("heading", { name: "监控总览", level: 1 }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("heading", { name: "Feed Freshness" }),
    ).toBeInTheDocument();
    expectNoBannedUiSymbols(container);
  });

  it("state regions keep request ids and reasons visible in place", () => {
    const { container } = render(
      <>
        <LoadingState label="正在加载数据" />
        <EmptyState title="当前范围没有数据" description="不会补造记录。" />
        <ErrorState
          description="Collector /status 加载失败"
          requestId="req-123"
        />
        <UnknownState cause="not-exposed" reason="上游连接能力未暴露" />
      </>,
    );

    expect(screen.getByRole("status")).toHaveTextContent("正在加载数据");
    expect(screen.getByText("当前范围没有数据")).toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent("req-123");
    expect(screen.getByText("契约未暴露")).toBeInTheDocument();
    expect(screen.getByText("上游连接能力未暴露")).toBeInTheDocument();
    expectNoBannedUiSymbols(container);
  });
});
