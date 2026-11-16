import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
  ServiceInstanceList,
  type ServiceInstanceRow,
} from "@/components/monitoring/service-instance-list";

function row(overrides: Partial<ServiceInstanceRow> = {}): ServiceInstanceRow {
  return {
    role: "collector",
    instanceId: null,
    service: "collector",
    ready: true,
    readyReason: null,
    uptimeS: 60,
    statusState: "ready",
    lastSuccessAt: Date.parse("2026-11-09T12:00:00.000Z"),
    details: [],
    ...overrides,
  };
}

describe("ServiceInstanceList source wording", () => {
  it("labels Current Contract per-Role rows as Service Roles", () => {
    render(
      <ServiceInstanceList
        sourceKind="role"
        rows={[row(), row({ role: "api" })]}
        lastSuccessAt={Date.parse("2026-11-09T12:00:00.000Z")}
      />,
    );

    const card = screen.getByRole("region", { name: "Service Role" });
    expect(
      within(card).getByRole("heading", { name: "Service Role" }),
    ).toBeInTheDocument();
    expect(within(card).getByText("2 个 Service Role")).toBeInTheDocument();
    expect(within(card).queryByText("Service Instance")).not.toBeInTheDocument();
  });

  it("labels Admin API rows as Service Instances", () => {
    render(
      <ServiceInstanceList
        sourceKind="instance"
        rows={[
          row({ instanceId: "collector-1", service: "collector primary" }),
          row({ role: "api", instanceId: "api-1", service: "api primary" }),
        ]}
        lastSuccessAt={Date.parse("2026-11-09T12:00:00.000Z")}
      />,
    );

    const card = screen.getByRole("region", { name: "Service Instance" });
    expect(
      within(card).getByRole("heading", { name: "Service Instance" }),
    ).toBeInTheDocument();
    expect(within(card).getByText("2 个 Service Instance")).toBeInTheDocument();
    expect(within(card).getByText("collector-1")).toBeInTheDocument();
    expect(within(card).queryByText("2 个 Service Role")).not.toBeInTheDocument();
  });
});
