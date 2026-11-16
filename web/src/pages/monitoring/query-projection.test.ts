import type { UseQueryResult } from "@tanstack/react-query";
import { describe, expect, it } from "vitest";
import {
  lastSuccessAt,
  sourceState,
} from "@/pages/monitoring/use-monitoring-data";

function query<T>(
  overrides: Partial<UseQueryResult<T>>,
): UseQueryResult<T> {
  return overrides as UseQueryResult<T>;
}

describe("query projections keep last-success semantics", () => {
  it("reports last success only after a real successful update", () => {
    expect(lastSuccessAt(query({ data: undefined, dataUpdatedAt: 0 }))).toBe(
      null,
    );
    expect(
      lastSuccessAt(query({ data: { value: 1 }, dataUpdatedAt: 0 })),
    ).toBeNull();
    expect(
      lastSuccessAt(
        query({ data: { value: 1 }, dataUpdatedAt: 1794225600000 }),
      ),
    ).toBe(1794225600000);
  });

  it("keeps the last-success timestamp visible data when a refetch fails", () => {
    const failed = query({
      data: { value: 1 },
      isError: true,
      dataUpdatedAt: 1794225600000,
    });

    expect(sourceState(failed)).toBe("error");
    expect(lastSuccessAt(failed)).toBe(1794225600000);
  });

  it("distinguishes loading from ready without inventing data", () => {
    expect(sourceState(query({ isPending: true }))).toBe("loading");
    expect(sourceState(query({ isPending: false, isError: false }))).toBe(
      "ready",
    );
  });
});
