import { describe, expect, it } from "vitest";
import { trendSeriesName } from "@/api/monitoring/series-display";

describe("trend series display names", () => {
  it("appends the path label without banned separators", () => {
    expect(trendSeriesName({ path: "/api/v1/bars" }, "P99", 0, 1)).toBe(
      "P99 /api/v1/bars",
    );
    expect(trendSeriesName({ path: "/api/v1/bars" }, "P99", 0, 1)).not.toMatch(
      /[·：]/,
    );
  });

  it("numbers multiple anonymous series so they stay distinguishable", () => {
    expect(trendSeriesName({}, "QPS", 0, 2)).toBe("QPS 1");
    expect(trendSeriesName({}, "QPS", 1, 2)).toBe("QPS 2");
  });

  it("keeps a single anonymous series unnumbered", () => {
    expect(trendSeriesName({}, "QPS", 0, 1)).toBe("QPS");
  });
});
