import { describe, expect, it } from "vitest";
import {
  DAY_MS,
  MAX_DIRECT_RANGE_MS,
  MAX_JOB_RANGE_MS,
  classifyExportRange,
  formatRangeDays,
} from "@/lib/export-policy";

const start = new Date("2026-01-01T00:00:00.000Z");

function endAfter(ms: number): Date {
  return new Date(start.getTime() + ms);
}

describe("export path classification", () => {
  it("routes exactly 31 UTC days to the direct Blob path", () => {
    const decision = classifyExportRange(start, endAfter(MAX_DIRECT_RANGE_MS));

    expect(decision.kind).toBe("direct");
    expect(decision.rangeDays).toBe(31);
    expect(decision.message).toContain("≤31");
  });

  it("routes anything above 31 UTC days to an Export Job, never a Blob", () => {
    const decision = classifyExportRange(
      start,
      endAfter(MAX_DIRECT_RANGE_MS + 1),
    );

    expect(decision.kind).toBe("job");
    expect(decision.message).toContain("Export Job");
    expect(decision.message).toContain("不使用超大 Blob");
  });

  it("keeps exactly 366 UTC days in one job and one millisecond more split", () => {
    expect(
      classifyExportRange(start, endAfter(MAX_JOB_RANGE_MS)).kind,
    ).toBe("job");
    expect(
      classifyExportRange(start, endAfter(MAX_JOB_RANGE_MS + 1)).kind,
    ).toBe("split");
  });

  it("splits long ranges into consecutive 366-day chunks covering the range", () => {
    const end = endAfter(800 * DAY_MS);
    const decision = classifyExportRange(start, end);

    expect(decision.kind).toBe("split");
    expect(decision.chunkCount).toBe(3);
    expect(decision.chunks).toHaveLength(3);
    expect(decision.chunks[0]?.start.toISOString()).toBe(
      "2026-01-01T00:00:00.000Z",
    );
    expect(decision.chunks[0]?.end.toISOString()).toBe(
      "2027-01-02T00:00:00.000Z",
    );
    expect(decision.chunks[1]?.start.toISOString()).toBe(
      "2027-01-02T00:00:00.000Z",
    );
    expect(decision.chunks[2]?.end.toISOString()).toBe(
      "2028-03-11T00:00:00.000Z",
    );
    expect(decision.message).toContain("不支持多 Instrument ZIP");
  });

  it("rejects inverted and invalid ranges", () => {
    expect(classifyExportRange(endAfter(DAY_MS), start).kind).toBe("invalid");
    expect(
      classifyExportRange(new Date(Number.NaN), start).kind,
    ).toBe("invalid");
  });

  it("formats fractional day counts without rounding them to whole days", () => {
    expect(formatRangeDays(31)).toBe("31");
    expect(formatRangeDays(31.5)).toBe("31.50");
  });
});
