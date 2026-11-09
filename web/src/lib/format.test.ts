import { describe, expect, it } from "vitest";
import {
  formatBytes,
  formatDurationSeconds,
  formatLatencyMs,
  formatNumber,
  formatUtcDateTime,
  formatUtcTime,
} from "@/lib/format";

describe("value formatting", () => {
  it("formats numbers with explicit unknown for null", () => {
    expect(formatNumber(1234.567)).toBe("1,234.57");
    expect(formatNumber(0)).toBe("0");
    expect(formatNumber(null)).toBe("—");
  });

  it("formats durations from server seconds", () => {
    expect(formatDurationSeconds(42)).toBe("42s");
    expect(formatDurationSeconds(754)).toBe("12m 34s");
    expect(formatDurationSeconds(3900)).toBe("1h 05m");
    expect(formatDurationSeconds(183600)).toBe("2d 03h");
    expect(formatDurationSeconds(null)).toBe("—");
  });

  it("formats histogram seconds as milliseconds", () => {
    expect(formatLatencyMs(0.25)).toBe("250 ms");
    expect(formatLatencyMs(0.005)).toBe("5 ms");
    expect(formatLatencyMs(null)).toBe("—");
  });

  it("formats byte gauges without inventing a total", () => {
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(5572)).toBe("5.44 KB");
    expect(formatBytes(null)).toBe("—");
  });

  it("formats timestamps explicitly as UTC", () => {
    expect(formatUtcDateTime("2026-11-09T12:34:56.000000000Z")).toBe(
      "2026-11-09 12:34:56 UTC",
    );
    expect(formatUtcTime("2026-11-09T12:34:56.000000000Z")).toBe(
      "12:34:56 UTC",
    );
    expect(formatUtcDateTime(null)).toBe("—");
    expect(formatUtcDateTime("not-a-timestamp")).toBe("—");
  });
});
