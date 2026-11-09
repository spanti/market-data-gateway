import { describe, expect, it } from "vitest";
import {
  DEFAULT_MONITORING_RANGE_ID,
  DEFAULT_PROMETHEUS_POLL_INTERVAL_MS,
  DEFAULT_STATUS_POLL_INTERVAL_MS,
  getMonitoringRange,
  isMonitoringRangeId,
  monitoringRangeFromParams,
  peakRangeLabel,
  resolvePollIntervalMs,
} from "@/lib/monitoring-config";

describe("monitoring time ranges", () => {
  it("defaults to the most recent hour", () => {
    expect(DEFAULT_MONITORING_RANGE_ID).toBe("1h");
    expect(getMonitoringRange(null).id).toBe("1h");
  });

  it("accepts only the declared range ids and falls back for bad input", () => {
    expect(isMonitoringRangeId("15m")).toBe(true);
    expect(isMonitoringRangeId("24h")).toBe(true);
    expect(isMonitoringRangeId("7d")).toBe(false);

    const fallback = getMonitoringRange("7d");
    expect(fallback.id).toBe("1h");
  });

  it("reads the shareable range from URL parameters", () => {
    expect(
      monitoringRangeFromParams(new URLSearchParams("range=6h")).id,
    ).toBe("6h");
    expect(
      monitoringRangeFromParams(new URLSearchParams("range=bogus")).id,
    ).toBe("1h");
    expect(monitoringRangeFromParams(new URLSearchParams()).id).toBe("1h");
  });

  it("states the explicit peak QPS range separately from the rate window", () => {
    const range = getMonitoringRange("6h");
    expect(peakRangeLabel(range)).toBe("最近 6 小时");
    expect(range.qpsRateWindow).toBe("5m");
    expect(range.peakRateWindow).toBe("1m");
    expect(range.peakStepSeconds).toBe(15);
  });
});

describe("polling intervals", () => {
  it("defaults to 5s status and 15s Prometheus polling", () => {
    expect(DEFAULT_STATUS_POLL_INTERVAL_MS).toBe(5000);
    expect(DEFAULT_PROMETHEUS_POLL_INTERVAL_MS).toBe(15000);
  });

  it("accepts a configured positive interval", () => {
    expect(resolvePollIntervalMs("10000", 5000)).toBe(10000);
    expect(resolvePollIntervalMs(" 2500 ", 5000)).toBe(2500);
  });

  it("ignores malformed or sub-second values instead of polling aggressively", () => {
    for (const raw of ["", "abc", "0", "-5", "999", "NaN"]) {
      expect(resolvePollIntervalMs(raw, 5000)).toBe(5000);
    }
  });
});
