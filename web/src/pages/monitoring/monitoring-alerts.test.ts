import { describe, expect, it } from "vitest";
import {
  deriveMonitoringAlerts,
  type MonitoringAlertInput,
} from "@/pages/monitoring/monitoring-alerts";

const base: MonitoringAlertInput = {
  directOpsEnabled: true,
  opsGapReason: null,
  staleFeeds: [],
  staleVenues: [],
  notReadyServices: [],
  sourceFailures: [],
  capabilityGaps: [],
};

const lastEvent = "2026-11-09T11:55:00.000Z";
const lastSuccess = Date.parse("2026-11-09T12:00:00.000Z");

describe("monitoring in-page alerts", () => {
  it("derives a critical alert from a real stale Feed Freshness entry", () => {
    const alerts = deriveMonitoringAlerts({
      ...base,
      staleFeeds: [
        {
          instrumentId: "venue-a:AAA/USD",
          feed: "testfeed",
          interval: "1m",
          ageS: 182,
          thresholdS: 120,
          lastEventTs: lastEvent,
        },
      ],
    });

    expect(alerts).toHaveLength(1);
    expect(alerts[0]).toMatchObject({
      tone: "critical",
      title: "Feed Freshness 失活",
    });
    expect(alerts[0]?.detail).toContain("venue-a:AAA/USD");
    expect(alerts[0]?.detail).toContain("age 182s");
    expect(alerts[0]?.evidence).toContain("2026-11-09 11:55:00 UTC");
  });

  it("keeps backend raw errors verbatim and separate from UI copy", () => {
    const raw = "upstream · failed：timeout";
    const alerts = deriveMonitoringAlerts({
      ...base,
      sourceFailures: [
        { label: "Collector /status", error: raw, lastSuccessAt: lastSuccess },
      ],
    });

    expect(alerts).toHaveLength(1);
    expect(alerts[0]?.rawError).toBe(raw);
    expect(alerts[0]?.title).not.toContain("·");
    expect(alerts[0]?.title).not.toContain("：");
    expect(alerts[0]?.detail ?? "").not.toContain("·");
    expect(alerts[0]?.detail ?? "").not.toContain("：");
    expect(alerts[0]?.evidence).toContain("2026-11-09 12:00:00 UTC");
  });

  it("derives service, venue and capability alerts from loaded facts only", () => {
    const alerts = deriveMonitoringAlerts({
      ...base,
      staleVenues: [{ venue: "venue-a", staleCount: 2 }],
      notReadyServices: [
        { role: "collector", reason: "starting" },
        { role: "api", instanceId: "api-2", reason: "draining" },
      ],
      capabilityGaps: [
        {
          title: "Feed Freshness 能力未知",
          reason: "当前模式不暴露 collector /status",
        },
      ],
    });

    expect(alerts.map((alert) => alert.title)).toEqual([
      "上游 venue-a 有 Feed 失活",
      "Service Role collector 未就绪",
      "Service Instance api-2 未就绪",
      "Feed Freshness 能力未知",
    ]);
    expect(alerts[0]?.tone).toBe("warning");
    expect(alerts[1]?.tone).toBe("warning");
    expect(alerts[2]?.tone).toBe("warning");
    expect(alerts[3]?.tone).toBe("info");
  });

  it("invents no alert when every loaded condition is healthy", () => {
    expect(deriveMonitoringAlerts(base)).toEqual([]);
  });
});
