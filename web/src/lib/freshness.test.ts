import { describe, expect, it } from "vitest";
import type {
  CollectorHealth,
  DatasetStatus,
} from "@/api/endpoints/ops";
import {
  summarizeFeedFreshness,
  toDatasetFreshnessRows,
  toFeedFreshnessView,
} from "@/lib/freshness";

const trackedBase = {
  feed: "testfeed",
  instrument_id: "testvenue:AAA/USD",
  interval: "1m" as const,
  last_event_ts: "2026-11-09T12:00:00.000000000Z",
};

function health(entries: CollectorHealth["tracked"]): CollectorHealth {
  return {
    tracked: entries,
    stale: entries.filter((entry) => entry.stale),
    stale_count: entries.filter((entry) => entry.stale).length,
    gap_unrecoverable: [],
    gap_unrecoverable_count: 0,
  };
}

describe("feed freshness countdown", () => {
  it("derives the remaining countdown from server age_s and threshold_s", () => {
    const view = toFeedFreshnessView({
      ...trackedBase,
      age_s: 20,
      threshold_s: 120,
      stale: false,
    });

    expect(view.remainingS).toBe(100);
    expect(view.statusLabel).toBe("正常");
    expect(view.tone).toBe("good");
  });

  it("never shows a stale server entry as healthy, even if the countdown looks positive", () => {
    const view = toFeedFreshnessView({
      ...trackedBase,
      age_s: 10,
      threshold_s: 120,
      stale: true,
    });

    expect(view.tone).toBe("critical");
    expect(view.statusLabel).toBe("失活");
    expect(view.remainingS).toBe(110);
  });

  it("keeps a server-stale entry stale when age is unavailable", () => {
    const view = toFeedFreshnessView({
      ...trackedBase,
      last_event_ts: null,
      age_s: null,
      threshold_s: 120,
      stale: true,
    });

    expect(view.remainingS).toBeNull();
    expect(view.tone).toBe("critical");
    expect(view.statusLabel).toBe("失活");
  });

  it("is unknown when the server gives no measurable age and does not mark stale", () => {
    const view = toFeedFreshnessView({
      ...trackedBase,
      last_event_ts: null,
      age_s: null,
      threshold_s: null,
      stale: false,
    });

    expect(view.remainingS).toBeNull();
    expect(view.tone).toBe("unknown");
    expect(view.statusLabel).toBe("未知");
  });

  it("summarises tracked, stale and unmeasurable entries without inventing health", () => {
    const summary = summarizeFeedFreshness(
      health([
        {
          ...trackedBase,
          age_s: 20,
          threshold_s: 120,
          stale: false,
        },
        {
          ...trackedBase,
          instrument_id: "testvenue:BBB/USD",
          age_s: 200,
          threshold_s: 120,
          stale: true,
        },
        {
          ...trackedBase,
          instrument_id: "testvenue:CCC/USD",
          last_event_ts: null,
          age_s: null,
          threshold_s: null,
          stale: false,
        },
      ]),
    );

    expect(summary).toEqual({
      tracked: 3,
      stale: 1,
      unknown: 1,
      tone: "critical",
    });
  });

  it("has no health summary when nothing is tracked", () => {
    expect(summarizeFeedFreshness(health([]))).toEqual({
      tracked: 0,
      stale: 0,
      unknown: 0,
      tone: "unknown",
    });
  });
});

describe("dataset freshness", () => {
  const dataset: DatasetStatus = {
    dataset: "bars",
    schema_version: 1,
    last_flushed_ts: "2026-11-09T12:00:00.000000000Z",
    active_file_count: 2,
    median_file_size_bytes: 512,
    intervals: {
      "1m": {
        last_flushed_ts: "2026-11-09T12:00:00.000000000Z",
        active_file_count: 1,
        median_file_size_bytes: 500,
      },
      "5m": {
        last_flushed_ts: "2026-11-09T11:59:30.000000000Z",
        active_file_count: 1,
        median_file_size_bytes: 524,
      },
    },
  };

  it("derives per-interval flush ages from the dataset status", () => {
    const rows = toDatasetFreshnessRows(
      dataset,
      Date.parse("2026-11-09T12:01:00.000000000Z"),
    );

    expect(rows.map((row) => row.interval)).toEqual(["1m", "5m"]);
    expect(rows[0]).toMatchObject({
      lastFlushedTs: "2026-11-09T12:00:00.000000000Z",
      ageS: 60,
      activeFileCount: 1,
      medianFileSizeBytes: 500,
    });
    expect(rows[1]?.ageS).toBe(90);
  });

  it("keeps a null flush timestamp as unknown rather than zero", () => {
    const rows = toDatasetFreshnessRows(
      {
        ...dataset,
        intervals: {
          "1m": {
            last_flushed_ts: null,
            active_file_count: 0,
            median_file_size_bytes: null,
          },
        },
      },
      Date.parse("2026-11-09T12:01:00.000000000Z"),
    );

    expect(rows[0]?.ageS).toBeNull();
    expect(rows[0]?.medianFileSizeBytes).toBeNull();
  });
});
