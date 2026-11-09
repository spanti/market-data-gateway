import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";

const datasetStatus = {
  dataset: "bars",
  schema_version: 1,
  last_flushed_ts: null,
  active_file_count: 0,
  median_file_size_bytes: null,
  intervals: {},
};

const apiStatus = {
  ...datasetStatus,
  service: "api",
  role: "api",
  uptime_s: 0,
  http: { host: "127.0.0.1", port: null },
  zmq: { publish_endpoint: null, collector_endpoint: "tcp://127.0.0.1:5555" },
  messages_received: 0,
  messages_forwarded: 0,
  collector: { ready: false, last_bar_ts: null, status: null },
};

const collectorStatus = {
  service: "collector",
  role: "collector",
  ready: true,
  uptime_s: 0,
  feed: null,
  last_ingest_ts: null,
  last_bar_ts: null,
  bars_ingested: 0,
  bars_written: 0,
  stale_count: 0,
  gap_unrecoverable_count: 0,
  write_queue_depth: 0,
  write_dropped_bars: 0,
  write_lag_bars: 0,
  write_last_flush_ts: null,
  write_last_flush_duration_s: null,
  write_sink: {
    mode: "buffered",
    queue_depth: 0,
    queue_capacity: 0,
    pending_bytes: 0,
    dropped_bars: 0,
    lag_bars: 0,
    dropped_identities: 0,
    flushes: 0,
    flush_failures: 0,
    persisted_bars: 0,
    last_flush_ts: null,
    last_flush_duration_s: null,
  },
  quarantine: {
    schema_version: 1,
    root: "/tmp/quarantine",
    retention_days: 30,
    records_written: 0,
    write_failures: 0,
    last_write_ts: null,
    active_file_count: 0,
    active_bytes: 0,
    gc_runs: 0,
    last_gc: null,
    records_by_source_stage: {},
    records_by_reason_code: {},
    records_by_stage_and_reason: [],
  },
  health: {
    tracked: [],
    stale: [],
    stale_count: 0,
    gap_unrecoverable: [],
    gap_unrecoverable_count: 0,
  },
};

const replayStatus = {
  service: "replay",
  role: "replay",
  uptime_s: 0,
  control_endpoint: null,
  publish_endpoint: null,
  active_sessions: [],
  sessions: [],
  instrument_snapshot: {
    status: "missing",
    loaded: false,
    snapshot: null,
    count: 0,
  },
};

/**
 * Default handlers model the current repository state: no Admin API, and the
 * real Current Contract endpoints return empty/no-data payloads. Individual
 * tests override these handlers with contract fixtures.
 */
export const server = setupServer(
  http.get("*/api/v1/admin/capabilities", () => {
    return HttpResponse.json(
      {
        error: {
          code: "not_found",
          message: "not found",
          details: {},
          request_id: "default-current-contract",
        },
      },
      {
        status: 404,
        headers: { "X-Request-ID": "default-current-contract" },
      },
    );
  }),

  // Status-like paths share one dispatcher because a `*/status` mask would
  // also match `/api/v1/status` and the two ops prefixes.
  http.get("*/status", ({ request }) => {
    const path = new URL(request.url).pathname;
    if (path === "/api/v1/status") {
      return HttpResponse.json(datasetStatus);
    }
    if (path === "/ops/collector/status") {
      return HttpResponse.json(collectorStatus);
    }
    if (path === "/ops/replay/status") {
      return HttpResponse.json(replayStatus);
    }
    if (path === "/status") {
      return HttpResponse.json(apiStatus);
    }
    return HttpResponse.json(
      { error: { code: "not_found", message: "not found", details: {} } },
      { status: 404 },
    );
  }),

  http.get("*/readyz", ({ request }) => {
    const path = new URL(request.url).pathname;
    if (path === "/ops/collector/readyz") {
      return HttpResponse.json({
        ready: true,
        reason: "collector ready",
        service: "collector",
      });
    }
    if (path === "/ops/replay/readyz") {
      return HttpResponse.json({
        ready: true,
        reason: "replay service ready",
        service: "replay",
      });
    }
    return HttpResponse.json({
      ready: true,
      reason: "collector optional and not connected",
      collector_ready: false,
    });
  }),

  http.get("*/api/v1/instruments", () => {
    return HttpResponse.json({ instruments: [] });
  }),

  http.get("*/prometheus/api/v1/query_range", () => {
    return HttpResponse.json({
      status: "success",
      data: { resultType: "matrix", result: [] },
    });
  }),

  http.get("*/prometheus/api/v1/query", () => {
    return HttpResponse.json({
      status: "success",
      data: { resultType: "vector", result: [] },
    });
  }),
);
