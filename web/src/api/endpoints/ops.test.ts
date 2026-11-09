import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import {
  fetchApiReady,
  fetchApiStatus,
  fetchCollectorReady,
  fetchCollectorStatus,
  fetchDatasetStatus,
  fetchReplayReady,
  fetchReplayStatus,
} from "@/api/endpoints/ops";
import { server } from "@/test/server";

describe("ops adapters", () => {
  it("reads Dataset status from the Dataset-only endpoint", async () => {
    let path = "";
    server.use(
      http.get("*/api/v1/status", ({ request }) => {
        path = new URL(request.url).pathname;
        return HttpResponse.json({
          dataset: "bars",
          schema_version: 1,
          last_flushed_ts: null,
          active_file_count: 0,
          median_file_size_bytes: null,
          intervals: {},
        });
      }),
    );

    const status = await fetchDatasetStatus();

    expect(path).toBe("/api/v1/status");
    expect(status.intervals).toEqual({});
  });

  it("reads the API role status from /status", async () => {
    let path = "";
    server.use(
      http.get("*/status", ({ request }) => {
        path = new URL(request.url).pathname;
        return HttpResponse.json({
          service: "api",
          role: "api",
          uptime_s: 12,
          http: { host: "127.0.0.1", port: 8080 },
          zmq: { publish_endpoint: null, collector_endpoint: "tcp://x" },
          messages_received: 0,
          messages_forwarded: 0,
          collector: { ready: false, last_bar_ts: null, status: null },
          dataset: "bars",
          schema_version: 1,
          last_flushed_ts: null,
          active_file_count: 0,
          median_file_size_bytes: null,
          intervals: {},
        });
      }),
    );

    const status = await fetchApiStatus();

    expect(path).toBe("/status");
    expect(status.uptime_s).toBe(12);
  });

  it("reads collector and replay ops through their same-origin prefixes", async () => {
    const paths: string[] = [];
    server.use(
      http.get("*/ops/collector/status", ({ request }) => {
        paths.push(new URL(request.url).pathname);
        return HttpResponse.json({ service: "collector", role: "collector" });
      }),
      http.get("*/ops/replay/status", ({ request }) => {
        paths.push(new URL(request.url).pathname);
        return HttpResponse.json({ service: "replay", role: "replay" });
      }),
    );

    await fetchCollectorStatus();
    await fetchReplayStatus();

    expect(paths).toEqual(["/ops/collector/status", "/ops/replay/status"]);
  });

  it("treats a 503 readiness body as data, not as a failed request", async () => {
    server.use(
      http.get("*/readyz", ({ request }) => {
        const path = new URL(request.url).pathname;
        if (path === "/ops/collector/readyz") {
          return HttpResponse.json(
            {
              ready: false,
              reason: "collector starting",
              service: "collector",
            },
            { status: 503 },
          );
        }
        if (path === "/ops/replay/readyz") {
          return HttpResponse.json(
            {
              ready: false,
              reason: "replay service starting",
              service: "replay",
            },
            { status: 503 },
          );
        }
        return HttpResponse.json(
          { ready: false, reason: "waiting for collector subscription" },
          { status: 503 },
        );
      }),
    );

    await expect(fetchApiReady()).resolves.toEqual({
      ready: false,
      reason: "waiting for collector subscription",
    });
    await expect(fetchCollectorReady()).resolves.toMatchObject({
      ready: false,
      service: "collector",
    });
    await expect(fetchReplayReady()).resolves.toMatchObject({
      ready: false,
      service: "replay",
    });
  });

  it("still rejects a non-503 readiness failure", async () => {
    server.use(
      http.get("*/ops/replay/readyz", () =>
        HttpResponse.json(
          { error: "boom" },
          { status: 500 },
        ),
      ),
    );

    await expect(fetchReplayReady()).rejects.toMatchObject({
      code: "ops_error",
      status: 500,
    });
  });
});
