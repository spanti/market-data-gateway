import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";
import {
  classifyHttpError,
  normalizeOpsFailure,
  normalizeRestFailure,
  parseBlobFailure,
} from "@/api/http/errors";
import {
  parsePrometheusExposition,
  parsePrometheusPayload,
  PrometheusContractError,
} from "@/api/http/prometheus";
import { apiClient } from "@/api/http/client";
import { server } from "@/test/server";

const envelope = {
  error: {
    code: "invalid_interval",
    message:
      "query parameter 'interval' is required and must be a Bar Interval",
    details: {},
    request_id: "envelope-request-id",
  },
};

describe("REST error normalization", () => {
  it("reads code, message, details and request_id from the unified envelope", () => {
    const error = normalizeRestFailure({
      status: 400,
      headers: { "x-request-id": "header-request-id" },
      data: envelope,
    });

    expect(error).toMatchObject({
      kind: "rest",
      status: 400,
      code: "invalid_interval",
      message:
        "query parameter 'interval' is required and must be a Bar Interval",
      details: {},
      requestId: "envelope-request-id",
    });
  });

  it("falls back to the X-Request-ID header when the envelope omits it", () => {
    const error = normalizeRestFailure({
      status: 500,
      headers: { "x-request-id": "header-only" },
      data: {
        error: {
          code: "internal_error",
          message: "internal server error",
          details: {},
        },
      },
    });

    expect(error.code).toBe("internal_error");
    expect(error.requestId).toBe("header-only");
  });

  it("fails closed when a REST failure body is not the unified envelope", () => {
    const error = normalizeRestFailure({
      status: 502,
      headers: { "X-Request-ID": "proxy-request-id" },
      data: "<html>bad gateway</html>",
    });

    expect(error.code).toBe("unexpected_error_response");
    expect(error.requestId).toBe("proxy-request-id");
    expect(error.message).toContain("502");
  });

  it("normalizes a network failure without inventing a request id", () => {
    const error = normalizeRestFailure({
      status: null,
      headers: {},
      data: undefined,
      transportMessage: "Network Error",
    });

    expect(error).toMatchObject({
      kind: "rest",
      status: null,
      code: "network_error",
      requestId: null,
    });
    expect(error.message).toContain("Network Error");
  });
});

describe("ops error normalization", () => {
  it("does not expect the REST envelope on per-role ops endpoints", () => {
    const error = normalizeOpsFailure({
      status: 503,
      headers: {},
      data: { ready: false, reason: "collector starting" },
    });

    expect(error).toMatchObject({
      kind: "ops",
      status: 503,
      code: "ops_error",
      message: "collector starting",
      requestId: null,
    });
  });

  it("keeps plain-text ops bodies and reports them as operational text", () => {
    const error = normalizeOpsFailure({
      status: 500,
      headers: {},
      data: "Internal Server Error",
    });

    expect(error.kind).toBe("ops");
    expect(error.code).toBe("ops_error");
    expect(error.message).toContain("Internal Server Error");
    expect(error.requestId).toBeNull();
  });
});

describe("Prometheus response adapter", () => {
  it("converts a matrix result into labeled series with numeric points", () => {
    const result = parsePrometheusPayload({
      status: "success",
      data: {
        resultType: "matrix",
        result: [
          {
            metric: { __name__: "mdgw_api_http_requests_total", path: "/x" },
            values: [
              [1760000000, "12.5"],
              [1760000015, "13"],
            ],
          },
        ],
      },
    });

    expect(result.resultType).toBe("matrix");
    expect(result.series).toHaveLength(1);
    expect(result.series[0]?.labels.path).toBe("/x");
    expect(result.series[0]?.points).toEqual([
      { ts: 1760000000, value: 12.5 },
      { ts: 1760000015, value: 13 },
    ]);
  });

  it("throws a contract error for a Prometheus error payload", () => {
    expect(() =>
      parsePrometheusPayload({
        status: "error",
        errorType: "bad_data",
        error: "parse error",
      }),
    ).toThrowError(PrometheusContractError);

    try {
      parsePrometheusPayload({
        status: "error",
        errorType: "bad_data",
        error: "parse error",
      });
    } catch (error) {
      expect(error).toMatchObject({
        code: "prometheus_error",
        message: "parse error",
      });
    }
  });

  it("rejects malformed Prometheus payloads instead of treating them as healthy", () => {
    try {
      parsePrometheusPayload({ status: "success", data: { result: "nope" } });
      throw new Error("expected parsePrometheusPayload to throw");
    } catch (error) {
      expect(error).toBeInstanceOf(PrometheusContractError);
      expect((error as PrometheusContractError).code).toBe(
        "prometheus_malformed",
      );
    }
  });

  it("parses the text exposition format into samples", () => {
    const samples = parsePrometheusExposition(
      [
        "# HELP mdgw_up Whether the service is up",
        "# TYPE mdgw_up gauge",
        'mdgw_up{role="api"} 1',
        'mdgw_api_http_requests_total{path="/api/v1/bars",status="200"} 42',
      ].join("\n"),
    );

    expect(samples).toEqual([
      { name: "mdgw_up", labels: { role: "api" }, value: 1 },
      {
        name: "mdgw_api_http_requests_total",
        labels: { path: "/api/v1/bars", status: "200" },
        value: 42,
      },
    ]);
  });
});

describe("Blob error parsing", () => {
  it("parses a JSON error envelope from a Blob response", async () => {
    const blob = new Blob([JSON.stringify(envelope)], {
      type: "application/json",
    });

    const error = await parseBlobFailure(blob, {
      status: 400,
      headers: { "x-request-id": "blob-header-id" },
    });

    expect(error).toMatchObject({
      kind: "rest",
      code: "invalid_interval",
      requestId: "envelope-request-id",
    });
  });

  it("returns a safe error and header request id for a non-JSON Blob body", async () => {
    const blob = new Blob(["<html>proxy failure</html>"], {
      type: "text/html",
    });

    const error = await parseBlobFailure(blob, {
      status: 502,
      headers: { "X-Request-ID": "blob-proxy-id" },
    });

    expect(error.code).toBe("blob_error_response");
    expect(error.requestId).toBe("blob-proxy-id");
    expect(error.status).toBe(502);
  });
});

describe("global HTTP error classification", () => {
  const base = {
    kind: "rest" as const,
    status: 400,
    code: "x",
    message: "x",
    details: {},
    requestId: null,
  };

  it("turns 401 into a login-required stop", () => {
    expect(classifyHttpError({ ...base, status: 401, code: "unauthenticated" }))
      .toMatchObject({ action: "login-required" });
  });

  it("turns 403 into a permission-denied notice with the missing role", () => {
    expect(
      classifyHttpError({
        ...base,
        status: 403,
        code: "forbidden",
        details: { required_role: "SuperAdmin" },
      }),
    ).toMatchObject({
      action: "permission-denied",
      requiredRole: "SuperAdmin",
    });
  });

  it("turns 409 into a conflict notice that requires a refresh", () => {
    expect(
      classifyHttpError({
        ...base,
        status: 409,
        code: "conflict",
        details: { reason: "version_mismatch" },
      }),
    ).toMatchObject({ action: "conflict", reason: "version_mismatch" });
  });

  it("honours Retry-After for 429", () => {
    expect(
      classifyHttpError({
        ...base,
        status: 429,
        code: "rate_limited",
        details: {},
        headers: { "retry-after": "12" },
      }),
    ).toMatchObject({ action: "retry-after", retryAfterS: 12 });
  });
});

describe("Axios client error wiring", () => {
  it("surfaces the envelope and X-Request-ID through the client", async () => {
    server.use(
      http.get("*/api/v1/bars", () => {
        return HttpResponse.json(envelope, {
          status: 400,
          headers: { "X-Request-ID": "header-request-id" },
        });
      }),
    );

    await expect(apiClient.get("/api/v1/bars")).rejects.toMatchObject({
      kind: "rest",
      code: "invalid_interval",
      requestId: "envelope-request-id",
      status: 400,
    });
  });

  it("normalizes a transport failure as network_error", async () => {
    server.use(
      http.get("*/api/v1/bars", () => {
        return HttpResponse.error();
      }),
    );

    await expect(apiClient.get("/api/v1/bars")).rejects.toMatchObject({
      kind: "rest",
      code: "network_error",
      status: null,
    });
  });
});
