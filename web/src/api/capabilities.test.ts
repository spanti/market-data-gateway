import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import {
  discoverCapabilities,
  resetCapabilityCache,
  resolveCapabilityState,
  type AdminApiState,
  type CapabilityCacheEntry,
  type CapabilityDocument,
  type CapabilitiesUnknownState,
  type CapabilityState,
} from "@/api/capabilities";
import { server } from "@/test/server";

const validDocument = {
  schema_version: 1,
  server_time: "2026-11-09T12:00:00.000000000Z",
  version: {
    admin_api: "1.0.0",
    contract: 1,
    supported_contracts: [1],
  },
  auth: {
    required: true,
    mode: "oidc",
    state: "available",
    roles: ["SuperAdmin", "Operator", "Auditor"],
  },
  adapters: [],
  capabilities: {
    feed_subscription: { state: "enabled" },
    service_lifecycle: { state: "unsupported" },
    monitoring_proxy: { state: "adapter_unavailable" },
  },
  limits: { page_default: 50, page_max: 200 },
};

const cachedEntry: CapabilityCacheEntry = {
  document: validDocument as CapabilityDocument,
  lastSuccessAt: "2026-11-09T11:59:00.000Z",
};

function narrowAdmin(state: CapabilityState): AdminApiState {
  if (state.mode !== "admin-api") {
    throw new Error(`expected admin-api, got ${state.mode}`);
  }
  return state;
}

function narrowUnknown(state: CapabilityState): CapabilitiesUnknownState {
  if (state.mode !== "capabilities-unknown") {
    throw new Error(`expected capabilities-unknown, got ${state.mode}`);
  }
  return state;
}

beforeEach(() => {
  resetCapabilityCache();
});

describe("capability mode resolution", () => {
  it("200 with a valid document resolves to Admin API", () => {
    const state = narrowAdmin(
      resolveCapabilityState({
        kind: "response",
        status: 200,
        data: validDocument,
      }),
    );

    expect(state.mode).toBe("admin-api");
    expect(state.document?.version.supported_contracts).toEqual([1]);
    expect(state.compatible).toBe(true);
  });

  it("treats an unknown capability state as unsupported (fail closed)", () => {
    const state = narrowAdmin(
      resolveCapabilityState({
        kind: "response",
        status: 200,
        data: {
          ...validDocument,
          capabilities: {
            feed_subscription: { state: "enabled" },
            future_feature: { state: "maybe" },
            missing_state: {},
          },
        },
      }),
    );

    expect(state.mode).toBe("admin-api");
    expect(state.document?.capabilities.feed_subscription?.state).toBe(
      "enabled",
    );
    expect(state.document?.capabilities.future_feature?.state).toBe(
      "unsupported",
    );
    expect(state.document?.capabilities.missing_state?.state).toBe(
      "unsupported",
    );
  });

  it("marks an Admin API with an unsupported contract version as incompatible", () => {
    const state = narrowAdmin(
      resolveCapabilityState({
        kind: "response",
        status: 200,
        data: {
          ...validDocument,
          version: {
            admin_api: "2.0.0",
            contract: 2,
            supported_contracts: [2],
          },
        },
      }),
    );

    expect(state.mode).toBe("admin-api");
    expect(state.compatible).toBe(false);
  });

  it("401 proves an Admin API exists and leaves the user unauthenticated", () => {
    const state = narrowAdmin(
      resolveCapabilityState({
        kind: "response",
        status: 401,
        data: null,
      }),
    );

    expect(state.mode).toBe("admin-api");
    expect(state.document).toBeNull();
    expect(state.compatible).toBeNull();
  });

  it.each([404, 405, 501])(
    "%i resolves to the Current Contract, not Admin API",
    (status) => {
      const state = resolveCapabilityState({
        kind: "response",
        status,
        data: null,
      });

      expect(state.mode).toBe("current-contract");
    },
  );

  it("a network failure resolves to capabilities-unknown with the cached last success", () => {
    const state = resolveCapabilityState(
      { kind: "network-error", message: "Failed to fetch" },
      cachedEntry,
    );

    expect(state).toMatchObject({
      mode: "capabilities-unknown",
      lastSuccessAt: "2026-11-09T11:59:00.000Z",
      errorCode: "network_error",
    });
  });

  it("a timeout resolves to capabilities-unknown", () => {
    const state = narrowUnknown(
      resolveCapabilityState({
        kind: "timeout",
        message: "timeout of 15000ms exceeded",
      }),
    );

    expect(state.mode).toBe("capabilities-unknown");
    expect(state.errorCode).toBe("timeout");
    expect(state.lastSuccessAt).toBeNull();
  });

  it("a 5xx resolves to capabilities-unknown", () => {
    const state = resolveCapabilityState(
      { kind: "response", status: 503, data: null },
      cachedEntry,
    );

    expect(state.mode).toBe("capabilities-unknown");
    expect(state.lastSuccessAt).toBe("2026-11-09T11:59:00.000Z");
  });

  it("a malformed 200 body resolves to capabilities-unknown, never Current Contract", () => {
    const state = narrowUnknown(
      resolveCapabilityState({
        kind: "response",
        status: 200,
        data: { message: "looks like a proxy page" },
      }),
    );

    expect(state.mode).toBe("capabilities-unknown");
    expect(state.errorCode).toBe("malformed_capabilities");
  });
});

describe("capability discovery over HTTP", () => {
  it("fetches and caches a valid capability document", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(validDocument),
      ),
    );

    const state = await discoverCapabilities();

    expect(state.mode).toBe("admin-api");
    expect(state.lastSuccessAt).not.toBeNull();
  });

  it("annotates a later failure with the cached last success time", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(validDocument),
      ),
    );
    const success = await discoverCapabilities();

    server.use(
      http.get("*/api/v1/admin/capabilities", () =>
        HttpResponse.json(
          {
            error: {
              code: "internal_error",
              message: "internal server error",
              details: {},
              request_id: "boom",
            },
          },
          { status: 500 },
        ),
      ),
    );
    const failure = await discoverCapabilities();

    expect(failure.mode).toBe("capabilities-unknown");
    expect(failure.lastSuccessAt).toBe(success.lastSuccessAt);
  });

  it("treats a transport failure as capabilities-unknown", async () => {
    server.use(
      http.get("*/api/v1/admin/capabilities", () => HttpResponse.error()),
    );

    const state = narrowUnknown(await discoverCapabilities());

    expect(state.mode).toBe("capabilities-unknown");
    expect(state.errorCode).toBe("network_error");
  });

  it("uses the current-contract path when the admin route is absent", async () => {
    const state = await discoverCapabilities();

    expect(state.mode).toBe("current-contract");
  });
});
