import { describe, expect, it } from "vitest";
import type { CapabilityState } from "@/api/capabilities";
import { resolveWsTarget } from "@/api/ws/ws-target";

const location = { protocol: "https:", host: "console.example.internal" };

function adminCapability(
  ws: unknown,
  compatible = true,
): CapabilityState {
  return {
    mode: "admin-api",
    compatible,
    lastSuccessAt: "2026-11-09T12:00:00.000000000Z",
    document: {
      server_time: "2026-11-09T12:00:00.000000000Z",
      version: { admin_api: "1.0.0", contract: 1, supported_contracts: [1] },
      auth: { required: true, roles: ["SuperAdmin"] },
      adapters: [],
      capabilities: ws === undefined ? {} : { ws_admin_session: ws },
    } as never,
  };
}

describe("resolveWsTarget", () => {
  it("uses the public same-origin /ws only in Current Contract mode", () => {
    expect(
      resolveWsTarget(
        { mode: "current-contract", lastSuccessAt: null },
        location,
      ),
    ).toEqual({
      url: "wss://console.example.internal/ws",
      reason: null,
      source: "current-contract",
    });
  });

  it("does not open any socket while capability discovery is pending or unknown", () => {
    expect(resolveWsTarget(null, location)).toEqual({
      url: null,
      reason: { code: "capabilities-unknown", message: "能力未知，暂不可用" },
      source: "unknown",
    });
    expect(
      resolveWsTarget(
        {
          mode: "capabilities-unknown",
          lastSuccessAt: null,
          errorCode: "timeout",
          message: "timed out",
        },
        location,
      ),
    ).toEqual({
      url: null,
      reason: { code: "capabilities-unknown", message: "能力未知，暂不可用" },
      source: "unknown",
    });
  });

  it("targets the authenticated Admin WebSocket path declared by capabilities", () => {
    expect(
      resolveWsTarget(
        adminCapability({
          state: "enabled",
          path: "/api/v1/admin/ws",
        }),
        location,
      ),
    ).toEqual({
      url: "wss://console.example.internal/api/v1/admin/ws",
      reason: null,
      source: "admin-api",
    });
  });

  it("refuses to connect to the public /ws in Admin mode when the Admin WS is unsupported", () => {
    expect(
      resolveWsTarget(adminCapability({ state: "unsupported" }), location),
    ).toMatchObject({
      url: null,
      source: "admin-api",
      reason: { code: "backend-unsupported" },
    });
  });

  it("requires a session and a compatible Admin contract", () => {
    expect(resolveWsTarget(adminCapability(undefined), location)).toMatchObject({
      url: null,
      reason: { code: "backend-unsupported" },
    });
    expect(
      resolveWsTarget(adminCapability({ state: "enabled" }, false), location),
    ).toMatchObject({
      url: null,
      reason: { code: "backend-unsupported", message: "Admin API 版本不兼容" },
    });
    const anonymous: CapabilityState = {
      mode: "admin-api",
      compatible: null,
      lastSuccessAt: null,
      document: null,
    };
    expect(resolveWsTarget(anonymous, location)).toMatchObject({
      url: null,
      reason: { code: "requires-login" },
    });
  });
});
