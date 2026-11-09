import { describe, expect, it } from "vitest";
import type { CapabilityDocument, CapabilityState } from "@/api/capabilities";
import type { ServiceInstance } from "@/api/endpoints/admin";
import { adminServiceInstanceToRow } from "@/api/endpoints/admin-service-instances";
import { serviceInstanceSource } from "@/runtime/service-instance-source";

function capability(
  overrides: Partial<{
    serviceLifecycle: unknown | null;
    supervisor: unknown;
    compatible: boolean | null;
    document: boolean;
  }> = {},
): CapabilityState {
  const document = {
    server_time: "2026-11-09T12:00:00.000000000Z",
    version: {
      admin_api: "1.0.0",
      contract: 1,
      supported_contracts: [1],
    },
    auth: { required: true, roles: ["SuperAdmin", "Operator", "Auditor"] },
    adapters: [
      overrides.supervisor ?? {
        name: "supervisor",
        kind: "service_lifecycle",
        state: "available",
        reason: null,
      },
    ],
    capabilities:
      overrides.serviceLifecycle === null
        ? {}
        : overrides.serviceLifecycle === undefined
          ? { service_lifecycle: { state: "enabled" } }
          : { service_lifecycle: overrides.serviceLifecycle },
  };
  return {
    mode: "admin-api",
    document:
      overrides.document === false
        ? null
        : (document as unknown as CapabilityDocument),
    compatible:
      overrides.compatible === undefined ? true : overrides.compatible,
    lastSuccessAt: "2026-11-09T12:00:00.000000000Z",
  };
}

describe("serviceInstanceSource", () => {
  it("uses direct Current Contract ops endpoints only in Current Contract mode", () => {
    expect(serviceInstanceSource({ mode: "current-contract", lastSuccessAt: null }))
      .toEqual({ kind: "direct" });
  });

  it("does not guess a source before capability discovery succeeds", () => {
    expect(serviceInstanceSource(null)).toMatchObject({
      kind: "gap",
      reason: { code: "capabilities-unknown" },
    });
    expect(
      serviceInstanceSource({
        mode: "capabilities-unknown",
        lastSuccessAt: null,
        errorCode: "timeout",
        message: "timed out",
      }),
    ).toMatchObject({
      kind: "gap",
      reason: { code: "capabilities-unknown" },
    });
  });

  it("uses the authenticated Admin service-instance seam when the capability is enabled", () => {
    expect(serviceInstanceSource(capability())).toEqual({ kind: "admin" });
  });

  it("shows an explicit capability gap when the Admin service-instance API is missing or unsupported", () => {
    const missing = serviceInstanceSource(capability({ serviceLifecycle: null }));
    const unsupported = serviceInstanceSource(
      capability({ serviceLifecycle: { state: "unsupported" } }),
    );
    for (const source of [missing, unsupported]) {
      expect(source).toEqual({
        kind: "gap",
        reason: {
          code: "backend-unsupported",
          message: "需要 Admin API 服务实例接口（未实现）",
        },
      });
    }
  });

  it("reports an unavailable supervisor adapter instead of falling back to direct ops", () => {
    expect(
      serviceInstanceSource(
        capability({
          supervisor: {
            name: "supervisor",
            kind: "service_lifecycle",
            state: "unavailable",
            reason: "supervisor down",
          },
        }),
      ),
    ).toMatchObject({
      kind: "gap",
      reason: { code: "adapter-unavailable" },
    });
  });

  it("requires a session and a compatible Admin contract", () => {
    expect(
      serviceInstanceSource(capability({ document: false })),
    ).toMatchObject({ kind: "gap", reason: { code: "requires-login" } });
    expect(
      serviceInstanceSource(capability({ compatible: false })),
    ).toMatchObject({
      kind: "gap",
      reason: { code: "backend-unsupported", message: "Admin API 版本不兼容" },
    });
  });
});

describe("adminServiceInstanceToRow", () => {
  const instance: ServiceInstance = {
    instance_id: "api-1",
    service_role: "api",
    display_name: "API primary",
    desired_state: "running",
    observed_state: "running",
    health: "healthy",
    ready: true,
    ready_reason: "collector subscription ready",
    pid: 4242,
    started_at: "2026-11-09T11:00:00.000000000Z",
    last_observed_at: "2026-11-09T12:00:00.000000000Z",
    last_error: null,
    available_actions: ["stop", "reload"],
  };

  it("preserves Admin instance identity, states and available actions", () => {
    expect(adminServiceInstanceToRow(instance)).toEqual({
      instanceId: "api-1",
      role: "api",
      displayName: "API primary",
      desiredState: "running",
      observedState: "running",
      health: "healthy",
      ready: true,
      readyReason: "collector subscription ready",
      pid: 4242,
      startedAt: "2026-11-09T11:00:00.000000000Z",
      lastObservedAt: "2026-11-09T12:00:00.000000000Z",
      lastError: null,
      availableActions: ["stop", "reload"],
    });
  });
});
