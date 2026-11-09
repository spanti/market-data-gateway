import type { CapabilityState } from "@/api/capabilities";
import type { DisabledReason } from "@/runtime/permissions";

/**
 * Chooses the only truthful Service Instance data source for the active
 * runtime mode.
 *
 * - Current Contract: the real per-Role `/status` + `/readyz` endpoints.
 * - Admin API: the authenticated `/api/v1/admin/service-instances` seam, and
 *   only when the `service_lifecycle` capability is enabled.
 * - Anything else: an explicit capability gap. Admin mode never falls back to
 *   unauthenticated `/status` or `/ops/*` reads.
 */
export type ServiceInstanceSource =
  | { kind: "direct" }
  | { kind: "admin" }
  | { kind: "gap"; reason: DisabledReason };

export const ADMIN_SERVICE_INSTANCE_GAP =
  "需要 Admin API 服务实例接口（未实现）";

function gap(code: DisabledReason["code"], message: string): ServiceInstanceSource {
  return { kind: "gap", reason: { code, message } };
}

export function serviceInstanceSource(
  capability: CapabilityState | null,
): ServiceInstanceSource {
  if (!capability) {
    return gap("capabilities-unknown", "能力未知，暂不可用");
  }

  switch (capability.mode) {
    case "current-contract":
      return { kind: "direct" };
    case "capabilities-unknown":
      return gap("capabilities-unknown", "能力未知，暂不可用");
    case "admin-api": {
      if (capability.compatible === false) {
        return gap("backend-unsupported", "Admin API 版本不兼容");
      }
      if (!capability.document) {
        return gap("requires-login", "需要登录");
      }
      const entry = capability.document.capabilities.service_lifecycle;
      if (!entry || entry.state === "unsupported") {
        return gap("backend-unsupported", ADMIN_SERVICE_INSTANCE_GAP);
      }
      if (entry.state === "adapter_unavailable") {
        return gap("adapter-unavailable", "适配器不可用");
      }
      const supervisor = capability.document.adapters?.find(
        (adapter) =>
          adapter.name === "supervisor" && adapter.kind === "service_lifecycle",
      );
      if (supervisor?.state === "unavailable") {
        return gap("adapter-unavailable", "适配器不可用");
      }
      if (supervisor?.state === "unsupported") {
        return gap("backend-unsupported", ADMIN_SERVICE_INSTANCE_GAP);
      }
      return { kind: "admin" };
    }
  }
}
