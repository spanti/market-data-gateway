import type { CapabilityState } from "@/api/capabilities";
import type { StatusTone } from "@/components/status-badge";

/** Runtime mode plus the pre-resolution state. */
export type RuntimeModeKind =
  | "discovering"
  | "current-contract"
  | "admin-api"
  | "capabilities-unknown";

export interface ModePresentation {
  tone: StatusTone;
  label: string;
  detail: string;
}

const MODE_PRESENTATION: Record<RuntimeModeKind, ModePresentation> = {
  discovering: {
    tone: "unknown",
    label: "发现能力中",
    detail: "能力发现完成前不启用管理写。",
  },
  "current-contract": {
    tone: "unknown",
    label: "Current Contract",
    detail: "真实状态与公开 /ws 可用。管理写需要 Admin API。",
  },
  "admin-api": {
    tone: "info",
    label: "Admin API",
    detail:
      "Service Instance 使用 service-instance 接缝。WebSocket 仅使用 /api/v1/admin/ws。",
  },
  "capabilities-unknown": {
    tone: "warning",
    label: "能力未知",
    detail: "不连接任何 WebSocket，管理写保持禁用。",
  },
};

export function capabilityModeKind(
  capability: CapabilityState | null,
): RuntimeModeKind {
  return capability?.mode ?? "discovering";
}

/**
 * One presentation map for runtime-mode badges. Pages may override the label
 * or detail for their own context, but must not re-implement the mode cascade.
 */
export function capabilityModePresentation(
  capability: CapabilityState | null,
  overrides: Partial<Record<RuntimeModeKind, Partial<ModePresentation>>> = {},
): ModePresentation {
  const kind = capabilityModeKind(capability);
  return { ...MODE_PRESENTATION[kind], ...overrides[kind] };
}
