import type { CapabilityState } from "@/api/capabilities";
import {
  adminCompatibilityReason,
  CAPABILITY_KEYS,
} from "@/runtime/capability-gates";
import {
  capabilityDisabledReason,
  type DisabledReason,
} from "@/runtime/permissions";

/**
 * Login is offered only when capability discovery says the Admin API is
 * compatible and OIDC is usable. Explicit `unavailable`/`unsupported` states
 * and unknown capability states fail closed with a visible reason.
 */
export function loginDisabledReason(
  capability: CapabilityState | null,
): DisabledReason | null {
  if (!capability || capability.mode !== "admin-api") {
    return null;
  }
  const incompatible = adminCompatibilityReason(capability);
  if (incompatible) {
    return incompatible;
  }
  const document = capability.document;
  if (!document) {
    // A 401 capability probe proves an Admin API exists but does not describe
    // its auth adapters; the login entry remains available.
    return null;
  }
  if (document.auth.state === "unavailable") {
    return {
      code: "adapter-unavailable",
      message: "OIDC 暂不可用",
    };
  }
  if (document.auth.state === "unsupported") {
    return {
      code: "backend-unsupported",
      message: "后端未实现 OIDC",
    };
  }
  return capabilityDisabledReason(
    document.capabilities[CAPABILITY_KEYS.oidcSession],
  );
}
