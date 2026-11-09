import type {
  CapabilityAvailability,
  CapabilityEntry,
} from "@/api/capabilities";
import type { AccessRole } from "@/runtime/roles";

export type { AccessRole } from "@/runtime/roles";

/**
 * UI-side permission model. The server remains the only security boundary;
 * these checks only decide whether a control is enabled, disabled-with-reason,
 * or a gated route renders a forbidden state.
 *
 * `anonymous` means Current Contract (read-only, no Admin API).
 * `unauthenticated` means an Admin API exists but no session/role is assigned.
 * `unknown` means capability discovery failed; nothing may be assumed.
 */

export type PermissionSubject =
  | { kind: "anonymous" }
  | { kind: "unauthenticated" }
  | { kind: "unknown" }
  | { kind: "role"; role: AccessRole };

export const ALL_ACTIONS = [
  "monitoring.view",
  "bars.view",
  "quality.view",
  "export.csv.direct",
  "export.job.create",
  "export.job.view.own",
  "export.job.view.all",
  "feed.subscription.view",
  "feed.subscription.write",
  "flow.control.view",
  "flow.control.write",
  "backfill.job.view",
  "backfill.job.create",
  "backfill.job.cancel",
  "ws.session.subscribe",
  "ws.debug.freeform",
  "service.instance.view",
  "service.lifecycle.start",
  "service.lifecycle.stop",
  "service.lifecycle.reload",
  "audit.view",
  "monitoring.proxy.query",
  "user.role.manage",
] as const;

export type Action = (typeof ALL_ACTIONS)[number];

const VIEW_ACTIONS: readonly Action[] = [
  "monitoring.view",
  "bars.view",
  "quality.view",
  "feed.subscription.view",
  "flow.control.view",
  "backfill.job.view",
  "service.instance.view",
  "audit.view",
  "monitoring.proxy.query",
];

const OPERATOR_WRITE_ACTIONS: readonly Action[] = [
  "feed.subscription.write",
  "flow.control.write",
  "backfill.job.create",
  "backfill.job.cancel",
];

const LIFECYCLE_ACTIONS: readonly Action[] = [
  "service.lifecycle.start",
  "service.lifecycle.stop",
  "service.lifecycle.reload",
];

const ROLES: readonly AccessRole[] = ["SuperAdmin", "Operator", "Auditor"];

const ROLE_ACTIONS: Record<AccessRole, ReadonlySet<Action>> = {
  SuperAdmin: new Set<Action>([
    ...VIEW_ACTIONS,
    ...OPERATOR_WRITE_ACTIONS,
    ...LIFECYCLE_ACTIONS,
    "export.csv.direct",
    "export.job.create",
    "export.job.view.own",
    "export.job.view.all",
    "ws.session.subscribe",
    "ws.debug.freeform",
    "user.role.manage",
  ]),
  Operator: new Set<Action>([
    ...VIEW_ACTIONS,
    ...OPERATOR_WRITE_ACTIONS,
    "export.csv.direct",
    "export.job.create",
    "export.job.view.own",
    "ws.session.subscribe",
    "ws.debug.freeform",
  ]),
  Auditor: new Set<Action>([
    ...VIEW_ACTIONS,
    "export.csv.direct",
    "export.job.create",
    "export.job.view.own",
    "ws.session.subscribe",
  ]),
};

/**
 * Current Contract: anonymous read-only. No role is assigned. Controlled
 * WebSocket subscriptions are allowed; all admin mutations are not.
 */
const ANONYMOUS_ACTIONS: ReadonlySet<Action> = new Set<Action>([
  "monitoring.view",
  "bars.view",
  "quality.view",
  "export.csv.direct",
  "ws.session.subscribe",
  "service.instance.view",
]);

/**
 * Derived from the single Access Role matrix above, so a capability change
 * cannot drift between `can()` and the disabled-reason copy. Role order is
 * the canonical matrix order (SuperAdmin, Operator, Auditor).
 */
const REQUIRED_ROLES: Record<Action, readonly AccessRole[]> = ALL_ACTIONS.reduce(
  (required, action) => {
    required[action] = ROLES.filter((role) => ROLE_ACTIONS[role].has(action));
    return required;
  },
  {} as Record<Action, readonly AccessRole[]>,
);

export function can(subject: PermissionSubject, action: Action): boolean {
  if (subject.kind === "anonymous") {
    return ANONYMOUS_ACTIONS.has(action);
  }
  if (subject.kind === "role") {
    return ROLE_ACTIONS[subject.role].has(action);
  }
  return false;
}

export type DisabledReasonCode =
  | "requires-admin-api"
  | "requires-login"
  | "capabilities-unknown"
  | "requires-role"
  | "backend-unsupported"
  | "adapter-unavailable";

export interface DisabledReason {
  code: DisabledReasonCode;
  message: string;
}

function roleMessage(roles: readonly AccessRole[]): string {
  if (roles.length === 1) {
    return `需要 ${roles[0] ?? "更高"} 角色`;
  }
  return `需要 ${roles.join(" 或 ")} 角色`;
}

/**
 * Every unavailable control must carry this visible explanation. A disabled
 * control without a reason is not acceptable.
 */
export function disabledReasonFor(
  subject: PermissionSubject,
  action: Action,
): DisabledReason | null {
  if (can(subject, action)) {
    return null;
  }
  if (subject.kind === "anonymous") {
    return { code: "requires-admin-api", message: "需要 Admin API" };
  }
  if (subject.kind === "unauthenticated") {
    return { code: "requires-login", message: "需要登录" };
  }
  if (subject.kind === "unknown") {
    return {
      code: "capabilities-unknown",
      message: "能力未知，暂不可用",
    };
  }
  return {
    code: "requires-role",
    message: roleMessage(REQUIRED_ROLES[action]),
  };
}

export function capabilityDisabledReason(
  entry: Pick<CapabilityEntry, "state"> | undefined | null,
): DisabledReason | null {
  if (!entry) {
    // A missing capability key is treated as unsupported (fail closed).
    return { code: "backend-unsupported", message: "后端未实现" };
  }
  const state: CapabilityAvailability = entry.state;
  if (state === "enabled") {
    return null;
  }
  if (state === "adapter_unavailable") {
    return { code: "adapter-unavailable", message: "适配器不可用" };
  }
  return { code: "backend-unsupported", message: "后端未实现" };
}
