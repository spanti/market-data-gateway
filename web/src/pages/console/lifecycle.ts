/**
 * Service Instance lifecycle confirmation and operation state machine.
 *
 * Contract target (admin-api-contract.md §5): Start / Graceful Stop / Reload
 * return 202 Accepted + Operation. Accepted is not success; every definitive
 * state comes from GET /api/v1/admin/operations/{id}. A network break after
 * acceptance resolves to `unknown`, never to failure or success, and the
 * frontend never retries a non-idempotent write automatically.
 */

import type { ServiceLifecycleAction } from "@/api/endpoints/admin";
import { OPERATION_REASON_MAX_LENGTH } from "@/lib/validation";

export type { ServiceLifecycleAction } from "@/api/endpoints/admin";

export const LIFECYCLE_REASON_MAX_LENGTH = OPERATION_REASON_MAX_LENGTH;

export interface LifecycleConfirmationInput {
  action: ServiceLifecycleAction;
  instanceId: string;
  reason: string;
  confirmInstanceName?: string;
}

export interface LifecycleRequestBody {
  reason: string;
  confirm: true;
  confirm_instance_name?: string;
}

export type LifecycleValidation =
  | { valid: true; body: LifecycleRequestBody }
  | {
      valid: false;
      field: "reason" | "confirm_instance_name";
      error: string;
    };

export function validateLifecycleConfirmation(
  input: LifecycleConfirmationInput,
): LifecycleValidation {
  const reason = input.reason.trim();
  if (reason.length === 0) {
    return { valid: false, field: "reason", error: "操作原因不能为空" };
  }
  if (reason.length > LIFECYCLE_REASON_MAX_LENGTH) {
    return {
      valid: false,
      field: "reason",
      error: `操作原因不能超过 ${LIFECYCLE_REASON_MAX_LENGTH} 字符`,
    };
  }

  const requiresInstanceName = input.action === "stop" || input.action === "reload";
  if (requiresInstanceName) {
    const typed = input.confirmInstanceName?.trim() ?? "";
    if (typed !== input.instanceId) {
      return {
        valid: false,
        field: "confirm_instance_name",
        error: `请输入目标实例名 ${input.instanceId} 以确认`,
      };
    }
    return {
      valid: true,
      body: {
        reason,
        confirm: true,
        confirm_instance_name: typed,
      },
    };
  }

  return { valid: true, body: { reason, confirm: true } };
}

export type LifecyclePhase =
  | "pending"
  | "submitting"
  | "accepted"
  | "running"
  | "succeeded"
  | "failed"
  | "unknown";

export interface LifecycleOperationState {
  action: ServiceLifecycleAction;
  instanceId: string;
  phase: LifecyclePhase;
  operationId: string | null;
  requestId: string | null;
  errorCode: string | null;
  errorMessage: string | null;
  lastUpdatedAt: string | null;
}

export type LifecycleEvent =
  | { type: "submit" }
  | {
      type: "accepted";
      /** null when the response did not return an Operation ID. */
      operationId: string | null;
      requestId?: string | null;
      at?: string;
    }
  | {
      type: "operation";
      status: "accepted" | "running" | "succeeded" | "failed" | "unknown";
      errorCode?: string | null;
      errorMessage?: string | null;
      at?: string;
    }
  | {
      type: "submit-failed";
      errorCode: string;
      errorMessage: string;
      at?: string;
    }
  | { type: "disconnect"; at?: string }
  | { type: "query-failed"; at?: string };

export function createLifecycleOperation(
  action: ServiceLifecycleAction,
  instanceId: string,
): LifecycleOperationState {
  return {
    action,
    instanceId,
    phase: "pending",
    operationId: null,
    requestId: null,
    errorCode: null,
    errorMessage: null,
    lastUpdatedAt: null,
  };
}

const IN_FLIGHT: ReadonlySet<LifecyclePhase> = new Set([
  "submitting",
  "accepted",
  "running",
  "succeeded",
  "unknown",
]);

const QUERYABLE: ReadonlySet<LifecyclePhase> = new Set([
  "accepted",
  "running",
  "unknown",
]);

export function canQueryOperation(state: LifecycleOperationState): boolean {
  return state.operationId !== null && QUERYABLE.has(state.phase);
}

export function isTerminalPhase(phase: LifecyclePhase): boolean {
  return phase === "succeeded" || phase === "failed";
}

export function reduceLifecycleOperation(
  state: LifecycleOperationState,
  event: LifecycleEvent,
): LifecycleOperationState {
  switch (event.type) {
    case "submit": {
      // Duplicate-submit lock and no automatic retry from any in-flight or
      // unresolved phase. Only a fresh pending or a definitive failed action
      // may be submitted again, deliberately, by the operator.
      if (state.phase === "pending" || state.phase === "failed") {
        return {
          ...state,
          phase: "submitting",
          errorCode: null,
          errorMessage: null,
        };
      }
      return state;
    }

    case "accepted": {
      if (state.phase !== "submitting") {
        return state;
      }
      return {
        ...state,
        phase: "accepted",
        operationId: event.operationId,
        requestId: event.requestId ?? state.requestId,
        lastUpdatedAt: event.at ?? state.lastUpdatedAt,
      };
    }

    case "operation": {
      if (!QUERYABLE.has(state.phase)) {
        return state;
      }
      return {
        ...state,
        phase: event.status,
        errorCode: event.errorCode ?? null,
        errorMessage: event.errorMessage ?? null,
        lastUpdatedAt: event.at ?? state.lastUpdatedAt,
      };
    }

    case "submit-failed": {
      if (state.phase !== "submitting") {
        return state;
      }
      return {
        ...state,
        phase: "failed",
        errorCode: event.errorCode,
        errorMessage: event.errorMessage,
        lastUpdatedAt: event.at ?? state.lastUpdatedAt,
      };
    }

    case "disconnect": {
      // A broken connection cannot prove that the operation failed or
      // succeeded. Keep the operation ID so it can be queried later.
      if (IN_FLIGHT.has(state.phase) && !isTerminalPhase(state.phase)) {
        return {
          ...state,
          phase: "unknown",
          lastUpdatedAt: event.at ?? state.lastUpdatedAt,
        };
      }
      return state;
    }

    case "query-failed": {
      if (QUERYABLE.has(state.phase)) {
        return {
          ...state,
          phase: "unknown",
          lastUpdatedAt: event.at ?? state.lastUpdatedAt,
        };
      }
      return state;
    }
  }
}

const PHASE_LABELS: Record<LifecyclePhase, string> = {
  pending: "待提交",
  submitting: "提交中",
  accepted: "已接受（未完成）",
  running: "执行中",
  succeeded: "成功",
  failed: "失败",
  unknown: "状态未知",
};

export function lifecyclePhaseLabel(phase: LifecyclePhase): string {
  return PHASE_LABELS[phase];
}

export type LifecyclePhaseTone =
  | "unknown"
  | "info"
  | "warning"
  | "good"
  | "critical";

export function lifecyclePhaseTone(phase: LifecyclePhase): LifecyclePhaseTone {
  switch (phase) {
    case "pending":
      return "unknown";
    case "submitting":
    case "accepted":
    case "running":
      return "warning";
    case "succeeded":
      return "good";
    case "failed":
      return "critical";
    case "unknown":
      return "unknown";
  }
}
