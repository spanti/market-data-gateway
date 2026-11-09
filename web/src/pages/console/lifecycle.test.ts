import { describe, expect, it } from "vitest";
import {
  canQueryOperation,
  createLifecycleOperation,
  lifecyclePhaseLabel,
  reduceLifecycleOperation,
  validateLifecycleConfirmation,
} from "@/pages/console/lifecycle";

describe("lifecycle confirmation rules", () => {
  it("requires a non-empty operation reason within the length limit", () => {
    expect(
      validateLifecycleConfirmation({
        action: "start",
        instanceId: "api",
        reason: "   ",
      }),
    ).toMatchObject({ valid: false, field: "reason" });

    expect(
      validateLifecycleConfirmation({
        action: "start",
        instanceId: "api",
        reason: "x".repeat(1001),
      }),
    ).toMatchObject({ valid: false, field: "reason" });

    expect(
      validateLifecycleConfirmation({
        action: "start",
        instanceId: "api",
        reason: "rotate certificates",
      }),
    ).toEqual({
      valid: true,
      body: { reason: "rotate certificates", confirm: true },
    });
  });

  it("requires typing the exact target instance name for Stop and Reload", () => {
    for (const action of ["stop", "reload"] as const) {
      expect(
        validateLifecycleConfirmation({
          action,
          instanceId: "collector",
          reason: "drain",
          confirmInstanceName: "",
        }),
      ).toMatchObject({ valid: false, field: "confirm_instance_name" });

      expect(
        validateLifecycleConfirmation({
          action,
          instanceId: "collector",
          reason: "drain",
          confirmInstanceName: "collector-1",
        }),
      ).toMatchObject({ valid: false, field: "confirm_instance_name" });

      expect(
        validateLifecycleConfirmation({
          action,
          instanceId: "collector",
          reason: "drain",
          confirmInstanceName: "collector",
        }),
      ).toEqual({
        valid: true,
        body: {
          reason: "drain",
          confirm: true,
          confirm_instance_name: "collector",
        },
      });
    }
  });

  it("does not require an instance-name confirmation for Start", () => {
    const result = validateLifecycleConfirmation({
      action: "start",
      instanceId: "api",
      reason: "bring back",
      confirmInstanceName: "wrong",
    });
    expect(result).toMatchObject({ valid: true });
  });
});

describe("lifecycle operation state machine", () => {
  it("starts pending and locks the submit during submission", () => {
    let state = createLifecycleOperation("stop", "api");
    expect(state.phase).toBe("pending");
    expect(lifecyclePhaseLabel(state.phase)).toBe("待提交");

    state = reduceLifecycleOperation(state, { type: "submit" });
    expect(state.phase).toBe("submitting");

    const duplicate = reduceLifecycleOperation(state, { type: "submit" });
    expect(duplicate.phase).toBe("submitting");
  });

  it("treats 202 Accepted as accepted, never as success", () => {
    let state = createLifecycleOperation("reload", "api");
    state = reduceLifecycleOperation(state, { type: "submit" });
    state = reduceLifecycleOperation(state, {
      type: "accepted",
      operationId: "op_1",
      requestId: "req_1",
    });

    expect(state.phase).toBe("accepted");
    expect(state.operationId).toBe("op_1");
    expect(lifecyclePhaseLabel(state.phase)).toContain("已接受");
    expect(lifecyclePhaseLabel(state.phase)).not.toContain("成功");
  });

  it("follows the operation status and only succeeds on a definitive success", () => {
    let state = createLifecycleOperation("stop", "api");
    state = reduceLifecycleOperation(state, { type: "submit" });
    state = reduceLifecycleOperation(state, {
      type: "accepted",
      operationId: "op_1",
    });

    state = reduceLifecycleOperation(state, {
      type: "operation",
      status: "running",
    });
    expect(state.phase).toBe("running");

    state = reduceLifecycleOperation(state, {
      type: "operation",
      status: "succeeded",
    });
    expect(state.phase).toBe("succeeded");
  });

  it("maps a definitive failed operation to failed", () => {
    let state = createLifecycleOperation("reload", "api");
    state = reduceLifecycleOperation(state, { type: "submit" });
    state = reduceLifecycleOperation(state, {
      type: "accepted",
      operationId: "op_1",
    });
    state = reduceLifecycleOperation(state, {
      type: "operation",
      status: "failed",
      errorCode: "config_invalid",
      errorMessage: "invalid config",
    });

    expect(state.phase).toBe("failed");
    expect(state.errorCode).toBe("config_invalid");
  });

  it("maps a definitive submit rejection to failed", () => {
    let state = createLifecycleOperation("stop", "api");
    state = reduceLifecycleOperation(state, { type: "submit" });
    state = reduceLifecycleOperation(state, {
      type: "submit-failed",
      errorCode: "forbidden",
      errorMessage: "needs SuperAdmin",
    });

    expect(state.phase).toBe("failed");
    expect(state.errorCode).toBe("forbidden");
  });

  it("resolves a disconnect after acceptance to unknown, not failure or success", () => {
    let state = createLifecycleOperation("reload", "api");
    state = reduceLifecycleOperation(state, { type: "submit" });
    state = reduceLifecycleOperation(state, {
      type: "accepted",
      operationId: "op_1",
    });
    const disconnected = reduceLifecycleOperation(state, {
      type: "disconnect",
    });

    expect(disconnected.phase).toBe("unknown");
    expect(disconnected.operationId).toBe("op_1");
    expect(lifecyclePhaseLabel(disconnected.phase)).toBe("状态未知");
  });

  it("resolves a disconnect while the write is in flight to unknown", () => {
    let state = createLifecycleOperation("stop", "api");
    state = reduceLifecycleOperation(state, { type: "submit" });
    state = reduceLifecycleOperation(state, { type: "disconnect" });
    expect(state.phase).toBe("unknown");
  });

  it("does not rewrite a definitive terminal state on a later disconnect", () => {
    let succeeded = createLifecycleOperation("start", "api");
    succeeded = reduceLifecycleOperation(succeeded, { type: "submit" });
    succeeded = reduceLifecycleOperation(succeeded, {
      type: "accepted",
      operationId: "op_s",
    });
    succeeded = reduceLifecycleOperation(succeeded, {
      type: "operation",
      status: "succeeded",
    });
    expect(
      reduceLifecycleOperation(succeeded, { type: "disconnect" }).phase,
    ).toBe("succeeded");

    let failed = createLifecycleOperation("start", "api");
    failed = reduceLifecycleOperation(failed, { type: "submit" });
    failed = reduceLifecycleOperation(failed, {
      type: "submit-failed",
      errorCode: "invalid_state",
      errorMessage: "stopping",
    });
    expect(reduceLifecycleOperation(failed, { type: "disconnect" }).phase).toBe(
      "failed",
    );
  });

  it("keeps unknown when an operation query itself fails", () => {
    let state = createLifecycleOperation("stop", "api");
    state = reduceLifecycleOperation(state, { type: "submit" });
    state = reduceLifecycleOperation(state, {
      type: "accepted",
      operationId: "op_1",
    });
    state = reduceLifecycleOperation(state, { type: "disconnect" });
    state = reduceLifecycleOperation(state, { type: "query-failed" });

    expect(state.phase).toBe("unknown");
    expect(canQueryOperation(state)).toBe(true);
  });

  it("offers no automatic retry from accepted, running, succeeded or unknown", () => {
    const phases = ["accepted", "running", "succeeded", "unknown"] as const;

    for (const phase of phases) {
      let state = createLifecycleOperation("stop", "api");
      state = reduceLifecycleOperation(state, { type: "submit" });
      if (phase !== "unknown") {
        state = reduceLifecycleOperation(state, {
          type: "accepted",
          operationId: "op_1",
        });
      }
      if (phase === "running") {
        state = reduceLifecycleOperation(state, {
          type: "operation",
          status: "running",
        });
      }
      if (phase === "succeeded") {
        state = reduceLifecycleOperation(state, {
          type: "operation",
          status: "succeeded",
        });
      }
      if (phase === "unknown") {
        state = reduceLifecycleOperation(state, { type: "disconnect" });
      }
      expect(state.phase).toBe(phase);

      const attempted = reduceLifecycleOperation(state, { type: "submit" });
      expect(attempted.phase, phase).toBe(phase);
    }
  });

  it("only queries an operation when an operation ID is known and not terminal", () => {
    let pending = createLifecycleOperation("stop", "api");
    expect(canQueryOperation(pending)).toBe(false);

    pending = reduceLifecycleOperation(pending, { type: "submit" });
    pending = reduceLifecycleOperation(pending, {
      type: "accepted",
      operationId: "op_1",
    });
    expect(canQueryOperation(pending)).toBe(true);

    pending = reduceLifecycleOperation(pending, {
      type: "operation",
      status: "failed",
      errorCode: "timeout",
      errorMessage: "timed out",
    });
    expect(canQueryOperation(pending)).toBe(false);
  });
});
