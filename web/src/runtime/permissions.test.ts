import { describe, expect, it } from "vitest";
import {
  ALL_ACTIONS,
  capabilityDisabledReason,
  can,
  disabledReasonFor,
  type AccessRole,
  type Action,
  type PermissionSubject,
} from "@/runtime/permissions";

const superAdmin: PermissionSubject = { kind: "role", role: "SuperAdmin" };
const operator: PermissionSubject = { kind: "role", role: "Operator" };
const auditor: PermissionSubject = { kind: "role", role: "Auditor" };
const anonymous: PermissionSubject = { kind: "anonymous" };

function allowed(subject: PermissionSubject): Action[] {
  return ALL_ACTIONS.filter((action) => can(subject, action));
}

const readOnly: Action[] = [
  "monitoring.view",
  "bars.view",
  "quality.view",
  "export.csv.direct",
  "export.job.view.own",
  "feed.subscription.view",
  "flow.control.view",
  "backfill.job.view",
  "service.instance.view",
  "audit.view",
  "monitoring.proxy.query",
];

const operatorActions: Action[] = [
  ...readOnly,
  "export.job.create",
  "feed.subscription.write",
  "flow.control.write",
  "backfill.job.create",
  "backfill.job.cancel",
  "ws.session.subscribe",
  "ws.debug.freeform",
];

describe("Access Role matrix", () => {
  it("SuperAdmin has every action, including lifecycle and user management", () => {
    expect(allowed(superAdmin)).toEqual(
      expect.arrayContaining([
        ...operatorActions,
        "export.job.view.all",
        "service.lifecycle.start",
        "service.lifecycle.stop",
        "service.lifecycle.reload",
        "user.role.manage",
      ]),
    );
    expect(can(superAdmin, "export.job.view.all")).toBe(true);
  });

  it("Operator has read, export, feed, flow, backfill and WS debug but no lifecycle", () => {
    const actions = allowed(operator);

    expect(actions).toEqual(expect.arrayContaining(operatorActions));
    expect(actions).toContain("export.job.create");
    expect(actions).toContain("ws.debug.freeform");
    expect(actions).not.toContain("export.job.view.all");
    expect(actions).not.toContain("service.lifecycle.start");
    expect(actions).not.toContain("service.lifecycle.stop");
    expect(actions).not.toContain("service.lifecycle.reload");
    expect(actions).not.toContain("user.role.manage");
  });

  it("Auditor can read, export and use the controlled WS topic form only", () => {
    const actions = allowed(auditor);

    expect(actions).toEqual(
      expect.arrayContaining([...readOnly, "export.job.create"]),
    );
    expect(actions).toContain("ws.session.subscribe");
    expect(actions).not.toContain("ws.debug.freeform");
    expect(actions).not.toContain("feed.subscription.write");
    expect(actions).not.toContain("flow.control.write");
    expect(actions).not.toContain("backfill.job.create");
    expect(actions).not.toContain("backfill.job.cancel");
    expect(actions).not.toContain("service.lifecycle.start");
    expect(actions).not.toContain("export.job.view.all");
  });

  it("Current Contract anonymous can read real data and use controlled WS only", () => {
    const actions = allowed(anonymous);

    expect(actions).toEqual(
      expect.arrayContaining([
        "monitoring.view",
        "bars.view",
        "quality.view",
        "export.csv.direct",
        "ws.session.subscribe",
        "service.instance.view",
      ]),
    );
    for (const forbidden of [
      "export.job.create",
      "feed.subscription.write",
      "flow.control.write",
      "backfill.job.create",
      "ws.debug.freeform",
      "service.lifecycle.start",
      "service.lifecycle.stop",
      "service.lifecycle.reload",
      "audit.view",
      "user.role.manage",
    ] satisfies Action[]) {
      expect(can(anonymous, forbidden)).toBe(false);
    }
  });

  it("only SuperAdmin may start, stop or reload a Service Instance", () => {
    for (const action of [
      "service.lifecycle.start",
      "service.lifecycle.stop",
      "service.lifecycle.reload",
    ] satisfies Action[]) {
      expect(can(superAdmin, action)).toBe(true);
      expect(can(operator, action)).toBe(false);
      expect(can(auditor, action)).toBe(false);
      expect(can(anonymous, action)).toBe(false);
    }
  });

  it("export visibility is all for SuperAdmin and own-only for the others", () => {
    expect(can(superAdmin, "export.job.view.all")).toBe(true);
    for (const subject of [operator, auditor]) {
      expect(can(subject, "export.job.view.own")).toBe(true);
      expect(can(subject, "export.job.view.all")).toBe(false);
    }
    expect(can(anonymous, "export.job.view.own")).toBe(false);
  });
});

describe("disabled reasons", () => {
  it("names the missing Admin API in Current Contract mode", () => {
    expect(disabledReasonFor(anonymous, "feed.subscription.write")).toEqual({
      code: "requires-admin-api",
      message: "需要 Admin API",
    });
  });

  it("names the required role for an authenticated denial", () => {
    expect(disabledReasonFor(auditor, "feed.subscription.write")).toMatchObject(
      {
        code: "requires-role",
        message: expect.stringContaining("Operator"),
      },
    );
    expect(disabledReasonFor(operator, "service.lifecycle.stop")).toMatchObject(
      {
        code: "requires-role",
        message: expect.stringContaining("SuperAdmin"),
      },
    );
  });

  it("returns no disabled reason when the action is allowed", () => {
    expect(disabledReasonFor(superAdmin, "service.lifecycle.stop")).toBeNull();
    expect(disabledReasonFor(anonymous, "monitoring.view")).toBeNull();
  });

  it("maps capability availability to truthful disabled reasons", () => {
    expect(capabilityDisabledReason({ state: "enabled" })).toBeNull();
    expect(capabilityDisabledReason({ state: "unsupported" })).toMatchObject({
      code: "backend-unsupported",
      message: "后端未实现",
    });
    expect(
      capabilityDisabledReason({ state: "adapter_unavailable" }),
    ).toMatchObject({
      code: "adapter-unavailable",
      message: "适配器不可用",
    });
  });
});

describe("role typings", () => {
  it("keeps exactly the three canonical Access Roles", () => {
    const roles: AccessRole[] = ["SuperAdmin", "Operator", "Auditor"];
    expect(new Set(roles).size).toBe(3);
  });
});
