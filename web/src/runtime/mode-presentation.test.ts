import { describe, expect, it } from "vitest";
import type { CapabilityState } from "@/api/capabilities";
import {
  capabilityModeKind,
  capabilityModePresentation,
} from "@/runtime/mode-presentation";

const currentContract: CapabilityState = {
  mode: "current-contract",
  lastSuccessAt: null,
};

const adminApi: CapabilityState = {
  mode: "admin-api",
  document: null,
  compatible: null,
  lastSuccessAt: null,
};

const unknown: CapabilityState = {
  mode: "capabilities-unknown",
  lastSuccessAt: null,
  errorCode: "timeout",
  message: "discovery timed out",
};

describe("capability mode presentation", () => {
  it("names the discovering state before capability resolution", () => {
    expect(capabilityModeKind(null)).toBe("discovering");
    expect(capabilityModePresentation(null)).toEqual({
      tone: "unknown",
      label: "发现能力中",
      detail: "能力发现完成前不启用管理写。",
    });
  });

  it("maps each resolved runtime mode to one canonical badge", () => {
    expect(capabilityModePresentation(currentContract)).toMatchObject({
      tone: "unknown",
      label: "Current Contract",
    });
    expect(capabilityModePresentation(adminApi)).toMatchObject({
      tone: "info",
      label: "Admin API",
    });
    expect(capabilityModePresentation(unknown)).toMatchObject({
      tone: "warning",
      label: "能力未知",
    });
  });

  it("allows a page-specific label without re-branching on the mode", () => {
    const presentation = capabilityModePresentation(currentContract, {
      "current-contract": { label: "Current Contract 匿名只读" },
    });

    expect(presentation.label).toBe("Current Contract 匿名只读");
    expect(presentation.tone).toBe("unknown");
    expect(capabilityModePresentation(unknown).label).toBe("能力未知");
  });
});
