import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterAll, afterEach, beforeAll } from "vitest";
import { resetCapabilityCache } from "@/api/capabilities";
import { clearCsrfCredential } from "@/runtime/csrf";
import { resetRetryAfter } from "@/runtime/retry-after";
import { resetWriteGate } from "@/runtime/write-gate";
import { server } from "./server";

// jsdom has no PointerEvent, so fireEvent.pointerDown would lose
// pointerType. HelpTrigger distinguishes keyboard, mouse and touch gestures.
if (typeof window !== "undefined" && typeof window.PointerEvent === "undefined") {
  class PointerEventStub extends MouseEvent {
    readonly pointerType: string;
    readonly pointerId: number;
    constructor(type: string, init: PointerEventInit = {}) {
      super(type, init);
      this.pointerType = init.pointerType ?? "";
      this.pointerId = init.pointerId ?? 1;
    }
  }
  window.PointerEvent = PointerEventStub as unknown as typeof PointerEvent;
}

beforeAll(() => {
  server.listen({ onUnhandledRequest: "error" });
});

afterEach(() => {
  cleanup();
  server.resetHandlers();
  resetCapabilityCache();
  clearCsrfCredential();
  resetRetryAfter();
  resetWriteGate();
  window.localStorage.clear();
  window.sessionStorage.clear();
});

afterAll(() => {
  server.close();
});
