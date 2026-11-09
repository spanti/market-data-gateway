import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterAll, afterEach, beforeAll } from "vitest";
import { resetCapabilityCache } from "@/api/capabilities";
import { clearCsrfCredential } from "@/runtime/csrf";
import { resetRetryAfter } from "@/runtime/retry-after";
import { resetWriteGate } from "@/runtime/write-gate";
import { server } from "./server";

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
