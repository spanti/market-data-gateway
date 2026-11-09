import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it } from "vitest";
import { apiClient } from "@/api/http/client";
import { server } from "@/test/server";
import {
  clearCsrfCredential,
  getCsrfCredential,
  setCsrfCredential,
} from "@/runtime/csrf";
import {
  resetWriteGate,
  stopWrites,
  writesAreStopped,
} from "@/runtime/write-gate";

beforeEach(() => {
  clearCsrfCredential();
  resetWriteGate();
  window.localStorage.clear();
  window.sessionStorage.clear();
});

describe("CSRF credential", () => {
  it("is held only in memory and is never written to web storage", () => {
    setCsrfCredential({ header_name: "X-CSRF-Token", token: "secret-token" });

    expect(getCsrfCredential()?.token).toBe("secret-token");
    expect(window.localStorage.length).toBe(0);
    expect(window.sessionStorage.length).toBe(0);

    clearCsrfCredential();
    expect(getCsrfCredential()).toBeNull();
  });

  it("attaches the CSRF header to unsafe methods only", async () => {
    setCsrfCredential({ header_name: "X-CSRF-Token", token: "secret-token" });
    const seen: Array<{ method: string; token: string | null }> = [];
    server.use(
      http.all("*/api/v1/admin/probe", ({ request }) => {
        seen.push({
          method: request.method,
          token: request.headers.get("X-CSRF-Token"),
        });
        return HttpResponse.json({ ok: true });
      }),
    );

    await apiClient.get("/api/v1/admin/probe");
    await apiClient.post("/api/v1/admin/probe", {});
    await apiClient.put("/api/v1/admin/probe", {});
    await apiClient.patch("/api/v1/admin/probe", {});
    await apiClient.delete("/api/v1/admin/probe");

    expect(seen).toEqual([
      { method: "GET", token: null },
      { method: "POST", token: "secret-token" },
      { method: "PUT", token: "secret-token" },
      { method: "PATCH", token: "secret-token" },
      { method: "DELETE", token: "secret-token" },
    ]);
  });
});

describe("write stop after 401", () => {
  it("rejects unsafe requests locally after a 401 without reaching the network", async () => {
    let networkCalls = 0;
    server.use(
      http.post("*/api/v1/admin/probe", () => {
        networkCalls += 1;
        return HttpResponse.json({ ok: true });
      }),
    );

    stopWrites();
    expect(writesAreStopped()).toBe(true);

    await expect(apiClient.post("/api/v1/admin/probe", {})).rejects.toMatchObject(
      { code: "writes_stopped" },
    );
    expect(networkCalls).toBe(0);
  });

  it("still allows reads while writes are stopped", async () => {
    stopWrites();
    server.use(
      http.get("*/api/v1/admin/probe", () =>
        HttpResponse.json({ ok: true }),
      ),
    );

    await expect(apiClient.get("/api/v1/admin/probe")).resolves.toMatchObject({
      status: 200,
    });
  });
});
