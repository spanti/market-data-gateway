import { describe, expect, it } from "vitest";
import {
  applyTheme,
  DEFAULT_PREFERENCES,
  loadPreferences,
  PREFERENCE_STORAGE_KEYS,
  resolveTheme,
  savePreferences,
  type UiPreferences,
} from "@/lib/preferences";

function createMemoryStorage(): Pick<Storage, "getItem" | "setItem"> & {
  entries: Map<string, string>;
} {
  const entries = new Map<string, string>();
  return {
    entries,
    getItem: (key) => entries.get(key) ?? null,
    setItem: (key, value) => {
      entries.set(key, value);
    },
  };
}

describe("non-sensitive UI preferences", () => {
  it("writes only the theme and time-zone keys", () => {
    const storage = createMemoryStorage();

    savePreferences({ theme: "dark", timezone: "local" }, storage);

    expect([...storage.entries.keys()].sort()).toEqual(
      [...PREFERENCE_STORAGE_KEYS].sort(),
    );
    expect(storage.entries.get("mdgw.theme")).toBe("dark");
    expect(storage.entries.get("mdgw.timezone")).toBe("local");
  });

  it("never writes identity, tokens, roles or a capability document", () => {
    const storage = createMemoryStorage();
    const unsafe = {
      theme: "light",
      timezone: "utc",
      accessToken: "secret-token",
      access_role: "SuperAdmin",
      capabilities: { state: "enabled" },
    } as unknown as UiPreferences;

    savePreferences(unsafe, storage);

    const persisted = [...storage.entries.entries()].flat().join(" ");
    expect(persisted).not.toContain("secret-token");
    expect(persisted).not.toContain("SuperAdmin");
    expect(persisted).not.toContain("capabilities");
    expect(storage.entries.size).toBe(2);
  });

  it("returns defaults when the storage is empty or invalid", () => {
    expect(loadPreferences(createMemoryStorage())).toEqual(DEFAULT_PREFERENCES);

    const storage = createMemoryStorage();
    storage.setItem("mdgw.theme", "neon");
    storage.setItem("mdgw.timezone", "mars");
    storage.setItem("mdgw.access_token", "should-be-ignored");

    expect(loadPreferences(storage)).toEqual(DEFAULT_PREFERENCES);
  });

  it("loads valid values and ignores extra storage entries", () => {
    const storage = createMemoryStorage();
    storage.setItem("mdgw.theme", "dark");
    storage.setItem("mdgw.timezone", "local");

    expect(loadPreferences(storage)).toEqual({
      theme: "dark",
      timezone: "local",
    });
  });

  it("declares only non-sensitive storage keys", () => {
    for (const key of PREFERENCE_STORAGE_KEYS) {
      expect(key.startsWith("mdgw.")).toBe(true);
      expect(key).not.toMatch(/token|session|role|identity|capabil/i);
    }
  });
});

describe("theme application", () => {
  it("resolves system preference without storing anything", () => {
    expect(resolveTheme("system", { prefersDark: true })).toBe("dark");
    expect(resolveTheme("system", { prefersDark: false })).toBe("light");
    expect(resolveTheme("light", { prefersDark: true })).toBe("light");
  });

  it("applies the resolved theme classes to the root element", () => {
    const root = document.createElement("div");

    applyTheme("dark", { root, prefersDark: false });
    expect(root.classList.contains("dark")).toBe(true);
    expect(root.dataset.theme).toBe("dark");

    applyTheme("light", { root, prefersDark: false });
    expect(root.classList.contains("dark")).toBe(false);
    expect(root.dataset.theme).toBe("light");
  });
});
