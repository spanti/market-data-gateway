/**
 * The only values the console may persist in localStorage: non-sensitive
 * theme and time-zone display preferences.
 *
 * Identity, OIDC tokens, session IDs, Access Roles, CSRF tokens, capability
 * documents, and job/operation contents must never be written here.
 */

export type ThemePreference = "light" | "dark" | "system";
export type TimezonePreference = "utc" | "local";

export interface UiPreferences {
  theme: ThemePreference;
  timezone: TimezonePreference;
}

const THEME_KEY = "mdgw.theme";
const TIMEZONE_KEY = "mdgw.timezone";

export const PREFERENCE_STORAGE_KEYS = [THEME_KEY, TIMEZONE_KEY] as const;

export const DEFAULT_PREFERENCES: UiPreferences = {
  theme: "light",
  timezone: "utc",
};

type ReadStorage = Pick<Storage, "getItem">;
type WriteStorage = Pick<Storage, "setItem">;

function safeGet(storage: ReadStorage, key: string): string | null {
  try {
    return storage.getItem(key);
  } catch {
    return null;
  }
}

function safeSet(storage: WriteStorage, key: string, value: string): void {
  try {
    storage.setItem(key, value);
  } catch {
    // Storage can be disabled by browser policy. Preferences are optional.
  }
}

function isTheme(value: string | null): value is ThemePreference {
  return value === "light" || value === "dark" || value === "system";
}

function isTimezone(value: string | null): value is TimezonePreference {
  return value === "utc" || value === "local";
}

export function loadPreferences(
  storage: ReadStorage = window.localStorage,
): UiPreferences {
  const theme = safeGet(storage, THEME_KEY);
  const timezone = safeGet(storage, TIMEZONE_KEY);
  return {
    theme: isTheme(theme) ? theme : DEFAULT_PREFERENCES.theme,
    timezone: isTimezone(timezone) ? timezone : DEFAULT_PREFERENCES.timezone,
  };
}

/**
 * Persists exactly the two whitelisted keys. Extra properties on the input
 * object (even if a caller casts them in) are never serialized.
 */
export function savePreferences(
  preferences: UiPreferences,
  storage: WriteStorage = window.localStorage,
): void {
  const theme = isTheme(preferences.theme)
    ? preferences.theme
    : DEFAULT_PREFERENCES.theme;
  const timezone = isTimezone(preferences.timezone)
    ? preferences.timezone
    : DEFAULT_PREFERENCES.timezone;

  safeSet(storage, THEME_KEY, theme);
  safeSet(storage, TIMEZONE_KEY, timezone);
}

export interface ResolveThemeOptions {
  prefersDark?: boolean;
}

export function resolveTheme(
  theme: ThemePreference,
  options: ResolveThemeOptions = {},
): "light" | "dark" {
  if (theme === "light" || theme === "dark") {
    return theme;
  }
  if (options.prefersDark !== undefined) {
    return options.prefersDark ? "dark" : "light";
  }
  if (typeof window.matchMedia === "function") {
    return window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }
  return "light";
}

export interface ApplyThemeOptions extends ResolveThemeOptions {
  root?: HTMLElement;
}

export function applyTheme(
  theme: ThemePreference,
  options: ApplyThemeOptions = {},
): void {
  const root = options.root ?? document.documentElement;
  const resolved = resolveTheme(theme, options);
  root.classList.toggle("dark", resolved === "dark");
  root.dataset.theme = resolved;
}
