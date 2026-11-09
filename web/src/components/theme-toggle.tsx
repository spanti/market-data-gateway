import { Moon, Sun } from "lucide-react";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import {
  applyTheme,
  loadPreferences,
  resolveTheme,
  savePreferences,
  type ThemePreference,
} from "@/lib/preferences";

/**
 * Theme is a non-sensitive local preference. Nothing else is persisted.
 */
export function ThemeToggle() {
  const [theme, setTheme] = useState<ThemePreference>(
    () => loadPreferences().theme,
  );
  const resolved = resolveTheme(theme);
  const nextTheme: ThemePreference = resolved === "dark" ? "light" : "dark";

  return (
    <Button
      variant="ghost"
      size="icon"
      aria-label={`切换主题（当前${resolved === "dark" ? "深色" : "浅色"}）`}
      onClick={() => {
        setTheme(nextTheme);
        applyTheme(nextTheme);
        savePreferences({ ...loadPreferences(), theme: nextTheme });
      }}
    >
      {resolved === "dark" ? (
        <Sun aria-hidden="true" />
      ) : (
        <Moon aria-hidden="true" />
      )}
    </Button>
  );
}
