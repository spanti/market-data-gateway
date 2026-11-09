import {
  AlertTriangle,
  CheckCircle2,
  CircleHelp,
  Info,
} from "lucide-react";
import type * as React from "react";
import { cn } from "@/lib/utils";

export type StatusTone =
  | "good"
  | "warning"
  | "serious"
  | "critical"
  | "unknown"
  | "info";

const TONE_CLASS: Record<StatusTone, string> = {
  good: "text-status-good",
  warning: "text-status-warning",
  serious: "text-status-serious",
  critical: "text-status-critical",
  unknown: "text-faint-foreground",
  info: "text-faint-foreground",
};

function ToneIcon({ tone }: { tone: StatusTone }) {
  const Icon =
    tone === "good"
      ? CheckCircle2
      : tone === "info"
        ? Info
        : tone === "unknown"
          ? CircleHelp
          : AlertTriangle;
  return <Icon className="size-4 shrink-0" aria-hidden="true" />;
}

export interface StatusBadgeProps extends React.ComponentProps<"span"> {
  tone: StatusTone;
  /**
   * Set only for values that update live. Static labels must not announce
   * repeatedly.
   */
  live?: boolean;
}

/**
 * Status is always icon + text. Color is redundant, never the only encoding.
 */
export function StatusBadge({
  tone,
  live = false,
  className,
  children,
  ...props
}: StatusBadgeProps) {
  return (
    <span
      data-slot="status-badge"
      data-tone={tone}
      role={live ? "status" : undefined}
      aria-live={live ? "polite" : undefined}
      className={cn(
        "inline-flex items-center gap-1.5 whitespace-nowrap text-xs font-medium",
        TONE_CLASS[tone],
        className,
      )}
      {...props}
    >
      <ToneIcon tone={tone} />
      {children}
    </span>
  );
}
