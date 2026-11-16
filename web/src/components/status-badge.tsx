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
  good: "bg-status-good-bg text-status-good-fg",
  warning: "bg-status-warning-bg text-status-warning-fg",
  serious: "bg-status-serious-bg text-status-serious-fg",
  critical: "bg-status-critical-bg text-status-critical-fg",
  unknown: "bg-status-unknown-bg text-status-unknown-fg",
  info: "bg-status-info-bg text-status-info-fg",
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
  return <Icon className="size-3.5 shrink-0" aria-hidden="true" />;
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
 * Variant D status pill. Status is always icon + text + soft color; the color
 * is redundant, never the only encoding.
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
        "inline-flex max-w-full items-center gap-1.5 rounded-full px-2.5 py-1 text-[11px] font-medium leading-none",
        TONE_CLASS[tone],
        className,
      )}
      {...props}
    >
      <ToneIcon tone={tone} />
      <span className="min-w-0 break-words">{children}</span>
    </span>
  );
}
