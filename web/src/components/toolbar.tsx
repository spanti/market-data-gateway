import type * as React from "react";
import { cn } from "@/lib/utils";

export interface FilterRowProps extends React.ComponentProps<"div"> {
  note?: React.ReactNode;
}

/**
 * The single shared filter band: one time-range row feeds every chart, KPI
 * and table on the page.
 */
export function FilterRow({ className, children, note, ...props }: FilterRowProps) {
  return (
    <div
      data-slot="filter-row"
      className={cn(
        "mt-5 flex flex-wrap items-center justify-between gap-3 border-y border-border bg-card px-3 py-2",
        className,
      )}
      {...props}
    >
      <div className="flex flex-wrap items-center gap-3">{children}</div>
      {note ? (
        <span className="text-xs text-faint-foreground">{note}</span>
      ) : null}
    </div>
  );
}

export interface ToolbarProps extends React.ComponentProps<"div"> {
  actions?: React.ReactNode;
}

export function Toolbar({ className, children, actions, ...props }: ToolbarProps) {
  return (
    <div
      data-slot="toolbar"
      className={cn(
        "flex flex-wrap items-center justify-between gap-3",
        className,
      )}
      {...props}
    >
      <div className="flex flex-wrap items-center gap-2">{children}</div>
      {actions ? (
        <div className="flex flex-wrap items-center gap-2">{actions}</div>
      ) : null}
    </div>
  );
}
