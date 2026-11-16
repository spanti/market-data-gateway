import type * as React from "react";
import { cn } from "@/lib/utils";

export interface PageHeaderProps {
  title: string;
  /** Optional and quiet. Prefer `?` help for supplementary explanations. */
  description?: React.ReactNode;
  /** Status summary shown directly under the title. */
  status?: React.ReactNode;
  actions?: React.ReactNode;
  /** Optional `?` help trigger next to the title. */
  help?: React.ReactNode;
  className?: string;
}

/**
 * Variant D page header: a concise title, an optional status summary and the
 * primary action group. No continuous rules, numbered headings or stacked
 * explanatory meta.
 */
export function PageHeader({
  title,
  description,
  status,
  actions,
  help,
  className,
}: PageHeaderProps) {
  return (
    <div
      role="group"
      aria-label={title}
      data-slot="page-header"
      className={cn(
        "flex flex-wrap items-start justify-between gap-4",
        className,
      )}
    >
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-[28px] font-semibold tracking-tight text-foreground">
            {title}
          </h1>
          {help}
        </div>
        {status ? (
          <div className="mt-2.5 flex flex-wrap items-center gap-2">{status}</div>
        ) : null}
        {description ? (
          <p className="mt-2 max-w-2xl text-xs leading-relaxed text-muted-foreground">
            {description}
          </p>
        ) : null}
      </div>
      {actions ? (
        <div className="flex flex-col items-end gap-2 text-right">{actions}</div>
      ) : null}
    </div>
  );
}
