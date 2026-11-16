import type * as React from "react";
import { cn } from "@/lib/utils";

/**
 * Shared Variant D business panel. The API is unchanged for modules that have
 * not migrated yet; only the surface treatment moved to the D card language.
 */
function Panel({ className, ...props }: React.ComponentProps<"section">) {
  return (
    <section
      data-slot="panel"
      className={cn(
        "min-w-0 rounded-xl border border-card-border bg-card text-card-foreground shadow-card",
        className,
      )}
      {...props}
    />
  );
}

function PanelHeader({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="panel-header"
      className={cn("flex flex-wrap items-start justify-between gap-3", className)}
      {...props}
    />
  );
}

function PanelTitle({ className, ...props }: React.ComponentProps<"h2">) {
  return (
    <h2
      data-slot="panel-title"
      className={cn("text-sm font-semibold tracking-tight text-foreground", className)}
      {...props}
    />
  );
}

function PanelDescription({ className, ...props }: React.ComponentProps<"p">) {
  return (
    <p
      data-slot="panel-description"
      className={cn("mt-1 text-xs leading-relaxed text-muted-foreground", className)}
      {...props}
    />
  );
}

export { Panel, PanelDescription, PanelHeader, PanelTitle };
