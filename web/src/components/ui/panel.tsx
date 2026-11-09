import type * as React from "react";
import { cn } from "@/lib/utils";

/**
 * Flat, square panel: Variant B uses borders, never shadows or card radii.
 */
function Panel({ className, ...props }: React.ComponentProps<"section">) {
  return (
    <section
      data-slot="panel"
      className={cn("border border-border bg-card", className)}
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
      className={cn("text-sm font-semibold text-foreground", className)}
      {...props}
    />
  );
}

function PanelDescription({ className, ...props }: React.ComponentProps<"p">) {
  return (
    <p
      data-slot="panel-description"
      className={cn("mt-1 text-xs text-muted-foreground", className)}
      {...props}
    />
  );
}

export { Panel, PanelDescription, PanelHeader, PanelTitle };
