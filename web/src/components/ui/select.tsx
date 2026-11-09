import type * as React from "react";
import { cn } from "@/lib/utils";

/**
 * Native select for filter rows. Radix Select is available for richer
 * pickers; filters need native keyboard behavior and URL-friendly values.
 */
function Select({ className, ...props }: React.ComponentProps<"select">) {
  return (
    <select
      data-slot="select"
      className={cn(
        "flex h-8 w-full rounded-md border border-input bg-card px-2 py-1 text-xs text-foreground disabled:cursor-not-allowed disabled:opacity-50",
        className,
      )}
      {...props}
    />
  );
}

export { Select };
