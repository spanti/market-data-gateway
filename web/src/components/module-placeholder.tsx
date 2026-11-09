import { Construction } from "lucide-react";
import type * as React from "react";
import { EmptyState } from "@/components/states";
import { Panel } from "@/components/ui/panel";
import { cn } from "@/lib/utils";

export interface ModulePlaceholderProps {
  title: string;
  description: React.ReactNode;
  children?: React.ReactNode;
  className?: string;
}

/**
 * Explicit "not implemented yet" body. It contains no mock metrics and no
 * simulated success; later tickets replace it with real queries.
 */
export function ModulePlaceholder({
  title,
  description,
  children,
  className,
}: ModulePlaceholderProps) {
  return (
    <Panel className={cn("mt-6 p-5", className)} aria-label={title}>
      <EmptyState
        title={title}
        description={description}
        icon={<Construction className="size-5" />}
      />
      {children}
    </Panel>
  );
}
