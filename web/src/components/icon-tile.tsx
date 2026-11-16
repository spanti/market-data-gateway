import type { LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

export interface IconTileProps {
  icon: LucideIcon;
  className?: string;
}

/**
 * Soft square icon holder used at the start of Variant D card headings.
 * The icon is decorative; the adjacent title carries the accessible name.
 */
export function IconTile({ icon: Icon, className }: IconTileProps) {
  return (
    <span
      className={cn(
        "flex size-8 shrink-0 items-center justify-center rounded-lg bg-accent-soft text-accent-strong",
        className,
      )}
    >
      <Icon className="size-4" aria-hidden="true" />
    </span>
  );
}
