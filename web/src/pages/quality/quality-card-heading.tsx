import type { LucideIcon } from "lucide-react";
import type * as React from "react";
import { HelpTrigger } from "@/components/help-trigger";
import { CardHeader, CardTitle } from "@/components/ui/card";

export interface QualityCardHeadingProps {
  icon: LucideIcon;
  title: string;
  helpLabel: string;
  helpContent: React.ReactNode;
  aside?: React.ReactNode;
}

/**
 * Shared Variant D heading for the Quality cards: icon tile, short title and a
 * `?` trigger. Supplementary definitions live in help; permissions, errors and
 * results stay inline in the card body.
 */
export function QualityCardHeading({
  icon: Icon,
  title,
  helpLabel,
  helpContent,
  aside,
}: QualityCardHeadingProps) {
  return (
    <CardHeader>
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <span className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-accent-soft text-accent-strong">
          <Icon className="size-4" aria-hidden="true" />
        </span>
        <CardTitle>{title}</CardTitle>
        <HelpTrigger
          label={helpLabel}
          content={helpContent}
          side="bottom"
        />
      </div>
      {aside ? (
        <div className="flex flex-wrap items-center gap-2">{aside}</div>
      ) : null}
    </CardHeader>
  );
}
