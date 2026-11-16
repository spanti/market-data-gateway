import type { LucideIcon } from "lucide-react";
import type * as React from "react";
import { HelpTrigger } from "@/components/help-trigger";
import { IconTile } from "@/components/icon-tile";
import { CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

export interface CardHeadingProps {
  icon: LucideIcon;
  title: string;
  titleId?: string;
  helpLabel: string;
  helpContent: React.ReactNode;
  aside?: React.ReactNode;
  headerClassName?: string;
  className?: string;
}

/**
 * Shared Variant D card heading: icon tile, short title, `?` help and an
 * optional status/action cluster. Supplementary definitions live in help;
 * permissions, errors and operation results stay inline in the card body.
 */
export function CardHeading({
  icon,
  title,
  titleId,
  helpLabel,
  helpContent,
  aside,
  headerClassName,
  className,
}: CardHeadingProps) {
  return (
    <CardHeader className={headerClassName}>
      <div
        className={cn(
          "flex min-w-0 flex-wrap items-center gap-2",
          className,
        )}
      >
        <IconTile icon={icon} />
        <CardTitle id={titleId}>{title}</CardTitle>
        <HelpTrigger label={helpLabel} content={helpContent} side="bottom" />
      </div>
      {aside ? (
        <div className="flex flex-wrap items-center gap-2">{aside}</div>
      ) : null}
    </CardHeader>
  );
}
