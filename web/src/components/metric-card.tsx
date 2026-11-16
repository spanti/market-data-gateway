import type { LucideIcon } from "lucide-react";
import type * as React from "react";
import { HelpTrigger, type HelpTriggerSide } from "@/components/help-trigger";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { IconTile } from "@/components/icon-tile";

export interface MetricHelp {
  /** Accessible name for the `?` trigger. */
  label: string;
  /** Supplementary definition. Required risk or error copy stays inline. */
  content: React.ReactNode;
  side?: HelpTriggerSide;
}

export interface MetricCardProps {
  label: string;
  /** null renders an explicit unknown, never 0 or a previous value. */
  value: string | null;
  unit?: string;
  help?: MetricHelp;
  /** Why the value is unknown. Shown in place next to the unknown value. */
  unknownReason?: React.ReactNode;
  /** Supporting in-place facts such as P50/P95 or sample time. */
  detail?: React.ReactNode;
  /** Time evidence, normally the last successful update. */
  evidence?: React.ReactNode;
  /** Optional visual slot such as a sparkline; never replaces the value. */
  visual?: React.ReactNode;
  icon?: LucideIcon;
  className?: string;
}

/**
 * One Variant D KPI card. The label, value, unit and time evidence are always
 * visible; help only carries supplementary definitions.
 */
export function MetricCard({
  label,
  value,
  unit,
  help,
  unknownReason,
  detail,
  evidence,
  visual,
  icon: Icon,
  className,
}: MetricCardProps) {
  return (
    <Card
      data-slot="metric-card"
      role="group"
      data-metric-label={label}
      aria-label={label}
      className={cn("min-h-[164px] p-5", className)}
    >
      <CardHeader className="p-0">
        <div className="flex min-w-0 items-center gap-1.5">
          <CardTitle className="text-[13px] font-medium text-muted-foreground">
            {label}
          </CardTitle>
          {help ? (
            <HelpTrigger
              label={help.label}
              content={help.content}
              side={help.side ?? "bottom"}
            />
          ) : null}
        </div>
        {Icon ? (
          <IconTile icon={Icon} />
        ) : null}
      </CardHeader>
      <CardContent className="flex flex-1 flex-col p-0 pt-3">
        <div className="flex min-w-0 flex-wrap items-baseline gap-x-1.5">
          {value === null ? (
            <>
              <span className="text-[28px] font-semibold leading-none tracking-tight text-faint-foreground">
                未知
              </span>
              {unknownReason ? (
                <CardDescription className="mt-0 text-xs text-faint-foreground">
                  {unknownReason}
                </CardDescription>
              ) : null}
            </>
          ) : (
            <>
              <span className="text-[30px] font-semibold leading-none tracking-tight tabular-nums text-foreground">
                {value}
              </span>
              {unit ? (
                <span className="text-xs text-faint-foreground">{unit}</span>
              ) : null}
            </>
          )}
        </div>
        {visual ? <div className="mt-3 min-w-0">{visual}</div> : null}
        {detail ? (
          <div className="mt-2 text-[11px] leading-relaxed text-muted-foreground">
            {detail}
          </div>
        ) : null}
        {evidence ? (
          <div className="mt-auto pt-3 text-[11px] leading-relaxed text-faint-foreground">
            {evidence}
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
