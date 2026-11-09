import type * as React from "react";
import { cn } from "@/lib/utils";

export interface KpiCellProps {
  label: string;
  qualifier?: string;
  value: string | null;
  unit?: string;
  detail?: React.ReactNode;
  unknownReason?: string;
  asOf?: string;
  className?: string;
}

/**
 * One cell of the border-divided KPI band. A null value renders an explicit
 * unknown with a reason, never 0.
 */
export function KpiCell({
  label,
  qualifier,
  value,
  unit,
  detail,
  unknownReason,
  asOf,
  className,
}: KpiCellProps) {
  return (
    <div
      data-slot="kpi-cell"
      data-kpi-label={label}
      className={cn("p-4", className)}
    >
      <dt className="text-xs text-muted-foreground">
        {label}
        {qualifier ? (
          <span className="ml-1 text-faint-foreground">{qualifier}</span>
        ) : null}
      </dt>
      <dd className="mt-2">
        {value === null ? (
          <>
            <span className="text-xl font-semibold text-faint-foreground">
              未知
            </span>
            {unknownReason ? (
              <span className="ml-2 text-xs text-faint-foreground">
                {unknownReason}
              </span>
            ) : null}
          </>
        ) : (
          <span className="inline-flex items-baseline gap-1">
            <span className="text-3xl font-semibold tabular-nums">{value}</span>
            {unit ? (
              <span className="text-sm font-normal text-faint-foreground">
                {unit}
              </span>
            ) : null}
          </span>
        )}
      </dd>
      {detail ? (
        <div className="mt-1 text-xs text-faint-foreground">{detail}</div>
      ) : null}
      {asOf ? (
        <div className="mt-1 text-[11px] text-faint-foreground">{asOf}</div>
      ) : null}
    </div>
  );
}
