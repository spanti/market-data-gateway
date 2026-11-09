import { useRef } from "react";
import { MONITORING_RANGES } from "@/lib/monitoring-config";
import { cn } from "@/lib/utils";

export interface RangeControlProps {
  value: string;
  onChange: (id: string) => void;
}

/**
 * One shared time range for every chart, KPI and table. Implemented as a
 * radio group with roving focus so arrow keys never leave the control.
 */
export function RangeControl({ value, onChange }: RangeControlProps) {
  const groupRef = useRef<HTMLDivElement | null>(null);

  const move = (delta: number) => {
    const index = MONITORING_RANGES.findIndex((item) => item.id === value);
    const current = index < 0 ? 0 : index;
    const nextIndex =
      (current + delta + MONITORING_RANGES.length) %
      MONITORING_RANGES.length;
    const next = MONITORING_RANGES[nextIndex];
    if (!next) {
      return;
    }
    onChange(next.id);
    requestAnimationFrame(() => {
      const radios =
        groupRef.current?.querySelectorAll<HTMLButtonElement>('[role="radio"]');
      radios?.[nextIndex]?.focus();
    });
  };

  return (
    <div
      ref={groupRef}
      role="radiogroup"
      aria-label="统一监控时间范围"
      className="inline-flex items-center rounded-md border border-border bg-card p-1"
      onKeyDown={(event) => {
        if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
          event.preventDefault();
          move(-1);
        } else if (event.key === "ArrowRight" || event.key === "ArrowDown") {
          event.preventDefault();
          move(1);
        } else if (event.key === "Home") {
          event.preventDefault();
          const first = MONITORING_RANGES[0];
          if (first) {
            onChange(first.id);
          }
        } else if (event.key === "End") {
          event.preventDefault();
          const last = MONITORING_RANGES[MONITORING_RANGES.length - 1];
          if (last) {
            onChange(last.id);
          }
        }
      }}
    >
      {MONITORING_RANGES.map((range) => {
        const checked = range.id === value;
        return (
          <button
            key={range.id}
            type="button"
            role="radio"
            aria-checked={checked}
            tabIndex={checked ? 0 : -1}
            onClick={() => onChange(range.id)}
            className={cn(
              "h-7 min-w-12 rounded-sm px-2.5 text-xs text-muted-foreground",
              "hover:bg-muted hover:text-foreground",
              checked && "bg-muted font-medium text-foreground",
            )}
          >
            {range.label}
          </button>
        );
      })}
    </div>
  );
}
