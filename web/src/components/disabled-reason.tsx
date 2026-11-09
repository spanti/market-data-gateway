import { LockKeyhole } from "lucide-react";
import { useId } from "react";
import { Button } from "@/components/ui/button";
import type { DisabledReason as DisabledReasonValue } from "@/runtime/permissions";
import { cn } from "@/lib/utils";

export interface DisabledReasonProps {
  reason: DisabledReasonValue | null;
  id?: string;
  className?: string;
}

/**
 * Visible explanation attached to a disabled control. A disabled control
 * without a reason is a bug, not a permission system.
 */
export function DisabledReason({
  reason,
  id,
  className,
}: DisabledReasonProps) {
  if (!reason) {
    return null;
  }
  return (
    <span
      id={id}
      data-slot="disabled-reason"
      className={cn(
        "inline-flex items-center gap-1 text-xs text-faint-foreground",
        className,
      )}
    >
      <LockKeyhole className="size-3.5" aria-hidden="true" />
      {reason.message}
    </span>
  );
}

export interface DisabledActionProps {
  label: string;
  reason: DisabledReasonValue | null;
  className?: string;
}

/**
 * A visible, disabled control with its reason linked through
 * `aria-describedby`. Used wherever a real action cannot be offered.
 */
export function DisabledAction({ label, reason, className }: DisabledActionProps) {
  const reasonId = useId();
  return (
    <span className={cn("inline-flex flex-wrap items-center gap-2", className)}>
      <Button
        variant="outline"
        size="sm"
        disabled
        aria-describedby={reason ? reasonId : undefined}
      >
        {label}
      </Button>
      <DisabledReason id={reasonId} reason={reason} />
    </span>
  );
}
