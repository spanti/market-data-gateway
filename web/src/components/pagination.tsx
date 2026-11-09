import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { DisabledReason } from "@/runtime/permissions";
import { DisabledReason as DisabledReasonText } from "@/components/disabled-reason";

export interface PaginationProps {
  label?: string;
  hasMore: boolean;
  hasPrevious?: boolean;
  onNext?: () => void;
  onPrevious?: () => void;
  disabledReason?: DisabledReason | null;
}

export function Pagination({
  label,
  hasMore,
  hasPrevious = false,
  onNext,
  onPrevious,
  disabledReason = null,
}: PaginationProps) {
  return (
    <nav
      data-slot="pagination"
      aria-label="分页"
      className="flex flex-wrap items-center gap-2"
    >
      <Button
        variant="outline"
        size="sm"
        onClick={onPrevious}
        disabled={!hasPrevious || !onPrevious}
      >
        <ChevronLeft aria-hidden="true" />
        上一页
      </Button>
      <Button
        variant="outline"
        size="sm"
        onClick={onNext}
        disabled={!hasMore || !onNext}
      >
        下一页
        <ChevronRight aria-hidden="true" />
      </Button>
      {label ? (
        <span className="text-xs text-faint-foreground">{label}</span>
      ) : null}
      <DisabledReasonText reason={disabledReason} />
    </nav>
  );
}
