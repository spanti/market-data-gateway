import * as React from "react";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

export type HelpTriggerSide = "top" | "right" | "bottom" | "left";

export interface HelpTriggerProps {
  /** Accessible name for the `?` button. */
  label: string;
  /** Readable help body. Required risk or error feedback must stay inline. */
  content: React.ReactNode;
  side?: HelpTriggerSide;
}

/**
 * Help disclosure state for the shared `?` affordance.
 *
 * Radix owns hover, focus and Escape. Pointer activation needs one extra
 * decision: a tap that begins while help is closed must end open, while a tap
 * that begins while help is open must end closed. The trigger remembers the
 * disclosure state at press start so Radix's focus/click ordering cannot
 * invert that result.
 */
function useHelpDisclosure() {
  const [open, setOpen] = React.useState(false);
  const openAtPressStart = React.useRef<boolean | null>(null);

  const startPointerGesture = () => {
    openAtPressStart.current = open;
  };

  const cancelPointerGesture = () => {
    openAtPressStart.current = null;
  };

  const resolveActivation = (event: React.MouseEvent<HTMLButtonElement>) => {
    const startedOpen = openAtPressStart.current;
    openAtPressStart.current = null;

    if (startedOpen === null) {
      // Keyboard or assistive-technology activation. Focus may already have
      // opened help; do not let Radix's click-to-close dismiss it.
      event.preventDefault();
      setOpen(true);
      return;
    }

    if (startedOpen) {
      // The press already closed help; Radix's click handler finishes the
      // gesture without reopening it.
      return;
    }

    // The gesture began closed, so it must end open. Focus may have opened
    // help during the gesture; suppressing Radix's click toggle keeps it open.
    event.preventDefault();
    setOpen(true);
  };

  return { open, setOpen, startPointerGesture, cancelPointerGesture, resolveActivation };
}

/**
 * Shared `?` help affordance.
 *
 * Hover and keyboard focus open the help surface; touch taps toggle it and
 * Escape closes it. The content is portaled, collision-aware, width-limited
 * and offset from the trigger so it never covers its own control. Only
 * supplementary definitions belong here: permission, risk, validation,
 * operation-result and Request ID feedback stay inline.
 */
export function HelpTrigger({
  label,
  content,
  side = "top",
}: HelpTriggerProps) {
  const {
    open,
    setOpen,
    startPointerGesture,
    cancelPointerGesture,
    resolveActivation,
  } = useHelpDisclosure();

  return (
    <Tooltip open={open} onOpenChange={setOpen}>
      <TooltipTrigger asChild>
        <button
          type="button"
          data-slot="help-trigger"
          aria-label={label}
          aria-expanded={open}
          className="inline-flex size-6 shrink-0 items-center justify-center rounded-full border border-card-border bg-card text-xs font-semibold leading-none text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          onPointerDownCapture={startPointerGesture}
          onClickCapture={resolveActivation}
          onPointerCancel={cancelPointerGesture}
        >
          <span aria-hidden="true">?</span>
        </button>
      </TooltipTrigger>
      <TooltipContent
        side={side}
        sideOffset={8}
        collisionPadding={12}
        className="bg-popover text-left text-popover-foreground"
      >
        <span
          data-help-content="true"
          className="block max-w-[min(20rem,calc(100vw-2rem))] whitespace-normal break-words text-left leading-relaxed"
        >
          {content}
        </span>
      </TooltipContent>
    </Tooltip>
  );
}
