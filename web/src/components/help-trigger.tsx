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
 * Shared `?` help affordance.
 *
 * Radix handles hover, keyboard focus and Escape dismissal. This wrapper adds
 * pointer and touch click-to-toggle on top of those defaults so a tap works
 * without a hover-capable pointer. The tooltip is portaled, collision-aware,
 * width-limited and offset from its trigger; it never replaces in-place
 * permission, error, risk or operation feedback.
 */
export function HelpTrigger({
  label,
  content,
  side = "top",
}: HelpTriggerProps) {
  const [open, setOpenState] = React.useState(false);
  const openRef = React.useRef(false);
  const pointerTypeRef = React.useRef<string | null>(null);
  const wasOpenAtPointerDownRef = React.useRef(false);

  const setOpen = React.useCallback((next: boolean) => {
    openRef.current = next;
    setOpenState(next);
  }, []);

  const handlePointerDownCapture = React.useCallback(
    (event: React.PointerEvent<HTMLButtonElement>) => {
      // Capture runs before Radix's own pointerdown close, so this snapshot is
      // independent of event-handler merge order.
      pointerTypeRef.current = event.pointerType;
      wasOpenAtPointerDownRef.current = openRef.current;
    },
    [],
  );

  const handleClick = React.useCallback(
    (event: React.MouseEvent<HTMLButtonElement>) => {
      const pointerType = pointerTypeRef.current;
      pointerTypeRef.current = null;
      if (!pointerType) {
        // Keyboard activation (or assistive-technology click) must not close
        // help that focus just opened. Radix closes on click by default.
        event.preventDefault();
        event.stopPropagation();
        if (!openRef.current) {
          setOpen(true);
        }
        return;
      }
      if (wasOpenAtPointerDownRef.current) {
        // Radix closed it on pointerdown; let its click handler finish.
        return;
      }
      // A pointer gesture that began closed must end open. Radix closes on
      // click, and focus or hover may have opened the tooltip during the same
      // gesture, so prevent its click handler from closing it again.
      event.preventDefault();
      if (!openRef.current) {
        setOpen(true);
      }
    },
    [setOpen],
  );

  const handlePointerCancel = React.useCallback(() => {
    pointerTypeRef.current = null;
  }, []);

  return (
    <Tooltip open={open} onOpenChange={setOpen} delayDuration={150}>
      <TooltipTrigger asChild>
        <button
          type="button"
          data-slot="help-trigger"
          aria-label={label}
          aria-expanded={open}
          className="inline-flex size-6 shrink-0 items-center justify-center rounded-full border border-card-border bg-card text-xs font-semibold leading-none text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          onPointerDownCapture={handlePointerDownCapture}
          onClickCapture={handleClick}
          onPointerCancel={handlePointerCancel}
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
