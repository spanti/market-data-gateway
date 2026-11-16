/**
 * jsdom has no ResizeObserver. Radix popper TooltipContent uses it for arrow
 * sizing. Tests that actually open a tooltip install this no-op stub; other
 * tests keep ResizeObserver undefined so the ECharts canvas host stays inert.
 */
export function installResizeObserverStub(): void {
  if (typeof globalThis.ResizeObserver !== "undefined") {
    return;
  }
  class ResizeObserverStub implements ResizeObserver {
    observe(): void {}
    unobserve(): void {}
    disconnect(): void {}
  }
  globalThis.ResizeObserver = ResizeObserverStub;
}
