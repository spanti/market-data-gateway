import { act, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { HttpError } from "@/api/http/types";
import { emitHttpError } from "@/runtime/http-error-bus";
import { resetRetryAfter } from "@/runtime/retry-after";
import { useRetryAfter } from "@/runtime/use-retry-after";

function Probe() {
  const retry = useRetryAfter();
  return (
    <div
      data-testid="probe"
      data-blocked={String(retry.blocked)}
      data-seconds={retry.secondsRemaining}
    >
      {retry.reason?.message ?? "ready"}
    </div>
  );
}

function probe() {
  return screen.getByTestId("probe");
}

function rateLimited(retryAfterS: number | undefined, headers = true) {
  return new HttpError({
    kind: "rest",
    status: 429,
    code: "rate_limited",
    message: "too many requests",
    details:
      retryAfterS === undefined ? {} : { retry_after_s: retryAfterS },
    requestId: "rate-request-id",
    headers: headers && retryAfterS !== undefined
      ? { "retry-after": String(retryAfterS) }
      : {},
  });
}

describe("429 Retry-After gate", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-11-09T12:00:00.000Z"));
  });

  afterEach(() => {
    resetRetryAfter();
    vi.useRealTimers();
  });

  it("blocks actions for the server delay, ticks down, and re-enables after expiry", () => {
    render(<Probe />);
    expect(probe()).toHaveTextContent("ready");
    expect(probe()).toHaveAttribute("data-blocked", "false");

    act(() => {
      emitHttpError(rateLimited(30));
    });

    expect(probe()).toHaveAttribute("data-blocked", "true");
    expect(probe()).toHaveAttribute("data-seconds", "30");
    expect(probe()).toHaveTextContent("限流中，请在 30 秒后重试");

    act(() => {
      vi.advanceTimersByTime(12_000);
    });
    expect(probe()).toHaveAttribute("data-seconds", "18");
    expect(probe()).toHaveTextContent("限流中，请在 18 秒后重试");

    act(() => {
      vi.advanceTimersByTime(18_000);
    });
    expect(probe()).toHaveAttribute("data-blocked", "false");
    expect(probe()).toHaveTextContent("ready");
  });

  it("takes the delay from details when the header is absent", () => {
    render(<Probe />);
    act(() => {
      emitHttpError(rateLimited(5, false));
    });
    expect(probe()).toHaveAttribute("data-seconds", "5");
  });

  it("does not shorten a longer active block", () => {
    render(<Probe />);
    act(() => {
      emitHttpError(rateLimited(30));
    });
    act(() => {
      emitHttpError(rateLimited(5));
    });
    expect(probe()).toHaveAttribute("data-seconds", "30");
  });

  it("ignores a 429 without a usable Retry-After", () => {
    render(<Probe />);
    act(() => {
      emitHttpError(rateLimited(undefined));
    });
    expect(probe()).toHaveAttribute("data-blocked", "false");
  });
});
