import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type * as React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  CsvDownloadCancelledError,
  fetchBarsCsv,
  type CsvDownloadResult,
  type CsvProgress,
} from "@/api/endpoints/bars";
import { HttpError } from "@/api/http/types";
import { CsvDownloadPanel } from "@/components/csv-download-panel";
import { TooltipProvider } from "@/components/ui/tooltip";
import { classifyExportRange } from "@/lib/export-policy";
import { expectNoBannedUiSymbols } from "@/test/ui-copy";

function renderPanel(ui: React.ReactElement) {
  return render(<TooltipProvider>{ui}</TooltipProvider>);
}

vi.mock("@/api/endpoints/bars", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return { ...actual, fetchBarsCsv: vi.fn() };
});

const START = new Date("2024-01-01T00:00:00.000Z");
const END = new Date("2024-01-02T00:00:00.000Z");
const QUERY = {
  instrument: "binance:BTC/USDT",
  interval: "1m" as const,
  start: START.toISOString(),
  end: END.toISOString(),
};
const DIRECT = classifyExportRange(START, END);

const mockFetch = vi.mocked(fetchBarsCsv);

function installObjectUrl() {
  Object.defineProperty(URL, "createObjectURL", {
    configurable: true,
    writable: true,
    value: () => "blob:test",
  });
  Object.defineProperty(URL, "revokeObjectURL", {
    configurable: true,
    writable: true,
    value: () => {},
  });
}

function doneResult(): CsvDownloadResult {
  return {
    blob: new Blob(["schema_version\n1\n"]),
    filename: "1m-bars.csv",
    contentType: "text/csv; charset=utf-8",
    contentLength: null,
    requestId: null,
    disposition: 'inline; filename="1m-bars.csv"',
  };
}

beforeEach(() => {
  installObjectUrl();
  mockFetch.mockReset();
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
  delete (URL as unknown as { createObjectURL?: unknown }).createObjectURL;
  delete (URL as unknown as { revokeObjectURL?: unknown }).revokeObjectURL;
});

describe("CSV download progress", () => {
  it("shows downloaded bytes with an unknown total and never a fake percentage", async () => {
    let emitProgress: ((progress: CsvProgress) => void) | null = null;
    let resolveDownload: ((result: CsvDownloadResult) => void) | null = null;
    mockFetch.mockImplementation((_query, options) => {
      emitProgress = options?.onProgress ?? null;
      return new Promise<CsvDownloadResult>((resolve) => {
        resolveDownload = resolve;
      });
    });

    const user = userEvent.setup();
    renderPanel(<CsvDownloadPanel query={QUERY} decision={DIRECT} />);
    await user.click(
      screen.getByRole("button", { name: "直接下载 CSV（≤31 天）" }),
    );

    act(() => {
      emitProgress?.({ loadedBytes: 1024, totalBytes: null, percent: null });
    });
    expect(
      screen.getByText(/已下载 1 KB \/ 总量未知/),
    ).toBeInTheDocument();
    expect(screen.queryByRole("progressbar")).not.toBeInTheDocument();

    act(() => {
      resolveDownload?.(doneResult());
    });
    expect(
      await screen.findByText(/已触发浏览器下载/),
    ).toBeInTheDocument();
    expect(screen.getByText(/总量未知/)).toBeInTheDocument();
  });

  it("shows a percentage only when the progress total is trusted", async () => {
    let emitProgress: ((progress: CsvProgress) => void) | null = null;
    let resolveDownload: ((result: CsvDownloadResult) => void) | null = null;
    mockFetch.mockImplementation((_query, options) => {
      emitProgress = options?.onProgress ?? null;
      return new Promise<CsvDownloadResult>((resolve) => {
        resolveDownload = resolve;
      });
    });

    const user = userEvent.setup();
    renderPanel(<CsvDownloadPanel query={QUERY} decision={DIRECT} />);
    await user.click(
      screen.getByRole("button", { name: "直接下载 CSV（≤31 天）" }),
    );

    act(() => {
      emitProgress?.({ loadedBytes: 50, totalBytes: 100, percent: 50 });
    });
    expect(screen.getByText(/已下载 50 B \/ 100 B（50%）/)).toBeInTheDocument();
    expect(
      (screen.getByRole("progressbar") as HTMLProgressElement).value,
    ).toBe(50);

    act(() => {
      resolveDownload?.(doneResult());
    });
    await screen.findByText(/已触发浏览器下载/);
  });

  it("aborts through its AbortController when cancel is clicked", async () => {
    mockFetch.mockImplementation((_query, options) => {
      return new Promise<CsvDownloadResult>((_resolve, reject) => {
        options?.signal?.addEventListener("abort", () => {
          reject(new CsvDownloadCancelledError());
        });
      });
    });

    const user = userEvent.setup();
    renderPanel(<CsvDownloadPanel query={QUERY} decision={DIRECT} />);
    await user.click(
      screen.getByRole("button", { name: "直接下载 CSV（≤31 天）" }),
    );
    await user.click(screen.getByRole("button", { name: "取消下载" }));

    expect(
      await screen.findByText(/下载已取消（AbortController）/),
    ).toBeInTheDocument();
  });

  it("keeps the Blob path visibly disabled above 31 days", () => {
    const jobDecision = classifyExportRange(
      START,
      new Date(START.getTime() + 45 * 24 * 60 * 60 * 1000),
    );
    renderPanel(<CsvDownloadPanel query={QUERY} decision={jobDecision} />);

    expect(
      screen.getByRole("button", { name: "直接下载 CSV（≤31 天）" }),
    ).toBeDisabled();
    expect(
      screen.getByText(/只创建 Admin Export Job，不自动退回超大 Blob/),
    ).toBeInTheDocument();
  });
});

describe("CSV download D surface", () => {
  it("uses a shared card, inline help and clean UI copy", () => {
    renderPanel(<CsvDownloadPanel query={QUERY} decision={DIRECT} />);

    const region = screen.getByRole("region", { name: "CSV 直接下载" });
    expect(region).toHaveAttribute("data-slot", "card");
    expect(
      within(region).getByRole("button", {
        name: "小文件 CSV 直接下载说明",
      }),
    ).toBeInTheDocument();
    expectNoBannedUiSymbols(region);
  });

  it("marks backend error text as verbatim raw evidence", async () => {
    mockFetch.mockRejectedValue(
      new HttpError({
        kind: "rest",
        status: 500,
        code: "internal_error",
        message: "upstream · failed：timeout",
        details: {},
        requestId: "req-raw-evidence",
      }),
    );

    const user = userEvent.setup();
    renderPanel(<CsvDownloadPanel query={QUERY} decision={DIRECT} />);
    await user.click(
      screen.getByRole("button", { name: "直接下载 CSV（≤31 天）" }),
    );

    const raw = await screen.findByText("upstream · failed：timeout");
    expect(raw).toHaveAttribute("data-raw-evidence", "true");
    expect(screen.getByText(/req-raw-evidence/)).toBeInTheDocument();

    const region = screen.getByRole("region", { name: "CSV 直接下载" });
    expectNoBannedUiSymbols(region);
  });
});
